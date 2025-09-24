#!/usr/bin/env python3
"""
Production deployment script for Hospital Price Transparency project
Handles complete setup and ETL pipeline execution
"""
import os
import sys
import subprocess
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/deployment.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Create logs directory
os.makedirs('logs', exist_ok=True)

def check_requirements():
    """Check if all requirements are installed"""
    logger.info("🔍 Checking requirements...")

    try:
        import pandas
        import psycopg2
        import requests
        import duckdb
        logger.info("✅ All Python requirements installed")
        return True
    except ImportError as e:
        logger.error(f"❌ Missing requirement: {e}")
        logger.info("📦 Install with: pip install -r requirements.txt")
        return False

def check_environment():
    """Check if .env file exists and is configured"""
    logger.info("🔍 Checking environment configuration...")

    env_file = Path('.env')
    if not env_file.exists():
        logger.warning("⚠️  .env file not found")
        logger.info("📝 Creating .env template...")
        create_env_template()
        logger.info("⚠️  Please edit .env with your database credentials")
        return False

    # Check if required variables are set
    required_vars = ['PGHOST', 'PGPORT', 'PGDATABASE', 'PGUSER', 'PGPASSWORD']
    missing = []

    for line in env_file.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            key = line.split('=')[0].strip()
            if key in required_vars:
                missing.append(key)

    if missing:
        logger.warning(f"⚠️  Missing environment variables: {missing}")
        logger.info("📝 Please update .env with the missing values")
        return False

    logger.info("✅ Environment configuration valid")
    return True

def create_env_template():
    """Create .env template file"""
    template = """# Hospital Price Transparency - Environment Configuration
# Copy this file to .env and fill in your actual values

# Database Configuration
PGHOST=localhost
PGPORT=5433
PGDATABASE=hpt_db
PGUSER=hpt_owner
PGPASSWORD=your_password_here
PGSCHEMA=hpt

# Optional: DuckDB Configuration
DUCKDB_PATH=data/hpt.duckdb
"""

    Path('.env').write_text(template)

def setup_database():
    """Set up PostgreSQL database and schema"""
    logger.info("🗄️  Setting up database...")

    try:
        # Load environment variables
        for line in Path('.env').read_text().splitlines():
            if '=' in line and not line.startswith('#'):
                key, value = line.split('=', 1)
                os.environ[key] = value

        # Test database connection
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv('PGHOST'),
            port=int(os.getenv('PGPORT', '5433')),
            user=os.getenv('PGUSER'),
            password=os.getenv('PGPASSWORD'),
            dbname=os.getenv('PGDATABASE')
        )

        # Create schema and tables
        with conn.cursor() as cur:
            cur.execute('CREATE SCHEMA IF NOT EXISTS hpt;')
            logger.info("✅ Schema hpt created/verified")

            # Create tables
            sql_files = ['warehouse/sql/02_tables.sql']
            for sql_file in sql_files:
                if Path(sql_file).exists():
                    logger.info(f"📄 Executing {sql_file}...")
                    sql = Path(sql_file).read_text()
                    cur.execute(sql)

            # Verify tables
            cur.execute("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'hpt'
                ORDER BY tablename;
            """)
            tables = cur.fetchall()
            logger.info(f"✅ Database tables created: {len(tables)}")
            for table in tables:
                logger.info(f"  - hpt.{table[0]}")

        conn.commit()
        conn.close()
        logger.info("✅ Database setup complete")
        return True

    except Exception as e:
        logger.error(f"❌ Database setup failed: {e}")
        return False

def run_etl_pipeline():
    """Run the complete ETL pipeline"""
    logger.info("🚀 Starting ETL pipeline...")

    steps = [
        ("Download hospital data", "python etl/fetch_sources.py --all --enabled-only"),
        ("Normalize data", "python etl/normalize_selected.py --all"),
        ("Load to database", "python etl/load_postgres.py data/staging/*__cms_tall_normalized.csv"),
        ("Run analytics", "python etl/analytics.py")
    ]

    for step_name, command in steps:
        logger.info(f"📊 {step_name}...")
        try:
            result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=3600)

            if result.returncode == 0:
                logger.info(f"✅ {step_name} completed successfully")
                if result.stdout:
                    logger.info(f"Output: {result.stdout.strip()}")
            else:
                logger.error(f"❌ {step_name} failed")
                logger.error(f"Error: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.error(f"⏰ {step_name} timed out")
            return False
        except Exception as e:
            logger.error(f"❌ {step_name} failed with exception: {e}")
            return False

    logger.info("🎉 ETL pipeline completed successfully!")
    return True

def show_status():
    """Show current project status"""
    logger.info("📋 Project Status Report")
    logger.info("=" * 30)

    # Check files
    logger.info("📁 Files Status:")
    files_to_check = [
        ('.env', 'Environment configuration'),
        ('docs/sources.csv', 'Hospital sources list'),
        ('requirements.txt', 'Python dependencies'),
        ('README.md', 'Documentation')
    ]

    for file_path, description in files_to_check:
        exists = Path(file_path).exists()
        status = "✅" if exists else "❌"
        logger.info(f"  {status} {description}: {file_path}")

    # Check data
    logger.info("\n💾 Data Status:")
    data_dirs = [
        ('data/raw', 'Raw downloaded data'),
        ('data/staging', 'Normalized CSV files'),
    ]

    for dir_path, description in data_dirs:
        path = Path(dir_path)
        if path.exists():
            files = list(path.rglob('*'))
            files = [f for f in files if f.is_file()]
            status = "✅" if files else "⚠️"
            logger.info(f"  {status} {description}: {len(files)} files")
        else:
            logger.info(f"  ❌ {description}: Directory not found")

    # Check database
    logger.info("\n🗄️  Database Status:")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv('PGHOST', 'localhost'),
            port=int(os.getenv('PGPORT', '5433')),
            user=os.getenv('PGUSER', 'hpt_owner'),
            password=os.getenv('PGPASSWORD', 'hpt_owner_pw'),
            dbname=os.getenv('PGDATABASE', 'hpt_db')
        )

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM hpt.standard_charge")
            count = cur.fetchone()[0]
            logger.info(f"  ✅ Database: {count","} records loaded")

        conn.close()
    except Exception as e:
        logger.info(f"  ❌ Database: Not accessible ({e})")

def main():
    """Main deployment function"""
    logger.info("🚀 HOSPITAL PRICE TRANSPARENCY - PRODUCTION DEPLOYMENT")
    logger.info("=" * 60)

    if not check_requirements():
        return False

    if not check_environment():
        return False

    # Show current status
    show_status()

    response = input("\n🔧 Setup Options:\n"
                    "  [1] Setup database only\n"
                    "  [2] Run full ETL pipeline\n"
                    "  [3] Show status only\n"
                    "  [4] Exit\n"
                    "Choose (1-4): ").strip()

    if response == '1':
        if setup_database():
            logger.info("✅ Database setup complete!")
        else:
            logger.error("❌ Database setup failed!")
            return False

    elif response == '2':
        if setup_database():
            if run_etl_pipeline():
                logger.info("🎉 Full deployment completed successfully!")
                logger.info("\n💡 Next steps:")
                logger.info("  • Run 'python etl/analytics.py' for insights")
                logger.info("  • Add hospitals to docs/sources.csv")
                logger.info("  • Query database directly for custom analysis")
            else:
                logger.error("❌ ETL pipeline failed!")
                return False
        else:
            logger.error("❌ Cannot run ETL without database setup")
            return False

    elif response == '3':
        logger.info("📋 Status report generated above")
        return True

    elif response == '4':
        logger.info("👋 Exiting...")
        return True

    else:
        logger.error("❌ Invalid choice")
        return False

    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
