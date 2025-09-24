from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path
import pandas as pd
import psycopg2

# Add project root to the Python path
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define paths
ROOT = Path(__file__).resolve().parents[1]
PROVIDER_CSV = ROOT / "docs" / "providers.csv"

def env(name, default=None):
    import os
    v = os.getenv(name, default)
    if v is None:
        logger.error(f"Missing required environment variable: {name}")
        sys.exit(1)
    return v

def connect_to_db():
    """Establishes a connection to the PostgreSQL database."""
    try:
        return psycopg2.connect(
            host=env("PGHOST", "localhost"),
            port=int(env("PGPORT", "5433")),
            user=env("PGUSER", "hpt_owner"),
            password=env("PGPASSWORD"),
            dbname=env("PGDATABASE", "hpt_db"),
        )
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)

def enrich_providers(conn):
    """
    Reads the provider directory and updates the main charges table with
    provider-level identifiers like NPI, EIN, and internal UUIDs.
    """
    logger.info("--- Starting Provider Enrichment ---")
    
    try:
        provider_df = pd.read_csv(PROVIDER_CSV)
    except FileNotFoundError:
        logger.error(f"Provider directory not found at: {PROVIDER_CSV}")
        return

    update_queries = []
    for _, row in provider_df.iterrows():
        sql = """
            UPDATE hpt.standard_charge
            SET
                hospital_template_id = %(hospital_template_id)s,
                health_system_id = %(health_system_id)s,
                npi_number = %(npi_number)s,
                ein = %(ein)s
            WHERE hospital_id = %(hospital_id)s;
        """
        params = {
            'hospital_template_id': row['hospital_template_id'],
            'health_system_id': row['health_system_id'],
            'npi_number': str(row['npi_number']),
            'ein': str(row['ein']),
            'hospital_id': row['hospital_id']
        }
        update_queries.append((sql, params))

    try:
        with conn.cursor() as cur:
            for sql, params in update_queries:
                logger.info(f"Updating provider info for {params['hospital_id']}")
                cur.execute(sql, params)
            conn.commit()
        logger.info("✓ Provider enrichment completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during provider enrichment: {e}")
        conn.rollback()


def main():
    parser = argparse.ArgumentParser(description="Data Enrichment for Price Transparency Project")
    parser.add_argument("--providers", action="store_true", help="Run the provider enrichment process.")
    # Add args for other enrichment steps here later
    args = parser.parse_args()

    conn = connect_to_db()
    try:
        if args.providers:
            enrich_providers(conn)
        
        if not any(vars(args).values()):
            logger.info("No specific enrichment task selected. Running all...")
            enrich_providers(conn)
            # Call other enrichment functions here

    finally:
        conn.close()


if __name__ == "__main__":
    main()
