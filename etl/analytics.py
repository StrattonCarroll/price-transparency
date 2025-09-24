#!/usr/bin/env python3
"""
Production-ready analytics for hospital price transparency data
"""
import os
import psycopg2
import sys
from pathlib import Path

def get_db_connection():
    """Get database connection with proper error handling"""
    try:
        return psycopg2.connect(
            host=os.getenv('PGHOST', 'localhost'),
            port=int(os.getenv('PGPORT', '5433')),
            user=os.getenv('PGUSER', 'hpt_owner'),
            password=os.getenv('PGPASSWORD', 'hpt_owner_pw'),
            dbname=os.getenv('PGDATABASE', 'hpt_db')
        )
    except Exception as e:
        print(f"ERROR: Cannot connect to database: {e}")
        sys.exit(1)

def run_analytics():
    """Run comprehensive analytics on the loaded hospital data"""
    print("🏥 HOSPITAL PRICE TRANSPARENCY ANALYSIS")
    print("=" * 50)

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            # Overall statistics
            print("\n📊 OVERALL STATISTICS:")
            cur.execute("""
                SELECT
                    COUNT(DISTINCT hospital_name) as hospitals,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT plan_name) as unique_payers,
                    AVG(standard_charge_discounted_cash) as avg_discounted_price,
                    MIN(standard_charge_discounted_cash) as min_price,
                    MAX(standard_charge_discounted_cash) as max_price
                FROM hpt.standard_charge
                WHERE standard_charge_discounted_cash IS NOT NULL
            """)
            result = cur.fetchone()

            if result[3] is None:
                print("  ⚠️  No pricing data found. Please check data loading.")
                return

            print("  Hospitals: {}".format(result[0]))
            print("  Total Records: {}".format(result[1]))
            print("  Unique Payers: {}".format(result[2]))
            print("  Average Discounted Price: ${:.2f}".format(result[3]))
            print("  Price Range: ${:.2f} - ${:.2f}".format(result[4], result[5]))

            # Top 5 most expensive procedures by discounted cash price
            print("\n💰 TOP 5 MOST EXPENSIVE PROCEDURES:")
            cur.execute("""
                SELECT
                    description,
                    COUNT(*) as records,
                    AVG(standard_charge_discounted_cash) as avg_price,
                    MIN(standard_charge_discounted_cash) as min_price,
                    MAX(standard_charge_discounted_cash) as max_price
                FROM hpt.standard_charge
                WHERE standard_charge_discounted_cash IS NOT NULL AND plan_name IS NOT NULL
                GROUP BY description
                ORDER BY avg_price DESC
                LIMIT 5
            """)
            results = cur.fetchall()

            for i, result in enumerate(results, 1):
                desc = str(result[0])[:60]
                print("  {}. {}...".format(i, desc))
                print("     {} records, Avg: ${:.2f}".format(result[1], result[2]))

            # Hospital comparison
            print("\n🏛️  HOSPITAL PRICE COMPARISON:")
            cur.execute("""
                SELECT
                    hospital_name,
                    COUNT(*) as procedures,
                    AVG(standard_charge_discounted_cash) as avg_price,
                    COUNT(DISTINCT plan_name) as payers
                FROM hpt.standard_charge
                WHERE standard_charge_discounted_cash IS NOT NULL
                GROUP BY hospital_name
                ORDER BY avg_price DESC
            """)
            results = cur.fetchall()

            for i, result in enumerate(results, 1):
                hosp_name = str(result[0])[:35]
                print("  {}. {:35} | {:5d} | ${:8.2f} | {:2d} payers".format(
                    i, hosp_name, result[1], result[2], result[3]))

            # Payer analysis
            print("\n💳 TOP 10 PAYERS BY PROCEDURE COUNT:")
            cur.execute("""
                SELECT
                    plan_name,
                    COUNT(*) as procedures,
                    AVG(standard_charge_discounted_cash) as avg_price
                FROM hpt.standard_charge
                WHERE standard_charge_discounted_cash IS NOT NULL
                GROUP BY plan_name
                ORDER BY procedures DESC
                LIMIT 10
            """)
            results = cur.fetchall()

            for i, result in enumerate(results, 1):
                payer = str(result[0])[:25]
                print("  {:2d}. {:25} | {:5d} | ${:8.2f}".format(
                    i, payer, result[1], result[2]))

    except Exception as e:
        print(f"ERROR: Analytics failed: {e}")
        sys.exit(1)
    finally:
        conn.close()

    print("\n✅ ANALYSIS COMPLETE!")
    print("\n💡 INSIGHTS:")
    print("  • Use 'python etl/analytics.py' to run this analysis anytime")
    print("  • Add more hospitals by updating docs/sources.csv")
    print("  • Database is ready for custom queries and reporting")

def main():
    """Main entry point"""
    print("Starting hospital price transparency analysis...")

    # Check if data exists
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM hpt.standard_charge")
        count = cur.fetchone()[0]

    conn.close()

    if count == 0:
        print("⚠️  No data found in database. Please run the ETL pipeline first:")
        print("  1. python etl/fetch_sources.py --all --enabled-only")
        print("  2. python etl/normalize_selected.py --all")
        print("  3. python etl/load_postgres.py data/staging/*normalized.csv")
        return

    run_analytics()

if __name__ == "__main__":
    main()
