from __future__ import annotations
import argparse
import logging
import json
import sys
from pathlib import Path
import psycopg2
import pandas as pd

# Add project root to the Python path
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from etl.schemas import HospitalTransparencyFile, StandardCharge, PayerRate, Code

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def map_charge_to_db_row(charge: StandardCharge, hospital_meta: dict, hospital_id: str) -> dict:
    """
    Maps a single StandardCharge object to a dictionary representing a row
    in the hpt.standard_charge database table.
    """
    # For simplicity, we'll just grab the first code.
    # A more advanced implementation might create separate rows for each code.
    primary_code = charge.codes[0] if charge.codes else Code(billing_code_type='UNKNOWN', billing_code='UNKNOWN')

    return {
        "hospital_id": hospital_id,
        "hospital_name": hospital_meta.get("hospital_name"),
        "hospital_address": hospital_meta.get("hospital_location"),
        "last_updated_on": hospital_meta.get("last_updated_on"),
        "version": hospital_meta.get("version"),
        "description": charge.description,
        "setting": charge.setting,
        "billing_class": charge.billing_class,
        "code": primary_code.code,
        "code_type": primary_code.code_type,
        "modifiers": ",".join(charge.modifiers) if charge.modifiers else None,
        "standard_gross_charge": charge.gross_charge,
        "standard_discounted_cash": charge.discounted_cash_charge,
        "source_file": charge.source_file,
    }

def load_file(inpath: Path):
    """
    Loads a single staged JSON file into the PostgreSQL database.
    One row will be inserted for each payer-specific rate.
    """
    logger.info(f"Processing staged file: {inpath}")
    hospital_id = inpath.stem  # e.g., "nwh_bentonville"

    # 1. Parse the file with our Pydantic model
    try:
        with open(inpath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        file_model = HospitalTransparencyFile.model_validate(data)
    except Exception as e:
        logger.error(f"Failed to parse or validate {inpath}: {e}")
        return

    # 2. Prepare data for insertion
    records_to_insert = []
    hospital_meta = {
        "hospital_name": file_model.hospital_name,
        "hospital_location": file_model.hospital_location,
        "last_updated_on": file_model.last_updated_on,
        "version": file_model.version,
    }

    for charge in file_model.standard_charges:
        base_row = map_charge_to_db_row(charge, hospital_meta, hospital_id)
        
        if not charge.payer_rates:
            # Handle cases with no specific payer rates (e.g., only gross charge)
            records_to_insert.append(base_row)
        else:
            # Create a distinct row for each payer rate
            for rate in charge.payer_rates:
                record = base_row.copy()
                rate_type = getattr(rate, 'negotiated_type', getattr(rate, 'negotiated_rate_type', None))
                record.update({
                    "raw_payer_name": rate.payer_name,
                    "plan_name": rate.plan_name,
                    "negotiated_rate_dollar": rate.negotiated_rate if rate_type == 'dollar' else None,
                    "negotiated_rate_percentage": rate.negotiated_rate if rate_type == 'percentage' else None,
                    "standard_charge_methodology": rate_type,
                })
                records_to_insert.append(record)
    
    if not records_to_insert:
        logger.warning(f"No records to insert for {hospital_id}.")
        return

    # 3. Insert data into PostgreSQL
    df = pd.DataFrame(records_to_insert)
    
    # Ensure all columns from the DB table exist in the DataFrame
    # This prevents errors if some optional fields are always missing.
    # We will get the full list of columns from the DB schema.
    # For now, this is a placeholder. A robust solution would query the DB info schema.
    db_columns = [
        'hospital_id', 'hospital_template_id', 'health_system_id', 'npi_number', 'ein',
        'hospital_name', 'hospital_address', 'hospital_region', 'last_updated_on', 'version',
        'description', 'setting', 'billing_class', 'code', 'code_type', 'modifiers',
        'drug_unit_of_measurement', 'drug_type_of_measurement', 'raw_payer_name',
        'payer_name', 'payer_product', 'payer_class', 'plan_name', 'standard_gross_charge',
        'standard_discounted_cash', 'negotiated_rate_dollar', 'negotiated_rate_percentage',
        'estimated_amount', 'standard_charge_methodology', 'baseline_rate',
        'baseline_schedule', 'relative_to_baseline', 'additional_generic_notes',
        'source_file'
    ]
    for col in db_columns:
        if col not in df.columns:
            df[col] = None

    df = df[db_columns] # Ensure column order

    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            # Truncate table for this hospital to avoid duplicates on re-runs
            logger.info(f"Deleting existing data for hospital_id: {hospital_id}")
            cur.execute("DELETE FROM hpt.standard_charge WHERE hospital_id = %s", (hospital_id,))
            
            # Use psycopg2's fast executemany
            from psycopg2.extras import execute_values
            
            tuples = [tuple(x) for x in df.to_numpy()]
            cols = ','.join(list(df.columns))
            query  = f"INSERT INTO hpt.standard_charge ({cols}) VALUES %s"
            
            execute_values(cur, query, tuples)
            conn.commit()
            logger.info(f"✓ Successfully inserted {len(records_to_insert)} records for {hospital_id}.")

    except Exception as e:
        logger.error(f"Database insertion failed for {hospital_id}: {e}")
        conn.rollback()
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Load staged JSON files into PostgreSQL.")
    parser.add_argument("file_path", help="Path to a single staged JSON file to load.")
    args = parser.parse_args()

    infile = Path(args.file_path)
    if not infile.exists():
        logger.error(f"File not found: {infile}")
        sys.exit(1)
        
    load_file(infile)

if __name__ == "__main__":
    main()
