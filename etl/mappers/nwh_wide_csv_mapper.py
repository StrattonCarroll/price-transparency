from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd
import sys
from pathlib import Path

# Add project root to the Python path
ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_DIR))

from etl.schemas import (
    HospitalTransparencyFile,
    StandardCharge,
    Code,
    PayerRate,
)
import datetime

logger = logging.getLogger(__name__)

# These are the fixed, non-payer columns expected in the NW Health wide format.
# We use this to separate metadata columns from payer rate columns.
WIDE_FORMAT_METADATA_COLS = {
    "description", "code|1", "code|1|type", "code|2", "code|2|type",
    "code|3", "code|3|type", "code|4", "code|4|type", "modifiers", "setting",
    "billing_class", "drug_unit_of_measurement", "drug_type_of_measurement",
    "standard_charge|gross", "standard_charge|discounted_cash",
    "standard_charge|min", "standard_charge|max", "additional_generic_notes"
}

def parse_header_metadata(inpath: Path) -> dict:
    """
    Parses the first two rows of the CSV to extract hospital metadata.
    """
    try:
        # Row 1 is headers, Row 2 is values.
        df_meta = pd.read_csv(inpath, nrows=1, header=0, dtype=str)
        meta_values = df_meta.iloc[0]

        hospital_name = meta_values.get('hospital_name')
        hospital_location = meta_values.get('hospital_location')
        last_updated_str = meta_values.get('last_updated_on')
        version = meta_values.get('version')

        return {
            "hospital_name": hospital_name,
            "hospital_location": hospital_location,
            "last_updated_on": datetime.datetime.strptime(last_updated_str.strip(), '%Y-%m-%d').date(),
            "version": version,
        }
    except Exception as e:
        logger.error(f"Could not parse header metadata from {inpath}: {e}")
        return {}

def map_row_to_canonical(row: pd.Series, header_metadata: dict, original_payer_cols: list, source_file: str) -> StandardCharge:
    """
    Takes a single row (as a pandas Series) from the wide CSV and maps it
    to our canonical StandardCharge Pydantic model.
    """
    # Map basic fields
    description = row.get("description", "")
    setting = row.get("setting")
    billing_class = row.get("billing_class")
    gross_charge = pd.to_numeric(row.get("standard_charge|gross"), errors='coerce')
    discounted_cash = pd.to_numeric(row.get("standard_charge|discounted_cash"), errors='coerce')

    # Map codes
    codes = []
    for i in range(1, 4):
        code_val = row.get(f"code|{i}")
        code_type = row.get(f"code|{i}|type")
        if code_val and pd.notna(code_val):
            codes.append(Code(billing_code=str(code_val), billing_code_type=str(code_type)))

    # Map payer rates
    payer_rates = []
    for col_name in original_payer_cols:
        # Access the row data using the lowercased version of the column name
        rate_val = row.get(col_name.lower().strip())
        if rate_val and pd.notna(rate_val):
            try:
                # But parse the ORIGINAL column name to preserve case
                parts = col_name.split('|')
                payer_name = parts[1]
                plan_name = parts[2] if len(parts) > 2 else "Standard"
                
                payer_rates.append(PayerRate(
                    payer_name=payer_name,
                    plan_name=plan_name,
                    negotiated_rate=float(rate_val),
                    negotiated_type="dollar" # Assumption for this format
                ))
            except (ValueError, IndexError) as e:
                logger.warning(f"Could not parse payer column '{col_name}' with value '{rate_val}': {e}")

    return StandardCharge(
        description=description,
        billing_code_information=codes,
        gross_charge=gross_charge,
        discounted_cash_charge=discounted_cash,
        payer_negotiated_rates=payer_rates,
        setting=setting,
        billing_class=billing_class,
        source_file=source_file
    )


def map_file(inpath: Path) -> HospitalTransparencyFile:
    """
    Reads a Northwest Health "wide" format CSV and maps its contents to our
    canonical HospitalTransparencyFile Pydantic model.
    """
    logger.info(f"Mapping file using nwh_wide_csv_mapper: {inpath}")

    # Step 1: Extract metadata from the file header
    header_metadata = parse_header_metadata(inpath)
    if not header_metadata:
        raise ValueError("Failed to extract essential header metadata.")

    # Step 2: Read the main data, skipping metadata rows.
    # The actual data headers are on row 3, so we use skiprows=2 and header=0.
    df = pd.read_csv(inpath, skiprows=2, header=0, dtype=str)
    
    # Identify which columns are for payers vs. which are fixed metadata
    # Keep the original casing for parsing later.
    original_payer_cols = [
        col for col in df.columns
        if col.lower().startswith('standard_charge|') and col.lower() not in WIDE_FORMAT_METADATA_COLS
    ]
    logger.info(f"Identified {len(original_payer_cols)} payer columns.")

    # Normalize column names for easier data access
    df.columns = [c.lower().strip() for c in df.columns]

    # Step 3: Map each row to a StandardCharge object
    standard_charges = [
        map_row_to_canonical(row, header_metadata, original_payer_cols, inpath.name)
        for _, row in df.iterrows()
    ]

    # Step 4: Assemble the final, canonical file object
    return HospitalTransparencyFile(
        hospital_name=header_metadata["hospital_name"],
        hospital_location=header_metadata.get("hospital_location"),
        last_updated_on=header_metadata["last_updated_on"],
        version=header_metadata.get("version"),
        standard_charges=standard_charges
    )
