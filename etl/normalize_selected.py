from __future__ import annotations
import argparse
import logging
import sys
import pandas as pd
from pathlib import Path
import importlib

# Add project root to the Python path
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

# Now, we can use absolute imports from the project root
from etl.schemas import HospitalTransparencyFile

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/orchestration.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Define project structure paths
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
STAGING_DIR = ROOT / "data" / "staging"
SOURCES_CSV = ROOT / "docs" / "sources.csv"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

def find_latest_raw_file(hospital_id: str) -> Path | None:
    """Finds the most recently downloaded raw file for a given hospital_id."""
    hospital_dir = RAW_DIR / hospital_id
    if not hospital_dir.exists():
        logger.warning(f"No raw data directory found for {hospital_id}")
        return None

    # Find the latest date-stamped subdirectory
    date_dirs = sorted([d for d in hospital_dir.iterdir() if d.is_dir()], reverse=True)
    if not date_dirs:
        logger.warning(f"No date directories found for {hospital_id}")
        return None
    latest_date_dir = date_dirs[0]

    # Find the first CSV file in that directory
    try:
        return next(latest_date_dir.glob('*.csv'))
    except StopIteration:
        logger.warning(f"No CSV files found in {latest_date_dir}")
        return None

def get_mapper_module(mapper_id: str):
    """Dynamically imports and returns a mapper module."""
    try:
        # We assume a convention: mapper_id 'nwh_wide' -> module 'nwh_wide_csv_mapper'
        module_name = f"etl.mappers.{mapper_id}_csv_mapper"
        return importlib.import_module(module_name)
    except ImportError:
        logger.error(f"Could not find or import mapper for id: '{mapper_id}'. "
                     f"Expected module: {module_name}")
        return None

def process_hospital(hospital_id: str, mapper_id: str):
    """
    Orchestrates the normalization process for a single hospital.
    1. Finds the latest raw file.
    2. Selects the appropriate mapper based on mapper_id.
    3. Runs the mapper to get a canonical data object.
    4. Saves the output as a standardized JSON file.
    """
    logger.info(f"--- Starting processing for {hospital_id} ---")
    
    # 1. Find raw file
    raw_file = find_latest_raw_file(hospital_id)
    if not raw_file:
        logger.error(f"Could not find raw file for {hospital_id}. Skipping.")
        return

    # 2. Get mapper
    mapper = get_mapper_module(mapper_id)
    if not mapper or not hasattr(mapper, 'map_file'):
        logger.error(f"Invalid mapper for id '{mapper_id}'. Skipping {hospital_id}.")
        return

    # 3. Run mapper
    try:
        canonical_data: HospitalTransparencyFile = mapper.map_file(raw_file)
    except Exception as e:
        logger.error(f"An error occurred while mapping {raw_file} for {hospital_id}: {e}", exc_info=True)
        return

    # 4. Save output
    output_path = STAGING_DIR / f"{hospital_id}.json"
    try:
        # Use Pydantic's model_dump_json() for proper serialization in v2
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(canonical_data.model_dump_json(indent=2, by_alias=True, exclude_none=True))
        logger.info(f"✓ Successfully mapped and saved to {output_path}")
    except Exception as e:
        logger.error(f"Could not save JSON for {hospital_id} to {output_path}: {e}")

    logger.info(f"--- Finished processing for {hospital_id} ---")


def main():
    parser = argparse.ArgumentParser(description="Normalization Orchestrator")
    parser.add_argument("--ids", help="Comma-separated list of hospital_ids to process.")
    parser.add_argument("--all", action="store_true", help="Process all enabled hospitals in sources.csv.")
    args = parser.parse_args()

    # Load the sources manifest
    sources_df = pd.read_csv(SOURCES_CSV)
    sources_df = sources_df[sources_df['enabled'] == 'Y'].copy()

    hospitals_to_process = []
    if args.ids:
        hospitals_to_process = args.ids.split(',')
    elif args.all:
        hospitals_to_process = sources_df['hospital_id'].tolist()
    else:
        logger.warning("No hospitals specified. Use --ids or --all.")
        return

    for hospital_id in hospitals_to_process:
        hospital_info = sources_df[sources_df['hospital_id'] == hospital_id]
        if hospital_info.empty:
            logger.warning(f"Hospital ID '{hospital_id}' not found or not enabled in sources.csv.")
            continue
        
        mapper_id = hospital_info.iloc[0]['mapper_id']
        if pd.isna(mapper_id):
            logger.warning(f"No mapper_id specified for '{hospital_id}' in sources.csv. Skipping.")
            continue
            
        process_hospital(hospital_id, mapper_id)


if __name__ == "__main__":
    main()
