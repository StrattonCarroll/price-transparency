# How to Use the Price Transparency ETL Pipeline

This document explains how to run the refactored ETL pipeline and how to extend it by adding new data sources.

The pipeline is divided into two main stages:
1.  **Normalization**: Raw source files (e.g., CSV, JSON) are transformed into a standardized, canonical JSON format.
2.  **Loading**: The standardized JSON files are loaded into a PostgreSQL database.

## Running the Pipeline

All commands should be run from the repository root.

### Prerequisites

1.  **Activate the Python Environment**:
    ```powershell
    & .\.venv\Scripts\Activate.ps1
    ```
2.  **Set Database Password**: The loader requires the `PGPASSWORD` environment variable.
    ```powershell
    $env:PGPASSWORD='hpt_owner_pw'
    ```

### Step 1: Fetch Raw Data

First, ensure your raw data is present in the `data/raw/<hospital_id>/<date>/` directory by running the `fetch_sources.py` script.

```powershell
python etl\fetch_sources.py --ids nwh_bentonville
```

### Step 2: Run the Normalization Orchestrator

This script finds the latest raw file for the specified hospital, transforms it into our standard JSON format using the correct mapper, and saves the output to `data/staging/`.

```powershell
# Normalize a single hospital
python etl\normalize_selected.py --ids nwh_bentonville

# Normalize all enabled hospitals in docs/sources.csv
python etl\normalize_selected.py --all
```

The output will be a file named `data/staging/nwh_bentonville.json`.

### Step 3: Run the Database Loader

This script loads a single, staged JSON file into the `hpt.standard_charge` table in PostgreSQL.

```powershell
python etl\load_postgres.py .\data\staging\nwh_bentonville.json
```

After this step, the data is available for querying in your database.

---

## How to Add a New Hospital Data Source

The new architecture makes it easy to add support for new data sources with different file formats.

### Step 1: Add the Hospital to the Manifest

Add a new row to the `docs/sources.csv` file. The most important new field is `mapper_id`. This ID should be a short, descriptive name for the new data format.

**Example**: Let's say we are adding "Mercy Hospital," which uses a "tall" CSV format. We'll invent a new `mapper_id` called `mercy_tall`.

```csv
hospital_id,hospital_name,city,state,source_url,format,notes,enabled,header_row,mapper_id
mercy_stl,"Mercy Hospital St. Louis","St. Louis","MO","http://.../mercy.csv","csv","Mercy's tall format",Y,1,mercy_tall
```

### Step 2: Create a New Mapper Module

Create a new Python file in the `etl/mappers/` directory. The file name must follow the convention: `<mapper_id>_csv_mapper.py`.

For our example, this would be: `etl/mappers/mercy_tall_csv_mapper.py`.

### Step 3: Implement the Mapper Logic

Inside your new mapper file, you must implement a function called `map_file(inpath: Path) -> HospitalTransparencyFile`.

This function takes the path to the raw downloaded file and must return a `HospitalTransparencyFile` object that conforms to our canonical schema defined in `etl/schemas.py`.

You can use `etl/mappers/nwh_wide_csv_mapper.py` as a template. The core logic will involve:
1.  Reading the source file (e.g., using `pandas.read_csv`).
2.  Iterating through the rows.
3.  For each row, creating and populating the Pydantic objects (`StandardCharge`, `PayerRate`, `Code`, etc.).
4.  Assembling and returning the final `HospitalTransparencyFile` object.

### Step 4: Run the Pipeline

Once the new mapper is created, you can immediately run the pipeline for your new hospital. The orchestrator will automatically find and use your new mapper based on the `mapper_id` in the manifest.

```powershell
# 1. Fetch
python etl\fetch_sources.py --ids mercy_stl

# 2. Normalize (this will now use your mercy_tall_csv_mapper.py)
python etl\normalize_selected.py --ids mercy_stl

# 3. Load
python etl\load_postgres.py .\data\staging\mercy_stl.json
```
