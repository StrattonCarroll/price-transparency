# Repository Guidelines

## Project Structure & Module Organization
- The bootstrap script "init_duckdb.py" seeds the "hpt" schema; keep it lean and move heavier transforms into the "etl" package.
- Analytical SQL lives in "warehouse/sql" (example: "01_init.sql"); prefix new files with two-digit increments to control execution order.
- DuckDB artifacts are written to "data"; clear stale runs with "rm data/*.duckdb" or "git clean -fd data" before reseeding.
- Park research notes and decision records in "docs" so documentation travels with the repo.
- The tracked ".venv" stub documents interpreter settings; never commit third-party packages inside it.

## Build, Test, and Development Commands
- Run "python -m venv .venv" followed by "source .venv/bin/activate" (or ".\.venv\Scriptsctivate" on Windows) to configure the interpreter.
- "pip install -r requirements.txt" refreshes dependencies after each pull or requirements change.
- "python init_duckdb.py" checks DuckDB bindings and produces "data/hpt.duckdb".
- "duckdb data/hpt.duckdb" opens an interactive shell for profiling and ad-hoc validation.

## Coding Style & Naming Conventions
- Follow PEP 8 with four-space indentation, snake_case modules, and UpperCamelCase classes; favor explicit imports.
- Provide type hints for public functions to describe frame schemas moving through the pipeline.
- Keep SQL keywords uppercase, identifiers snake_case, and prefix staging models with "stg_" to clarify lineage.
- List expected environment variables in ".env.example" and access them via os.getenv.

## Testing Guidelines
- Use pytest with suites in a top-level "tests" directory mirroring the source layout; name files "test_<topic>.py".
- Mock external APIs and rely on DuckDB temporary schemas so integration tests remain idempotent.
- Aim for at least 80% coverage on new Python modules and explain any gaps inside the pull request.

## Commit & Pull Request Guidelines
- Write commit subjects in the imperative mood ("Add payer pricing loader") under 72 characters and supply context-rich bodies.
- Group related schema or data contract updates into discrete commits to simplify rollbacks.
- Open pull requests with a concise summary, linked issue, migration notes, and validation evidence (row counts, screenshots, or DuckDB query output).
- Confirm local pytest and essential SQL smoke checks pass before requesting review; call out any skipped validations.

## Data & Security Notes
- Do not commit PHI, PII, or large ".duckdb" exports; rely on the existing ".gitignore" rules for "data".
- Load secrets from ".env" using python-dotenv and rotate credentials immediately if exposure occurs.
- Share only redacted CSV snippets and strip payer identifiers before posting to shared channels.

# agents.md

## Price Transparency — Agent Guide

This doc teaches the agent how to navigate the repo, what files do what, and which commands to run for common tasks.

### Project roots & conventions

* **OS:** Windows (PowerShell shell).
* **Repo root:** `Z:\price-transparency`
* **Python venv:** `Z:\price-transparency\.venv`
* **Primary data dirs**

  * Raw downloads: `data/raw/<hospital_id>/<YYYY-MM-DD>/<original_file>`
  * Staging (normalized CSV): `data/staging/<hospital_id>__cms_tall_normalized.csv`
* **Manifest of sources:** `docs/sources.csv`
* **Environment file:** `.env` (contains PGHOST/PORT/etc.)
* **Postgres:** local, default **port 5433** (update if different).

### Key scripts (all under `etl/`)

* `fetch_sources.py` — reads `docs/sources.csv` and downloads selected files into `data/raw/...` (creates a `.json` sidecar with hash & bytes).
* `normalize_cms_tall.py` — normalizes a **single raw CSV** (CMS “CSV Tall” style; auto-detects header row; default “row 3”).
* `normalize_selected.py` — convenience wrapper (below) that finds the **latest** raw file per hospital and calls `normalize_cms_tall.py`.
* `load_postgres.py` — bulk loads a normalized CSV into `hpt.standard_charge`.

### Agent rules of engagement

1. **Always activate the venv** before running Python:

   ```powershell
   & .\.venv\Scripts\Activate.ps1
   ```
2. **Work from repo root**:

   ```powershell
   cd Z:\price-transparency
   ```
3. **Assume Postgres port 5433** unless `.env` says otherwise. Use the full psql path when needed:

   ```powershell
   & "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h 127.0.0.1 -p 5433 -U hpt_owner -d hpt_db -c "select 1;"
   ```
4. **Never overwrite raw files** unless explicitly asked (`--overwrite`).
5. **When normalizing**, prefer the latest raw file for a given `hospital_id`.
6. **When loading**, use `$env:PGPASSWORD` temporarily and unset it after.

### Common tasks (recipes)

#### A) Add or edit sources

* File: `docs/sources.csv`
* Columns:

  * `hospital_id,hospital_name,city,state,source_url,format,notes,enabled,header_row`
* Example rows:

  ```csv
  hospital_id,hospital_name,city,state,source_url,format,notes,enabled,header_row
  nwh_bentonville,"Northwest Medical Center - Bentonville","Bentonville","AR","https://...bentonville_standardcharges.csv","csv","NW Health - chargemaster CSV",Y,3
  nwh_springdale,"Northwest Medical Center - Springdale","Springdale","AR","https://...springdale_standardcharges.csv","csv","NW Health - chargemaster CSV",Y,3
  ```

#### B) Download specific hospitals to `data/raw/`

```powershell
python etl\fetch_sources.py --ids nwh_bentonville,nwh_springdale
# or everything enabled:
python etl\fetch_sources.py --all --enabled-only
# dry-run to preview:
python etl\fetch_sources.py --grep northwest --dry-run
```

#### C) Normalize latest raw files for selected hospitals

```powershell
# uses normalize_selected.py (below)
python etl\normalize_selected.py --ids nwh_bentonville,nwh_springdale
# or grep/filter:
python etl\normalize_selected.py --grep northwest
```

#### D) Load normalized CSV(s) into Postgres

```powershell
$env:PGPASSWORD='hpt_owner_pw'
python etl\load_postgres.py .\data\staging\nwh_bentonville__cms_tall_normalized.csv
python etl\load_postgres.py .\data\staging\nwh_springdale__cms_tall_normalized.csv
Remove-Item Env:\PGPASSWORD
```

#### E) Sanity checks in Postgres

```powershell
& "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h 127.0.0.1 -p 5433 -U hpt_owner -d hpt_db -c "select count(*) rows, count(distinct payer_name) payers from hpt.standard_charge;"
& "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h 127.0.0.1 -p 5433 -U hpt_owner -d hpt_db -c "select payer_name, round(avg(standard_charge)::numeric,2) avg_amt from hpt.standard_charge where standard_charge is not null group by 1 order by 2 desc limit 15;"
```

### Helper: `normalize_selected.py`


# LOAD\_DATA.md

## Load Data — Price Transparency

End-to-end path: **fetch → normalize → load → verify**

### Prereqs

* Windows with PowerShell
* Python 3.10+ and a venv at `\.venv`
* Postgres 17 running locally (default port **5433** in this project)
* `.env` file at repo root:

  ```
  DUCKDB_PATH=data/hpt.duckdb
  PGHOST=localhost
  PGPORT=5433
  PGDATABASE=hpt_db
  PGUSER=hpt_owner
  PGPASSWORD=hpt_owner_pw
  PGSCHEMA=hpt
  ```

### 0) Activate environment

```powershell
cd Z:\price-transparency
& .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt  # if present
# or install core deps:
pip install requests python-dotenv tqdm pandas pyarrow psycopg2-binary
```

### 1) Add or update source links

Edit `docs/sources.csv` and add rows:

```csv
hospital_id,hospital_name,city,state,source_url,format,notes,enabled,header_row
nwh_bentonville,"Northwest Medical Center - Bentonville","Bentonville","AR","https://...bentonville_standardcharges.csv","csv","NW Health - chargemaster CSV",Y,3
nwh_springdale,"Northwest Medical Center - Springdale","Springdale","AR","https://...springdale_standardcharges.csv","csv","NW Health - chargemaster CSV",Y,3
```

### 2) Fetch raw files to `data/raw/`

Preview first:

```powershell
python etl\fetch_sources.py --ids nwh_bentonville,nwh_springdale --dry-run
```

Download:

```powershell
python etl\fetch_sources.py --ids nwh_bentonville,nwh_springdale
```

Result:

```
data/raw/nwh_bentonville/<YYYY-MM-DD>/<original>.csv
data/raw/nwh_springdale/<YYYY-MM-DD>/<original>.csv
```

### 3) Normalize to CMS “CSV Tall”

Normalize the **latest** raw file per hospital:

```powershell
python etl\normalize_selected.py --ids nwh_bentonville,nwh_springdale
```

Outputs:

```
data/staging/nwh_bentonville__cms_tall_normalized.csv
data/staging/nwh_springdale__cms_tall_normalized.csv
```

> The normalizer auto-detects header row (defaulting to “row 3”). If a file isn’t CMS-Tall, a `__UNSUPPORTED_HEADERS.txt` will be created—open it and update the mapper later.

### 4) Load into Postgres

```powershell
$env:PGPASSWORD='hpt_owner_pw'
python etl\load_postgres.py .\data\staging\nwh_bentonville__cms_tall_normalized.csv
python etl\load_postgres.py .\data\staging\nwh_springdale__cms_tall_normalized.csv
Remove-Item Env:\PGPASSWORD
```

### 5) Verify

```powershell
& "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h 127.0.0.1 -p 5433 -U hpt_owner -d hpt_db -c "select count(*) rows from hpt.standard_charge;"
& "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h 127.0.0.1 -p 5433 -U hpt_owner -d hpt_db -c "select payer_name, round(avg(standard_charge)::numeric,2) avg_amt from hpt.standard_charge where standard_charge is not null group by 1 order by 2 desc limit 10;"
```

### 6) Add more hospitals

1. Append rows to `docs/sources.csv` (set `enabled=Y`, `header_row=3` if applicable).
2. `python etl\fetch_sources.py --ids <comma-separated-ids>`
3. `python etl\normalize_selected.py --ids <same-ids>`
4. Load with `etl\load_postgres.py`.

### Troubleshooting

* **Auth failed** → confirm `PG*` values in `.env`; ensure service is on port 5433, or change `PGPORT` to match.
* **CSV encoding issues** → the normalizer reads UTF-8 and handles common cases; if needed, we can add fallback encodings.
* **Unexpected columns** → open the generated `__UNSUPPORTED_HEADERS.txt` and paste the header list into a ticket; we’ll add a column map.

