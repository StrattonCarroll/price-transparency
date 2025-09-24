# Price Transparency Project

A comprehensive data pipeline for collecting, standardizing, and analyzing hospital pricing data to improve healthcare cost transparency.

## Overview

This project automates the collection of hospital chargemaster data from various sources, normalizes it to a standard CMS Tall format, and loads it into PostgreSQL for analysis. The system is designed to handle multiple hospital data formats and scales to process large datasets efficiently.

## Features

- **Multi-format Support**: Handles both CMS Tall and wide format hospital data
- **Automated Data Collection**: Downloads pricing data from hospital websites
- **Data Normalization**: Transforms various formats to standard CMS Tall structure
- **PostgreSQL Integration**: Loads data into relational database for analysis
- **DuckDB Analytics**: Optional analytical database for complex queries
- **Comprehensive Logging**: Full audit trail of data processing
- **Scalable Architecture**: Processes large datasets efficiently

## Architecture

```
Raw Data → Normalization → PostgreSQL → Analytics
   ↓           ↓            ↓          ↓
Hospital → Wide/Tall → Standard → DuckDB
Websites    Format     Format    Queries
```

## Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL 17+ (default port 5433)
- Git

### Setup

1. **Clone and setup virtual environment:**
   ```bash
   git clone <repository>
   cd price-transparency
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .\.venv\Scripts\activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run deployment script:**
   ```bash
   python deploy.py
   ```

   Choose option 2 for full setup and ETL pipeline, or option 1 for database setup only.

### Alternative Manual Setup

1. **Configure environment:**
   ```bash
   # Edit .env with your database credentials
   ```

2. **Initialize DuckDB:**
   ```bash
   python init_duckdb.py
   ```

3. **Setup database:**
   ```bash
   python deploy.py  # Choose option 1
   ```

### Usage

#### Download Hospital Data

```bash
# Download specific hospitals
python etl/fetch_sources.py --ids nwh_bentonville,nwh_springdale

# Download all enabled sources
python etl/fetch_sources.py --all --enabled-only

# Preview what would be downloaded
python etl/fetch_sources.py --grep northwest --dry-run
```

#### Normalize Data

```bash
# Normalize specific hospitals
python etl/normalize_selected.py --ids nwh_bentonville,nwh_springdale

# Normalize all available raw data
python etl/normalize_selected.py --all
```

#### Load to PostgreSQL

```bash
# Set password temporarily
$env:PGPASSWORD='your_password'

# Load normalized data
python etl/load_postgres.py data/staging/nwh_bentonville__cms_tall_normalized.csv

# Unset password
Remove-Item Env:\PGPASSWORD
```

#### Analysis

```bash
# Run analytics on loaded data
python etl/analytics.py

# Start DuckDB shell
duckdb data/hpt.duckdb

# Run SQL queries for analysis
```

## Production Deployment

### Automated Setup

The easiest way to get started is using the deployment script:

```bash
python deploy.py
```

Choose from these options:
- **Option 1**: Setup database only (for manual ETL)
- **Option 2**: Full ETL pipeline (download, normalize, load, analyze)
- **Option 3**: Show project status
- **Option 4**: Exit

### Manual Commands

```bash
# Check project status
python deploy.py  # Choose option 3

# Setup database
python deploy.py  # Choose option 1

# Run full ETL pipeline
python deploy.py  # Choose option 2

# Individual steps
python etl/fetch_sources.py --all --enabled-only
python etl/normalize_selected.py --all
python etl/load_postgres.py data/staging/*__cms_tall_normalized.csv
python etl/analytics.py
```

### Environment Configuration

Create `.env` file with your PostgreSQL credentials:

```bash
# Database Configuration
PGHOST=localhost
PGPORT=5433
PGDATABASE=hpt_db
PGUSER=hpt_owner
PGPASSWORD=your_password_here
PGSCHEMA=hpt

# Optional: DuckDB Configuration
DUCKDB_PATH=data/hpt.duckdb
```

## Data Flow

### 1. Source Management
Hospital URLs and metadata are maintained in `docs/sources.csv`:
```csv
hospital_id,hospital_name,city,state,source_url,format,notes,enabled,header_row
nwh_bentonville,"Northwest Medical Center - Bentonville","Bentonville","AR",https://...,csv,"Hospital chargemaster",Y,3
```

### 2. Data Collection
- Downloads files to `data/raw/<hospital_id>/<date>/<filename>`
- Creates metadata JSON files with hashes and download info
- Supports incremental downloads with change detection

### 3. Normalization
- **Wide Format**: Transforms column-based (payer|plan|charge_type) to row-based
- **CMS Tall Format**: Standardizes existing CMS format data
- Outputs to `data/staging/<hospital_id>__cms_tall_normalized.csv`

### 4. Database Loading
- Loads normalized data to PostgreSQL `hpt.standard_charge` table
- Handles column mapping (e.g., `code|1` → `code_1`)
- Uses efficient COPY operations for bulk loading

## Supported Formats

### Wide Format (Hospital-Specific)
- **Structure**: Each column represents a payer|plan|charge_type combination
- **Example**: `standard_charge|Aetna|Commercial|negotiated_dollar`
- **Transformation**: Creates one row per procedure + payer combination
- **Metadata**: Extracts hospital info from first two rows

### CMS Tall Format
- **Structure**: Standard CMS format with dedicated payer columns
- **Columns**: description, payer_name, plan_name, standard_charge, etc.
- **Processing**: Validates and cleans existing format

## Database Schema

### Core Tables

#### `hpt.standard_charge`
Main pricing table with normalized data:
- Hospital and procedure identification
- Payer and plan information
- Multiple charge types (gross, discounted, negotiated rates)
- Metadata and audit fields

#### `hpt.hospital`
Hospital registry with licensing and location data

#### `hpt.item`
Procedure/service catalog with codes and descriptions

## Analytics

### Sample Queries

```sql
-- Average prices by procedure type
SELECT description, COUNT(*) as records, AVG(standard_charge) as avg_price
FROM hpt.standard_charge
WHERE standard_charge IS NOT NULL
GROUP BY description
ORDER BY avg_price DESC
LIMIT 20;

-- Price variation by payer
SELECT payer_name, COUNT(*) as records, AVG(standard_charge) as avg_price
FROM hpt.standard_charge
WHERE standard_charge IS NOT NULL
GROUP BY payer_name
ORDER BY avg_price DESC;

-- Hospital price comparison
SELECT hospital_name, COUNT(*) as procedures, AVG(standard_charge) as avg_price
FROM hpt.standard_charge
GROUP BY hospital_name
ORDER BY avg_price DESC;
```

## Development

### Project Structure
```
price-transparency/
├── etl/                    # Data processing scripts
├── warehouse/sql/         # Database schema and queries
├── docs/                  # Documentation and source registry
├── data/                  # Raw and processed data
├── tests/                 # Unit and integration tests
└── init_duckdb.py        # Database initialization
```

### Adding New Hospitals
1. Add entries to `docs/sources.csv`
2. Run `python etl/fetch_sources.py --ids <new_ids>`
3. Run `python etl/normalize_selected.py --ids <new_ids>`
4. Load with `python etl/load_postgres.py`

### Testing
```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests
pytest tests/

# Run with coverage
pytest tests/ --cov=etl
```

## Troubleshooting

### Common Issues

**Normalization Fails**
- Check `data/staging/*__UNSUPPORTED_HEADERS.txt` for column mapping hints
- Verify CSV format matches expected structure
- Check logs for parsing errors

**Database Connection Issues**
- Verify PostgreSQL is running on port 5433
- Check `.env` file for correct credentials
- Ensure database `hpt_db` exists with `hpt` schema

**Memory Issues**
- Large files processed in chunks (default 250k rows)
- Adjust chunk size in `normalize_cms_tall.py` if needed
- Consider processing hospitals individually

### Logs and Debugging
- Enable debug logging: `export PYTHONPATH=. && python -m etl.normalize_cms_tall --debug`
- Check `data/staging/` for processing artifacts
- Review download metadata in `data/raw/*/`

## Project Structure

```
price-transparency/
├── etl/                    # Data processing scripts
│   ├── fetch_sources.py    # Download hospital data
│   ├── normalize_cms_tall.py # Normalize to standard format
│   ├── normalize_selected.py # Convenience wrapper
│   ├── load_postgres.py    # Load to PostgreSQL
│   └── analytics.py        # Generate insights
├── warehouse/sql/          # Database schema
├── docs/                   # Documentation and sources
├── data/                   # Raw and processed data
├── logs/                   # Application logs
├── deploy.py              # Production deployment script
├── init_duckdb.py         # DuckDB initialization
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

## Troubleshooting

### Common Issues

**Database Connection Failed**
- Verify PostgreSQL is running on port 5433
- Check `.env` credentials match your database
- Ensure database `hpt_db` exists

**No Data Downloaded**
- Check internet connection
- Verify URLs in `docs/sources.csv` are accessible
- Check `logs/normalization.log` for errors

**Normalization Fails**
- Check CSV format matches expected structure
- Look for malformed lines in `data/staging/*__UNSUPPORTED_HEADERS.txt`
- Verify sufficient disk space

**Loading Issues**
- Ensure database schema is properly created
- Check data types match between CSV and database
- Verify column mappings are correct

### Logs and Debugging

- **Deployment logs**: `logs/deployment.log`
- **Normalization logs**: `logs/normalization.log`
- **Application logs**: Check individual script outputs

### Manual Data Inspection

```bash
# Check raw data
ls -la data/raw/

# Check normalized data
ls -la data/staging/

# Database connection test
python -c "import psycopg2; psycopg2.connect(host='localhost', port=5433, user='hpt_owner', password='hpt_owner_pw', dbname='hpt_db'); print('DB OK')"

# View database contents
PGPASSWORD='hpt_owner_pw' psql -h localhost -p 5433 -U hpt_owner -d hpt_db -c "SELECT COUNT(*) FROM hpt.standard_charge;"

# Quick analytics
python etl/analytics.py
```

### Data Notes

**Important**: Due to the data structure of the loaded hospital files, payer names are stored in the `plan_name` column rather than `payer_name`. The analytics scripts have been updated to use the correct columns:

- **Payer names**: `plan_name` column
- **Prices**: `standard_charge_discounted_cash` column
- **Procedures**: `description` column

## Contributing

1. Follow PEP 8 style guidelines
2. Add comprehensive error handling
3. Include logging for debugging
4. Update documentation for schema changes
5. Use descriptive commit messages
6. Test with multiple data formats

## License

This project is designed for healthcare cost transparency research and analysis.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the logs in the `logs/` directory
3. Run `python deploy.py` (option 3) for status check
4. Verify environment configuration in `.env`

---
**Built with ❤️ for healthcare transparency**
