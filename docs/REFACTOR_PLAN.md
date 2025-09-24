# Phase One Refactor: Building a Scalable ETL Foundation

This document outlines the tasks required to refactor the ETL pipeline to be more scalable, maintainable, and aligned with industry standards for hospital price transparency data.

## Guiding Principles

1.  **Flexibility over Specificity**: The system must not be overfit to a single data source's format. It should be easy to add new data sources with different file layouts (CSV, JSON, etc.) without rewriting core logic.
2.  **Standardize on a Canonical Model**: All raw, source-specific data formats will be transformed into a single, consistent, internal data model (the "canonical schema") during the staging process. We will use the official CMS JSON schema as our guide for this model.
3.  **Analytics-Ready Warehouse**: The final destination for the data will be a denormalized, wide table in PostgreSQL, modeled after best practices (e.g., Serif Health's data dictionary). This table will be optimized for analytical queries.
4.  **Phased Implementation**: The initial focus (Phase One) is on building the foundational data pipeline. Advanced features like data enrichment (NPI/EIN lookups, payer name standardization, baseline rate calculation) will be built on top of this foundation in a subsequent phase.

## Phase One Task List

-   [ ] **Task 1: Define the Target Warehouse Schema**
    -   [x] Redefine the `hpt.standard_charge` table in `warehouse/sql/02_tables.sql` to match the analytics-ready, Serif-inspired model.
-   [ ] **Task 2: Define the Canonical Staging Schema**
    -   [ ] Create a new file, `etl/schemas.py`, to define the Pydantic models for our internal, standardized data format.
-   [ ] **Task 3: Create the Mapper Architecture**
    -   [ ] Create a new directory: `etl/mappers/`.
    -   [ ] Move the existing normalization logic for Northwest Health's "wide" CSV format into its own module: `etl/mappers/nwh_wide_csv_mapper.py`.
    -   [ ] Update this mapper to output data that conforms to the new Pydantic schemas.
-   [ ] **Task 4: Refactor the Normalization Orchestrator**
    -   [ ] Modify `etl/normalize_selected.py` to act as an orchestrator.
    -   [ ] It will read a `mapper_id` from `docs/sources.csv` to dynamically select and run the correct mapper.
    -   [ ] The output will be saved as standardized JSON files in `data/staging/`.
-   [ ] **Task 5: Upgrade the Database Loader**
    -   [ ] Update `etl/load_postgres.py` to read the staged JSON files.
    -   [ ] It will parse the JSON using the Pydantic models and load the data into the new `hpt.standard_charge` PostgreSQL table.
-   [ ] **Task 6: Update Documentation**
    -   [ ] Create a new `INSTRUCTIONS.md` file explaining how to run the new, refactored ETL process and how to add a new data source with a custom mapper.
