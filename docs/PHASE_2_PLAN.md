# Phase Two Plan: Data Enrichment

This document outlines the tasks for Phase Two, which focuses on enriching the raw data loaded in Phase One to make it analytics-ready.

## Guiding Principles

1.  **Configuration as Code**: Business logic, such as provider identifiers and payer name mappings, should live in simple, version-controlled files (e.g., CSVs). This makes the logic transparent and easy for non-developers to audit or contribute to.
2.  **Idempotent & Decoupled Enrichment**: Enrichment processes should be runnable at any time, on the entire dataset. They will be separate from the core ETL pipeline, designed to `UPDATE` existing records rather than being part of the initial `INSERT`.
3.  **Pragmatic MVP**: We will focus on the highest-value enrichment tasks (provider and payer identity) that can be implemented robustly. More complex features, like the baseline rate engine, are deferred until a solid foundation is established.

## Phase Two Task List

-   [ ] **Task 1: Implement Provider Enrichment**
    -   [ ] Create a new provider directory file: `docs/providers.csv`.
    -   [ ] Populate the file with initial data for the existing Northwest Health hospitals (NPI, EIN, Health System).
    -   [ ] Create a new, unified enrichment script: `etl/enrich.py`.
    -   [ ] Implement the logic within `etl/enrich.py` to read the provider directory and `UPDATE` the `hpt.standard_charge` table with the correct identifiers.
-   [ ] **Task 2: Implement Payer Normalization**
    -   [ ] Create a new payer mapping file: `docs/payer_map.csv`.
    -   [ ] Query the database to find all distinct `raw_payer_name` values and use them to populate the mapping file.
    -   [ ] Extend `etl/enrich.py` to read the payer map and `UPDATE` the `payer_name` column in the `hpt.standard_charge` table.
-   [ ] **Task 3: Update Documentation**
    -   [ ] Update `INSTRUCTIONS.md` to include the new, final `enrich` step in the standard pipeline execution flow.

## Future Work (Post-Phase Two)

-   **Baseline Rate Engine**: This involves sourcing CMS fee schedules, creating a loader and lookup mechanism, and building the logic to calculate `baseline_rate` and `relative_to_baseline`. This is a significant feature that will be tackled in a future phase.
