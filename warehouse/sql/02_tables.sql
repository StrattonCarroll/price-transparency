-- warehouse/sql/02_tables.sql

-- This table is modeled after the Serif Health data dictionary.
-- It's a denormalized structure designed for analytical queries.
-- Enrichment columns (like health_system_id, baseline_rate) are nullable
-- to allow for a phased ETL implementation.

DROP TABLE IF EXISTS hpt.standard_charge;

CREATE TABLE hpt.standard_charge (
    -- Core Identifiers
    id BIGSERIAL PRIMARY KEY,
    hospital_id TEXT NOT NULL,                     -- The hospital's own ID from the source file or our internal ID
    hospital_template_id UUID,                     -- Persistent UUID for a hospital across years
    health_system_id UUID,                         -- Persistent UUID for the parent health system
    npi_number TEXT,                               -- National Provider Identifier (Type 2 for organizations)
    ein TEXT,                                      -- Employer Identification Number (TIN)

    -- Hospital Metadata
    hospital_name TEXT,
    hospital_address TEXT,
    hospital_region TEXT,                          -- State
    last_updated_on DATE,
    version TEXT,

    -- Service & Code Metadata
    description TEXT,
    setting TEXT,                                  -- e.g., inpatient, outpatient
    billing_class TEXT,
    code TEXT,
    code_type TEXT,                                -- e.g., CPT, HCPCS, MS-DRG
    modifiers TEXT,
    drug_unit_of_measurement TEXT,
    drug_type_of_measurement TEXT,

    -- Payer & Plan Metadata
    raw_payer_name TEXT,                           -- The payer name as it appears in the source file
    payer_name TEXT,                               -- Standardized payer name
    payer_product TEXT,                            -- e.g., HMO, PPO
    payer_class TEXT,                              -- e.g., Commercial, Medicare
    plan_name TEXT,

    -- Charge & Rate Metadata
    standard_gross_charge NUMERIC(18, 4),
    standard_discounted_cash NUMERIC(18, 4),
    negotiated_rate_dollar NUMERIC(18, 4),
    negotiated_rate_percentage NUMERIC(18, 4),
    estimated_amount NUMERIC(18, 4),
    standard_charge_methodology TEXT,

    -- Baseline & Enrichment (for future use)
    baseline_rate NUMERIC(18, 4),
    baseline_schedule TEXT,
    relative_to_baseline NUMERIC(18, 4),

    -- Auditing & Notes
    additional_generic_notes TEXT,
    source_file TEXT,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_hospital_id ON hpt.standard_charge (hospital_id);
CREATE INDEX IF NOT EXISTS idx_payer_name ON hpt.standard_charge (payer_name);
CREATE INDEX IF NOT EXISTS idx_code ON hpt.standard_charge (code, code_type);