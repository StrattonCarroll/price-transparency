CREATE TABLE IF NOT EXISTS hpt.hospital (
  hospital_id      TEXT PRIMARY KEY,
  name             TEXT,
  license_number   TEXT,
  address_line1    TEXT,
  city             TEXT,
  state            TEXT,
  zip              TEXT,
  system_name      TEXT,
  source_url       TEXT,
  last_updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS hpt.item (
  item_id          BIGSERIAL PRIMARY KEY,
  description      TEXT,
  code_system      TEXT,
  code             TEXT,
  modifier1        TEXT,
  modifier2        TEXT,
  modifier3        TEXT,
  modifier4        TEXT,
  setting          TEXT,
  revenue_center   TEXT
);

CREATE TABLE IF NOT EXISTS hpt.standard_charge (
  hospital_id      TEXT REFERENCES hpt.hospital(hospital_id),
  item_id          BIGINT REFERENCES hpt.item(item_id),
  charge_type      TEXT,
  payer_name       TEXT,
  plan_name        TEXT,
  amount           NUMERIC(18,4),
  currency         TEXT,
  effective_date   DATE,
  expires_date     DATE,
  notes            TEXT,
  load_file_hash   TEXT,
  loaded_at        TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hpt.load_audit (
  load_id          BIGSERIAL PRIMARY KEY,
  hospital_id      TEXT,
  source_url       TEXT,
  file_path        TEXT,
  file_hash        TEXT,
  bytes            BIGINT,
  row_count        BIGINT,
  loaded_at        TIMESTAMPTZ DEFAULT now(),
  errors_json      JSONB
);
