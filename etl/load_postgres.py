from __future__ import annotations
import os, sys
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

COLS = [
 "hospital_name","hospital_location","last_updated_on","version","financial_aid_policy",
 "license_number","setting","billing_class","description",
 "code|1","code|1|type","code|2","code|2|type","code|3","code|3|type",
 "drug_unit_of_measurement","drug_type_of_measurement","modifiers",
 "standard_charge|gross","standard_charge|discounted_cash","payer_name","plan_name",
 "standard_charge","standard_charge|percent","standard_charge|min","standard_charge|max",
 "standard_charge|contracting_method","additional_generic_notes","source_file"
]

TARGET = [
 "hospital_name","hospital_location","last_updated_on","version","financial_aid_policy",
 "license_number","setting","billing_class","description",
 "code_1","code_1_type","code_2","code_2_type","code_3","code_3_type",
 "drug_unit_of_measurement","drug_type_of_measurement","modifiers",
 "standard_charge_gross","standard_charge_discounted_cash","payer_name","plan_name",
 "standard_charge","standard_charge_percent","standard_charge_min","standard_charge_max",
 "contracting_method","additional_generic_notes","source_file"
]

def env(name, default=None):
    v = os.getenv(name, default)
    if v is None:
        print(f"Missing env var {name}", file=sys.stderr); sys.exit(1)
    return v

def main():
    if len(sys.argv) != 2:
        print("Usage: python etl\\load_postgres.py data\\staging\\<file>.csv"); sys.exit(1)
    infile = Path(sys.argv[1])
    if not infile.exists(): raise SystemExit(f"Not found: {infile}")

    conn = psycopg2.connect(
        host=env("PGHOST","127.0.0.1"),
        port=int(env("PGPORT","5433")),
        user=env("PGUSER","hpt_owner"),
        password=env("PGPASSWORD"),
        dbname=env("PGDATABASE","hpt_db"),
    )
    with conn, conn.cursor() as cur, infile.open("r", encoding="utf-8") as f:
        # use COPY for speed
        copy_sql = f"""
            COPY hpt.standard_charge ({", ".join(TARGET)})
            FROM STDIN WITH (FORMAT csv, HEADER true)
        """
        cur.copy_expert(copy_sql, f)
    print(f"✓ loaded {infile}")

if __name__ == "__main__":
    main()
