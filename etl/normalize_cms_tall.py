from __future__ import annotations
import argparse, os, re
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
STAGING = ROOT / "data" / "staging"
STAGING.mkdir(parents=True, exist_ok=True)

# Canonical columns (subset of CMS Tall)
CANON = [
  "hospital_name","hospital_location","last_updated_on","version","financial_aid_policy",
  "license_number","setting","billing_class","description",
  "code|1","code|1|type","code|2","code|2|type","code|3","code|3|type",
  "drug_unit_of_measurement","drug_type_of_measurement","modifiers",
  "standard_charge|gross","standard_charge|discounted_cash",
  "payer_name","plan_name","standard_charge","standard_charge|percent",
  "standard_charge|min","standard_charge|max","standard_charge|contracting_method",
  "additional_generic_notes"
]

def normalize_headers(cols):
    # Lowercase, strip, collapse spaces
    def norm(c): return re.sub(r"\s+", "_", c.strip().lower())
    return [norm(c) for c in cols]

def detect_cms_tall(cols_set: set) -> bool:
    must = {"description","payer_name","plan_name"}
    any_one_of = {"standard_charge","standard_charge|gross","standard_charge|discounted_cash"}
    return must.issubset(cols_set) and len(cols_set.intersection(any_one_of)) > 0

def read_in_chunks(path: Path, chunksize=250_000):
    try:
        for chunk in pd.read_csv(path, dtype=str, chunksize=chunksize, low_memory=False):
            yield chunk
    except UnicodeDecodeError:
        for chunk in pd.read_csv(path, dtype=str, chunksize=chunksize, low_memory=False, encoding="latin1"):
            yield chunk

def normalize_file(inpath: Path, hospital_id: str) -> Path:
    # Peek header
    head = pd.read_csv(inpath, nrows=0)
    head.columns = normalize_headers(list(head.columns))
    cols_set = set(head.columns)

    if not detect_cms_tall(cols_set):
        # Write a small heads-up file with the columns we saw so you can map later
        note = STAGING / f"{hospital_id}__UNSUPPORTED_HEADERS.txt"
        note.write_text("Unsupported/non-CMS-tall header set:\n" + "\n".join(sorted(cols_set)), encoding="utf-8")
        print(f"[!] {hospital_id}: not CMS CSV Tall. Wrote {note}. Open it and we’ll add a mapper.")
        return note

    out_csv = STAGING / f"{hospital_id}__cms_tall_normalized.csv"
    with out_csv.open("w", encoding="utf-8", newline="") as f_out:
        f_out.write(",".join(CANON + ["source_file"]) + "\n")
        for chunk in read_in_chunks(inpath):
            chunk.columns = normalize_headers(list(chunk.columns))
            df = pd.DataFrame(columns=CANON)
            # Copy over any matching columns, leave others empty
            for c in CANON:
                if c in chunk.columns: df[c] = chunk[c]
                else: df[c] = None
            df["source_file"] = str(inpath.name)
            # Light cleanup
            num_cols = ["standard_charge|gross","standard_charge|discounted_cash","standard_charge",
                        "standard_charge|percent","standard_charge|min","standard_charge|max"]
            for c in num_cols:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            # Write append
            df.to_csv(f_out, header=False, index=False)
    print(f"✓ normalized → {out_csv}")
    return out_csv

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Path to raw CSV (downloaded)")
    ap.add_argument("--hospital-id", required=True, help="ID matching sources.csv")
    a = ap.parse_args()
    out = normalize_file(Path(a.file), a.hospital_id)
    print(out)

if __name__ == "__main__":
    main()
