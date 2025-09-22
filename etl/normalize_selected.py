from __future__ import annotations
import argparse, csv, sys
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
STAGING = ROOT / "data" / "staging"
MANIFESTS = [ROOT / "docs" / "sources.csv", ROOT / "etl" / "sources.csv"]

def read_manifest():
    for m in MANIFESTS:
        if m.exists():
            rows = []
            with m.open("r", encoding="utf-8", newline="") as f:
                rdr = csv.DictReader(f)
                for row in rdr:
                    rows.append({(k or "").strip(): (v or "").strip() for k, v in row.items()})
            return rows
    print("No manifest found at docs/sources.csv or etl/sources.csv", file=sys.stderr)
    sys.exit(1)

def list_ids(manifest, ids, grep, enabled_only):
    chosen = []
    idset = set(map(str.strip, ids.split(","))) if ids else None
    for r in manifest:
        if enabled_only and r.get("enabled","" ).lower() not in ("","y","yes","1","true"):
            continue
        if idset and r.get("hospital_id") not in idset:
            continue
        hay = " ".join([r.get("hospital_id",""), r.get("hospital_name",""), r.get("source_url","")]).lower()
        if grep and grep.lower() not in hay:
            continue
        chosen.append(r.get("hospital_id"))
    return sorted(set(filter(None, chosen)))

def latest_raw_csv(hid: str) -> Path | None:
    files = sorted(RAW.glob(f"{hid}/**/*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def main():
    ap = argparse.ArgumentParser(description="Normalize latest raw file per hospital")
    ap.add_argument("--ids", help="Comma-separated hospital_id list")
    ap.add_argument("--grep", help="Substring filter across id/name/url")
    ap.add_argument("--all", action="store_true", help="Normalize all (respecting --enabled-only unless --ids/--grep provided)")
    ap.add_argument("--enabled-only", action="store_true")
    args = ap.parse_args()

    manifest = read_manifest()
    targets = list_ids(manifest, ids=args.ids, grep=args.grep, enabled_only=(args.enabled_only or (args.all and not args.ids and not args.grep)))
    if not targets:
        print("No matching hospital_id(s).", file=sys.stderr); sys.exit(2)

    for hid in targets:
        latest = latest_raw_csv(hid)
        if not latest:
            print(f"[SKIP] {hid}: no raw CSV found in data/raw/{hid}/", file=sys.stderr)
            continue
        print(f"→ {hid}: {latest}")
        subprocess.run([sys.executable, str(ROOT / "etl" / "normalize_cms_tall.py"), "--file", str(latest), "--hospital-id", hid], check=True)

if __name__ == "__main__":
    main()