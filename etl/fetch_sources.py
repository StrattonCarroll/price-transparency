from __future__ import annotations
import argparse, csv, datetime as dt, hashlib, json, os, sys
from pathlib import Path
from urllib.parse import urlparse
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

def find_manifest() -> Path:
    for p in (ROOT / "docs" / "sources.csv", ROOT / "etl" / "sources.csv"):
        if p.exists(): return p
    print("No manifest found at docs/sources.csv or etl/sources.csv", file=sys.stderr)
    sys.exit(1)

def read_manifest(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        return [{(k or "").strip(): (v or "").strip() for k, v in row.items()} for row in rdr]

def filter_rows(rows, ids, grep, enabled_only):
    out = []
    idset = set(map(str.strip, ids.split(","))) if ids else None
    for r in rows:
        if enabled_only:
            flag = r.get("enabled","").lower()
            if flag not in ("y","yes","1","true",""):  # blank counts as enabled
                continue
        if idset and r.get("hospital_id") not in idset:
            continue
        if grep:
            hay = " ".join([r.get("hospital_id",""), r.get("hospital_name",""), r.get("source_url","")]).lower()
            if grep.lower() not in hay: continue
        out.append(r)
    return out

def sha256_file(p: Path, bufsize=1024*1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(bufsize), b""):
            h.update(chunk)
    return h.hexdigest()

def download(url: str, dest: Path, overwrite=False) -> dict:
    if dest.exists() and not overwrite:
        return {"status":"exists","path":str(dest),"bytes":dest.stat().st_size}
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=90) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(1024*1024):
                if chunk: f.write(chunk)
    tmp.replace(dest)
    return {
        "status":"downloaded",
        "path":str(dest),
        "bytes":dest.stat().st_size,
        "sha256":sha256_file(dest),
        "source_url":url,
        "downloaded_at": dt.datetime.utcnow().isoformat() + "Z",
    }

def main():
    ap = argparse.ArgumentParser(description="Fetch price transparency sources from manifest")
    ap.add_argument("--ids", help="Comma-separated hospital_id list (e.g., nwh_bentonville,nwh_springdale)")
    ap.add_argument("--grep", help="Substring filter across id/name/url")
    ap.add_argument("--all", action="store_true", help="Fetch all manifest rows")
    ap.add_argument("--enabled-only", action="store_true", help="Only rows with enabled=Y (blank treated as Y)")
    ap.add_argument("--overwrite", action="store_true", help="Redownload even if file exists")
    ap.add_argument("--date-subdir", default=dt.date.today().isoformat(), help="Folder under data/raw/<id>/…")
    ap.add_argument("--manifest", help="Explicit path to manifest CSV")
    args = ap.parse_args()

    manifest = Path(args.manifest) if args.manifest else find_manifest()
    rows = read_manifest(manifest)
    chosen = filter_rows(
        rows,
        ids=args.ids,
        grep=args.grep,
        enabled_only=(args.enabled_only or (args.all and not args.ids and not args.grep))
    )
    if not chosen:
        print("No rows matched (use --ids, --grep, or --all).", file=sys.stderr)
        sys.exit(2)

    for r in chosen:
        hid, url = r.get("hospital_id") or "unknown", r.get("source_url")
        if not url:
            print(f"[SKIP] {hid}: missing source_url"); continue
        name = os.path.basename(urlparse(url).path) or f"{hid}.csv"
        out = RAW / hid / args.date_subdir / name
        print(f"→ {hid}\n   {url}\n   -> {out}")
        try:
            meta = download(url, out, overwrite=args.overwrite)
            with out.with_suffix(out.suffix + ".json").open("w", encoding="utf-8") as jf:
                json.dump(meta, jf, indent=2)
            print(f"   {meta['status']} ({meta['bytes']:,} bytes) sha256={meta.get('sha256','')[:12]}")
        except Exception as e:
            print(f"   [ERROR] {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
