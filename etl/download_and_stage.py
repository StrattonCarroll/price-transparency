from __future__ import annotations
import csv, hashlib, os, sys, time
from pathlib import Path
from urllib.parse import urlparse
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
STAGING = ROOT / "data" / "staging"
RAW.mkdir(parents=True, exist_ok=True)
STAGING.mkdir(parents=True, exist_ok=True)

def hash_file(p: Path, bufsize=1024*1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(bufsize)
            if not b: break
            h.update(b)
    return h.hexdigest()

def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk: f.write(chunk)
        tmp.replace(dest)
    return dest

def main():
    sources = ROOT / "etl" / "sources.csv"
    if not sources.exists():
        print("Missing etl/sources.csv", file=sys.stderr); sys.exit(1)

    with sources.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            hosp = row["hospital_id"]
            url = row["source_url"]
            ext = Path(urlparse(url).path).suffix.lower() or ".csv"
            out = RAW / f"{hosp}{ext}"
            print(f"↓ {hosp} from {url}")
            p = download(url, out)
            print(f"   saved: {p} ({p.stat().st_size:,} bytes) sha256={hash_file(p)[:12]}")

if __name__ == "__main__":
    main()
