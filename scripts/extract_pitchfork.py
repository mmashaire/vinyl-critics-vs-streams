from pathlib import Path
import sqlite3
import pandas as pd
import sys
import json
import hashlib
from datetime import datetime, timezone

# Source SQLite dump (immutable input) and destination for extracted CSVs.
RAW_DB = Path(r"D:\Projects\vinyl-critics-vs-streams\data\raw\pitchfork\database.sqlite")
OUTDIR = Path(r"D:\Projects\vinyl-critics-vs-streams\data\interim")

# Expected tables for downstream joins. Keep tight to avoid drifting schemas.
TABLES = ["artists", "reviews", "genres", "labels", "years", "content"]

# Where we store a machine-readable snapshot of the export.
MANIFEST = OUTDIR / "pitchfork_export_meta.json"

print(f"[info] using db: {RAW_DB}")
print(f"[info] writing to: {OUTDIR.resolve()}")

# Fail fast if the dump is missing. Early exit beats partial, silent failures.
if not RAW_DB.exists():
    print(f"[error] database not found at: {RAW_DB}", file=sys.stderr)
    sys.exit(1)

# Idempotent: safe to run repeatedly in local dev or CI.
OUTDIR.mkdir(parents=True, exist_ok=True)

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Memory-safe hashing for reproducibility & drift detection."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

con = sqlite3.connect(str(RAW_DB))
manifest = {
    "source_db": str(RAW_DB),
    "source_db_mtime": datetime.fromtimestamp(RAW_DB.stat().st_mtime, tz=timezone.utc).isoformat(),
    "exported_at": datetime.now(tz=timezone.utc).isoformat(),
    "tables": {},
    "missing_tables": [],
    "totals": {"tables_exported": 0, "rows_exported": 0, "bytes_exported": 0},
    "notes": [
        "sha256 is of the CSV at export time; commit this manifest to detect drift.",
        "missing_tables indicates expected-but-absent tables in the SQLite dump."
    ],
}

try:
    # Inventory the schema once; avoids hard-coded assumptions about what's present.
    existing = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table';", con
    )["name"].tolist()
    print(f"[info] tables found: {existing}")

    # Surface drift explicitly: warn if the upstream dump changed.
    missing = [t for t in TABLES if t not in existing]
    if missing:
        print(f"[warn] missing tables in DB: {missing}")
        manifest["missing_tables"] = missing

    # Extract only the tables we actually have; skip missing gracefully.
    for t in TABLES:
        if t not in existing:
            continue

        df = pd.read_sql(f"SELECT * FROM {t}", con)
        out = OUTDIR / f"pitchfork_{t}.csv"
        # CSV is the neutral interchange format for the staging layer.
        df.to_csv(out, index=False)

        # Collect per-table metadata for auditing and reproducibility.
        bytes_out = out.stat().st_size
        file_hash = sha256_file(out)

        manifest["tables"][t] = {
            "csv_path": str(out),
            "rows": int(len(df)),
            "bytes": int(bytes_out),
            "sha256": file_hash,
        }
        manifest["totals"]["tables_exported"] += 1
        manifest["totals"]["rows_exported"] += int(len(df))
        manifest["totals"]["bytes_exported"] += int(bytes_out)

        print(f"[ok] {t}: {len(df):,} rows -> {out} ({bytes_out:,} bytes)")

finally:
    # Always close the handle; avoids locked files on Windows and flaky reruns.
    con.close()
    print("[info] closed sqlite connection")

# Write manifest last so a partial export won’t leave a misleading manifest.
with MANIFEST.open("w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, ensure_ascii=False)
print(f"[ok] wrote manifest -> {MANIFEST}")
