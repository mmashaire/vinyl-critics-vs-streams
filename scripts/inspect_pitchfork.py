from pathlib import Path
import sqlite3
import pandas as pd

# Input DB. Keep this as an absolute path to avoid surprises when running from different folders.
DB_PATH = Path(r"D:\Projects\vinyl-critics-vs-streams\data\raw\pitchfork\database.sqlite")

# Quick gate: fail fast if the source isn't where we think it is.
if not DB_PATH.exists():
    raise FileNotFoundError(f"Database not found at: {DB_PATH}")

# Keep samples small; this script is for inspection, not ETL.
SAMPLE_TABLE = "reviews"
SAMPLE_ROWS = 10

# Use a context manager so the DB handle closes even if something throws.
with sqlite3.connect(str(DB_PATH)) as con:
    # Discover available tables from SQLite's catalog.
    tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", con
    )["name"].tolist()
    print("Tables:", tables)

    if SAMPLE_TABLE not in tables:
        raise ValueError(f"Expected table '{SAMPLE_TABLE}' not found in DB.")

    # Peek at schema; PRAGMA is faster than SELECT * LIMIT 0 for column metadata.
    schema = pd.read_sql(f"PRAGMA table_info({SAMPLE_TABLE});", con)
    print(f"\nSchema for {SAMPLE_TABLE}:")
    # name, type, notnull are the bits you actually care about at this stage.
    print(schema[["cid", "name", "type", "notnull"]].to_string(index=False))

    # Grab a tiny slice so you can eyeball typical values without pulling the whole table.
    df = pd.read_sql(
        f"SELECT * FROM {SAMPLE_TABLE} LIMIT ?;", con, params=(SAMPLE_ROWS,)
    )
    print(f"\nSample rows from {SAMPLE_TABLE} ({len(df)} rows):")
    print(df)

    # Dtypes help catch surprises (e.g., scores as text, dates not parsed).
    print("\nInferred dtypes:")
    print(df.dtypes)
