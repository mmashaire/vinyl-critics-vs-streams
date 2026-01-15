import glob
import os
import sqlite3
from pathlib import Path

import pandas as pd


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


DB = repo_root() / "data" / "processed" / "vinyl_dw.sqlite"
IN_DIR = repo_root() / "data" / "interim"

DB.parent.mkdir(parents=True, exist_ok=True)
con = sqlite3.connect(DB)

for f in glob.glob(str(IN_DIR / "*.csv")):
    name = os.path.splitext(os.path.basename(f))[0]
    df = pd.read_csv(f, low_memory=False)
    df.to_sql(name, con, if_exists="replace", index=False)
    print(f"Loaded {len(df):,} rows into table {name}")

con.execute("PRAGMA vacuum;")
con.close()
print(f"Warehouse ready -> {DB}")
