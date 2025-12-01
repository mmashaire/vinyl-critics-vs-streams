import sqlite3, pandas as pd, glob, os
from pathlib import Path

DB = Path(r"D:\Projects\vinyl-critics-vs-streams\data\processed\vinyl_dw.sqlite")
IN_DIR = Path(r"D:\Projects\vinyl-critics-vs-streams\data\interim")

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
