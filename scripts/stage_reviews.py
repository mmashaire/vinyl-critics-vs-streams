from pathlib import Path
import pandas as pd

SRC = Path("data/interim/pitchfork_reviews.csv")
OUT = Path("data/interim/pitchfork_reviews_typed.csv")

df = pd.read_csv(SRC)

# Convert strings → datetime; invalids become NaT so we can count them
df["pub_date"] = pd.to_datetime(df["pub_date"], errors="coerce")

# Enforce compact, explicit dtypes (saves space; prevents silent float/int drift)
df["best_new_music"] = df["best_new_music"].astype("int8")
df["pub_year"] = df["pub_year"].astype("int16")
df["pub_month"] = df["pub_month"].astype("int8")
df["pub_day"] = df["pub_day"].astype("int8")
df["score"] = df["score"].astype("float32")

# Guardrails: Pitchfork scores are 0.0–10.0
bad = df[(df["score"] < 0) | (df["score"] > 10) | (df["score"].isna())]
if len(bad):
    raise ValueError(f"Score domain violated on {len(bad)} rows")

null_dates = df["pub_date"].isna().sum()
print(f"[check] null pub_date after parse: {null_dates}")

df.to_csv(OUT, index=False)
print(f"[ok] wrote {OUT} with {len(df):,} rows")
