from pathlib import Path
import unicodedata
import pandas as pd
import re

SRC = Path("data/interim/pitchfork_reviews_typed.csv")
OUT = Path("data/interim/pitchfork_review_artists.csv")

# Split only on true separators; never split inside words like "Islands" or "Hands".
SEP_RE = re.compile(r"\s*(?:,|&|/|\+|\band\b|\bfeat\.?\b|\bfeaturing\b|\bwith\b)\s*", re.IGNORECASE)

# Allow a few legitimate short names; drop other 1–2 char tokens.
VALID_SHORT = {"x", "u2", "m", "bj", "om", "vv", "xx"}

def clean_token(s: str) -> str:
    s = unicodedata.normalize("NFKC", (s or "")).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def split_artists(s: str) -> list[str]:
    s = clean_token(s)
    if not s:
        return []
    parts = [clean_token(p) for p in SEP_RE.split(s) if p]
    # drop junk tokens like lone "s"
    parts = [p for p in parts if len(p) > 2 or p.casefold() in VALID_SHORT]
    return parts

print(f"[info] source: {SRC.resolve()}")
if not SRC.exists():
    raise FileNotFoundError(f"Missing {SRC}. Run stage_reviews.py first.")

df = pd.read_csv(SRC, dtype={"reviewid": "int64", "artist": "string"})
df = df[["reviewid", "artist"]].copy()

df["artist"] = df["artist"].fillna("").map(split_artists)
df = df.explode("artist").dropna(subset=["artist"])
df["artist"] = df["artist"].astype(str).str.strip()
df = df[df["artist"] != ""]

before = len(df)
df = df.drop_duplicates(["reviewid", "artist"]).reset_index(drop=True)

print(f"[ok] exploded pairs: {before:,} -> after de-dup: {len(df):,}")
print(f"[ok] example:\n{df.head(5)}")

OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)
print(f"[ok] wrote bridge -> {OUT.resolve()} ({OUT.stat().st_size:,} bytes)") 
