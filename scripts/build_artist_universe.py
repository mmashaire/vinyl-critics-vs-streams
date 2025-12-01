from pathlib import Path
import sqlite3
import pandas as pd
import unicodedata
import re

DB = Path("data/processed/vinyl_dw.sqlite")
OUT = Path("data/processed/artist_universe.csv")

FEAT_PAT = re.compile(r"\b(feat\.?|ft\.?)\b.*$", flags=re.IGNORECASE)
AND_SPLIT = re.compile(r"\s*[&,+/]\s*")

def norm(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = FEAT_PAT.sub("", s)            # drop trailing features
    s = unicodedata.normalize("NFKC", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()

with sqlite3.connect(DB) as con:
    df = pd.read_sql_query("""
        SELECT artist, COUNT(*) AS n_reviews
        FROM pitchfork_review_artists
        GROUP BY artist
    """, con)

# Split obvious compound credits (A & B, A/B, A + B) into separate rows too
rows = []
for a, n in zip(df["artist"], df["n_reviews"]):
    parts = AND_SPLIT.split(a) if isinstance(a, str) else [a]
    for p in parts if parts else [a]:
        p = p.strip()
        if p:
            rows.append((p, n))

u = pd.DataFrame(rows, columns=["artist", "n_reviews"])
u = u.groupby("artist", as_index=False)["n_reviews"].sum()

u["artist_norm"] = u["artist"].apply(norm)
u["is_various"] = u["artist_norm"].isin({"various artists"})
u["is_suspicious_token"] = u["artist"].str.len().fillna(0) <= 2

# De-duplicate by normalized key but keep the max count for a stable first pass
u = (u.sort_values(["artist_norm", "n_reviews"], ascending=[True, False])
       .drop_duplicates(subset=["artist_norm"], keep="first"))

OUT.parent.mkdir(parents=True, exist_ok=True)
u.to_csv(OUT, index=False)
print(f"[ok] wrote {OUT} ({len(u):,} artists)")
print(u.sort_values("n_reviews", ascending=False).head(15).to_string(index=False))
