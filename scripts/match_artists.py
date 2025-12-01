# scripts/match_artists.py
from pathlib import Path
import pandas as pd
from rapidfuzz import process, fuzz

OUT_MAP = Path("data/overrides/artist_map.csv")
OUT_REVIEW = Path("data/overrides/artist_review_queue.csv")
BLOCKLIST = {"various artists", "soundtrack", "original soundtrack", "va", "ost"}

pitchfork = pd.read_csv("data/interim/pitchfork_artists.csv", dtype=str)
spotify = pd.read_csv("data/interim/spotify_youtube_clean.csv", dtype=str)

pf_names = (pitchfork["artist"].fillna("").str.strip().str.replace(r"\s+", " ", regex=True))
sp_names = (spotify["artist"].fillna("").str.strip().str.replace(r"\s+", " ", regex=True))
pf_names = pf_names[pf_names.ne("")].drop_duplicates().tolist()
sp_names = pd.Series(sp_names[sp_names.ne("")].drop_duplicates().tolist()).tolist()

def clean(name: str) -> str:
    name = name.lower().strip()
    if name.startswith("the "):
        name = name[4:]
    return "".join(ch for ch in name if ch.isalnum() or ch.isspace())

def ok_pair(a_raw: str, b_raw: str, score: int) -> bool:
    a = a_raw.lower(); b = b_raw.lower()
    if a in BLOCKLIST or b in BLOCKLIST:
        return False
    if a[0] != b[0]:                 # first-letter must match
        return False
    la, lb = len(a), len(b)
    r = (min(la, lb) / max(la, lb)) if max(la, lb) else 0
    if r < 0.6:                       # avoid crazy length mismatches
        return False
    # token overlap (cheap Jaccard on words)
    ta, tb = set(a.split()), set(b.split())
    if ta and tb and (len(ta & tb) / len(ta | tb)) < 0.25:
        # allow short-name exceptions if very high score
        if score < 95:
            return False
    return True

pf_clean = [clean(x) for x in pf_names]
sp_clean = [clean(x) for x in sp_names]

CUTOFF = 93
rows, review = [], []

for i, p in enumerate(pf_clean):
    res = process.extractOne(p, sp_clean, scorer=fuzz.WRatio, score_cutoff=CUTOFF)
    if res is None:
        continue
    match_clean, score, j = res
    a_raw = pf_names[i]; b_raw = sp_names[j]
    if ok_pair(a_raw, b_raw, int(score)):
        rows.append((a_raw, b_raw, int(score)))
    else:
        review.append((a_raw, b_raw, int(score), pf_clean[i], sp_clean[j]))

df = pd.DataFrame(rows, columns=["pitchfork_artist", "spotify_artist", "score"]).drop_duplicates()
df_review = pd.DataFrame(review, columns=["pf_artist","sp_artist","score","pf_clean","sp_clean"]).drop_duplicates()

OUT_MAP.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_MAP, index=False, encoding="utf-8")
df_review.to_csv(OUT_REVIEW, index=False, encoding="utf-8")

print(f"[ok] saved {len(df):,} high-confidence matches (≥{CUTOFF}) → {OUT_MAP}")
print(f"[review] queued {len(df_review):,} borderline pairs → {OUT_REVIEW}")
