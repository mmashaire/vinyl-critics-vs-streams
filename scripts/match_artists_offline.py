from __future__ import annotations

from pathlib import Path
from collections import defaultdict
import unicodedata
import pandas as pd
import re
import json

# Repo-local IO only (no secrets / network)
UNIVERSE_CSV = Path("data/processed/artist_universe.csv")
RAW_DIRS = [Path("data/raw/spotify_attributes"), Path("data/raw/spotify_youtube")]
OUT_CSV = Path("data/processed/artist_map.csv")

# Only accept fuzzy matches at/above this confidence
MIN_FUZZY = 0.65


# Normalization: stable keys for matching
def norm(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("µ", "mu").replace("μ", "mu")           # μ/µ -> mu
    s = re.sub(r"\b(feat\.?|ft\.?|featuring)\b.*$", "", s, flags=re.I)
    s = re.sub(r"[’`']", "", s)                           # unify apostrophes
    s = re.sub(r"[\s._\-]+", " ", s).strip()
    return s.casefold()


LABEL_RE = re.compile(
    r"\b(records?|recordings?|music|music\s+group|entertainment|studios?|llc|inc\.?|ltd\.?)\b",
    re.I,
)
def looks_like_label(name: str) -> bool:
    return bool(LABEL_RE.search(name))


def tokenize(s: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", s))


def jaccard(a: str, b: str) -> float:
    ta, tb = tokenize(a), tokenize(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def load_spotify_candidates() -> pd.DataFrame:
    """
    Scan local CSVs for likely artist columns. No network calls.
    """
    candidate_cols = {"artist", "artist_name", "artists", "primary_artist"}
    names: set[str] = set()

    for root in RAW_DIRS:
        if not root.exists():
            continue
        for p in root.rglob("*.csv"):
            try:
                cols = pd.read_csv(p, nrows=0).columns
                wanted = [c for c in cols if str(c).lower() in candidate_cols]
                if not wanted:
                    continue
                df = pd.read_csv(p, usecols=wanted)
            except Exception:
                continue

            for c in df.columns:
                for v in df[c].dropna().astype(str).values:
                    v = v.strip()
                    if not v:
                        continue
                    # JSON list like '["Drake","21 Savage"]'
                    try:
                        obj = json.loads(v)
                        if isinstance(obj, list):
                            for w in obj:
                                w = str(w).strip()
                                if w and not looks_like_label(w):
                                    names.add(w)
                            continue
                    except Exception:
                        pass
                    # Delimited
                    if ";" in v:
                        for w in (x.strip() for x in v.split(";")):
                            if w and not looks_like_label(w):
                                names.add(w)
                    else:
                        if not looks_like_label(v):
                            names.add(v)

    # Collapse to one display value per normalized key
    canon: dict[str, str] = {}
    for raw in names:
        k = norm(raw)
        if k and k not in canon:
            canon[k] = raw

    cand = pd.DataFrame({"artist_norm": list(canon.keys())})
    cand["artist_spotify"] = cand["artist_norm"].map(canon.get)
    return cand


def bucket_by_prefix(keys: list[str]) -> dict[str, list[str]]:
    """
    Light bucketing by first alnum char to avoid O(N^2) when fuzzing.
    """
    buckets: dict[str, list[str]] = defaultdict(list)
    for k in keys:
        m = re.search(r"[a-z0-9]", k)
        b = m.group(0) if m else "_"
        buckets[b].append(k)
    return buckets


def main() -> None:
    if not UNIVERSE_CSV.exists():
        raise FileNotFoundError(f"Missing {UNIVERSE_CSV}. Build it first.")

    u = pd.read_csv(UNIVERSE_CSV)
    cols = [c for c in ["artist", "artist_norm", "n_reviews", "is_various", "is_suspicious_token"] if c in u.columns]
    u = u[cols].copy()
    if "is_various" in u.columns:
        u = u[u["is_various"] == False]
    if "is_suspicious_token" in u.columns:
        u = u[u["is_suspicious_token"] == False]
    if "artist_norm" not in u.columns:
        u["artist_norm"] = u["artist"].astype(str).map(norm)

    print(f"[info] universe artists: {len(u):,}")

    cand = load_spotify_candidates()
    if cand.empty:
        out = u.assign(artist_spotify=pd.NA, match_type="none", score=0.0, spotify_artist_id=pd.NA)
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(OUT_CSV, index=False)
        print(f"[warn] no local spotify candidates found; wrote skeleton {OUT_CSV} ({len(out):,} rows)")
        return

    print(f"[info] candidate names found: {len(cand):,}")

    # Exact normalized match
    left = u.merge(cand.drop_duplicates("artist_norm"), on="artist_norm", how="left")
    left["match_type"] = left["artist_spotify"].notna().map({True: "exact_norm", False: ""})
    left["score"] = left["match_type"].map({"exact_norm": 1.0}).fillna(0.0)

    # Fuzzy for the rest (token Jaccard within prefix bucket)
    missing = left["artist_spotify"].isna()
    if missing.any():
        cand_keys = cand["artist_norm"].unique().tolist()
        buckets = bucket_by_prefix(cand_keys)
        key_to_disp = cand.set_index("artist_norm")["artist_spotify"].to_dict()

        def best(k: str) -> tuple[str | None, float]:
            if not isinstance(k, str) or not k:
                return None, 0.0
            m = re.search(r"[a-z0-9]", k)
            b = m.group(0) if m else "_"
            pool = buckets.get(b, cand_keys)
            bk, bs = None, 0.0
            for c in pool:
                s = jaccard(k, c)
                if s > bs:
                    bk, bs = c, s
            return bk, bs

        miss_keys = left.loc[missing, "artist_norm"].tolist()
        bests = [best(k) for k in miss_keys]
        best_norms = [bk for bk, _ in bests]
        best_scores = [bs for _, bs in bests]
        best_disp = [key_to_disp.get(k) for k in best_norms]

        left.loc[missing, "artist_spotify"] = best_disp
        left.loc[missing, "score"] = best_scores
        left.loc[missing, "match_type"] = "jaccard_token"

        # Reject weak fuzzies
        weak = (left["match_type"].eq("jaccard_token")) & (left["score"] < MIN_FUZZY)
        left.loc[weak, ["artist_spotify", "match_type", "score"]] = [pd.NA, "", 0.0]

    # Placeholder for future API enrichment (keep public-safe)
    left["spotify_artist_id"] = pd.NA

    # De-dupe by key: keep best-scoring, then higher n_reviews
    if left["artist_norm"].duplicated().any():
        left = (
            left.sort_values(["artist_norm", "score", "n_reviews"], ascending=[True, False, False])
            .drop_duplicates(subset=["artist_norm"], keep="first")
        )

    # Summary
    n_total = len(left)
    n_exact = int((left["match_type"] == "exact_norm").sum())
    n_fuzzy = int((left["match_type"] == "jaccard_token").sum())
    print(f"[summary] matched exact={n_exact} ({n_exact/n_total:.1%}), fuzzy={n_fuzzy} ({n_fuzzy/n_total:.1%}), total={n_total}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    left.sort_values(["score", "n_reviews"], ascending=[False, False]).to_csv(OUT_CSV, index=False)
    print(f"[ok] wrote {OUT_CSV} ({len(left):,} rows)")
    print(left.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
