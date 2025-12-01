# scripts/load_dim_artist.py
from pathlib import Path
import sqlite3
import pandas as pd

DB = Path("data/processed/vinyl_dw.sqlite")
MAP = Path("data/processed/artist_map.csv")

def main():
    if not DB.exists():
        raise FileNotFoundError(f"Missing warehouse DB: {DB}")
    if not MAP.exists():
        raise FileNotFoundError(f"Missing artist map CSV: {MAP}")

    df = pd.read_csv(MAP)

    # Keep only columns we intend to publish into the dim table
    keep = [
        "artist", "artist_norm", "n_reviews",
        "artist_spotify", "match_type", "score", "spotify_artist_id"
    ]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Basic hygiene
    df["artist"] = df["artist"].astype(str)
    df["artist_norm"] = df["artist_norm"].astype(str)

    if "match_type" not in df.columns:
        df["match_type"] = ""
    else:
        df["match_type"] = df["match_type"].fillna("")

    if "score" not in df.columns:
        df["score"] = 0.0
    else:
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0.0)

    # Optional columns that might be missing in the map
    if "spotify_artist_id" not in df.columns:
        df["spotify_artist_id"] = pd.NA
    if "n_reviews" not in df.columns:
        df["n_reviews"] = pd.NA

    con = sqlite3.connect(DB)
    try:
        # Stage into a temp table first
        df.to_sql("dim_artist_stage", con, if_exists="replace", index=False)

        # Build the final constrained table. If multiple rows share artist_norm,
        # keep the best by score desc, then n_reviews desc.
        con.executescript("""
        DROP TABLE IF EXISTS dim_artist;

        CREATE TABLE dim_artist (
          artist_key        INTEGER PRIMARY KEY AUTOINCREMENT,
          artist            TEXT NOT NULL,
          artist_norm       TEXT NOT NULL UNIQUE,
          n_reviews         INTEGER,
          artist_spotify    TEXT,
          match_type        TEXT,
          score             REAL,
          spotify_artist_id TEXT
        );

        INSERT INTO dim_artist (artist, artist_norm, n_reviews, artist_spotify, match_type, score, spotify_artist_id)
        WITH ranked AS (
          SELECT
            s.*,
            ROW_NUMBER() OVER (
              PARTITION BY s.artist_norm
              ORDER BY s.score DESC, COALESCE(s.n_reviews,0) DESC
            ) AS rn
          FROM dim_artist_stage s
        )
        SELECT artist, artist_norm, n_reviews, artist_spotify, match_type, score, spotify_artist_id
        FROM ranked
        WHERE rn = 1;

        CREATE INDEX IF NOT EXISTS ix_dim_artist_norm ON dim_artist(artist_norm);
        CREATE INDEX IF NOT EXISTS ix_dim_artist_spotify ON dim_artist(artist_spotify);
        """)

        # Coverage stats: how many Pitchfork bridge artists map to a Spotify name
        cur = con.cursor()
        total_artists = cur.execute("""
            SELECT COUNT(DISTINCT artist) FROM pitchfork_review_artists
        """).fetchone()[0]

        mapped_artists = cur.execute("""
            SELECT COUNT(DISTINCT pra.artist)
            FROM pitchfork_review_artists pra
            JOIN dim_artist da ON da.artist = pra.artist
            WHERE da.artist_spotify IS NOT NULL
        """).fetchone()[0]

        exact_mapped = cur.execute("""
            SELECT COUNT(DISTINCT pra.artist)
            FROM pitchfork_review_artists pra
            JOIN dim_artist da ON da.artist = pra.artist
            WHERE da.match_type = 'exact_norm'
        """).fetchone()[0]

        fuzzy_mapped = cur.execute("""
            SELECT COUNT(DISTINCT pra.artist)
            FROM pitchfork_review_artists pra
            JOIN dim_artist da ON da.artist = pra.artist
            WHERE da.match_type = 'jaccard_token'
        """).fetchone()[0]

        print(f"[ok] dim_artist loaded: {cur.execute('SELECT COUNT(*) FROM dim_artist').fetchone()[0]:,} rows")
        if total_artists:
            pct = mapped_artists / total_artists
            print(f"[coverage] pitchfork artists mapped: {mapped_artists:,}/{total_artists:,} ({pct:.1%})")
        print(f"[coverage] exact_norm: {exact_mapped:,} | jaccard_token: {fuzzy_mapped:,}")

        # Quick sanity: uniqueness and nulls
        dup_norms = cur.execute("""
            SELECT COUNT(*) FROM (
              SELECT artist_norm, COUNT(*) c FROM dim_artist GROUP BY artist_norm HAVING c > 1
            )
        """).fetchone()[0]
        null_norms = cur.execute("""
            SELECT COUNT(*) FROM dim_artist WHERE artist_norm IS NULL OR TRIM(artist_norm)=''
        """).fetchone()[0]

        if dup_norms != 0:
            raise RuntimeError(f"Duplicate artist_norm keys in dim_artist: {dup_norms}")
        if null_norms != 0:
            raise RuntimeError(f"Null/blank artist_norm in dim_artist: {null_norms}")

        con.commit()
    finally:
        con.close()

if __name__ == "__main__":
    main()
