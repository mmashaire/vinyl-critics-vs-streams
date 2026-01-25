# scripts/validate_dw.py
from __future__ import annotations

import sqlite3
from pathlib import Path

DB = Path("data/processed/vinyl_dw.sqlite")

REQUIRED_TABLES = [
    "pitchfork_reviews",
    "pitchfork_review_artists",
    "spotify_youtube_clean",
    "dim_artist",
]

# These are in your README; enforce them so the repo stays honest.
REQUIRED_VIEWS = [
    "vw_review_with_artist",
    "vw_unmatched_artists",
    "vw_artist_summary",
    "vw_artist_streams",
    "vw_artist_critics_vs_streams",
]


def object_exists(con: sqlite3.Connection, obj_type: str, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type=? AND name=?",
        (obj_type, name),
    ).fetchone()
    return row is not None


def count_rows(con: sqlite3.Connection, table: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def scalar_int(con: sqlite3.Connection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def main() -> None:
    if not DB.exists():
        raise FileNotFoundError(f"Missing warehouse DB: {DB}")

    con = sqlite3.connect(DB)
    try:
        # -----------------------------
        # Presence checks (tables/views)
        # -----------------------------
        missing_tables = [t for t in REQUIRED_TABLES if not object_exists(con, "table", t)]
        if missing_tables:
            raise RuntimeError(f"Missing required tables: {missing_tables}")

        missing_views = [v for v in REQUIRED_VIEWS if not object_exists(con, "view", v)]
        if missing_views:
            raise RuntimeError(f"Missing required views: {missing_views}")

        # -----------------------------
        # Non-empty checks
        # -----------------------------
        for t in REQUIRED_TABLES:
            n = count_rows(con, t)
            if n <= 0:
                raise RuntimeError(f"Table is empty: {t}")
            print(f"[ok] {t}: {n:,} rows")

        # -----------------------------
        # dim_artist sanity checks
        # -----------------------------
        blank_norms = scalar_int(
            con,
            """
            SELECT COUNT(*)
            FROM dim_artist
            WHERE artist_norm IS NULL OR TRIM(artist_norm)=''
            """,
        )
        if blank_norms:
            raise RuntimeError(f"dim_artist has blank artist_norm rows: {blank_norms}")

        dup_norms = scalar_int(
            con,
            """
            SELECT COUNT(*) FROM (
              SELECT artist_norm, COUNT(*) c
              FROM dim_artist
              GROUP BY artist_norm
              HAVING c > 1
            )
            """,
        )
        if dup_norms:
            raise RuntimeError(f"dim_artist has duplicate artist_norm keys: {dup_norms}")

        # -----------------------------
        # Bridge integrity checks
        # pitchfork_review_artists(reviewid, artist)
        # -----------------------------
        # Every bridge row should point to an existing review.
        orphans = scalar_int(
            con,
            """
            SELECT COUNT(*)
            FROM pitchfork_review_artists pra
            LEFT JOIN pitchfork_reviews pr ON pr.reviewid = pra.reviewid
            WHERE pr.reviewid IS NULL
            """,
        )
        if orphans:
            raise RuntimeError(f"Bridge has orphan rows (no matching review): {orphans}")

        # Artist text should not be blank.
        blank_artists = scalar_int(
            con,
            """
            SELECT COUNT(*)
            FROM pitchfork_review_artists
            WHERE artist IS NULL OR TRIM(artist)=''
            """,
        )
        if blank_artists:
            raise RuntimeError(f"Bridge has blank artist values: {blank_artists}")

        # No duplicate (reviewid, artist) pairs.
        dup_pairs = scalar_int(
            con,
            """
            SELECT COUNT(*) FROM (
              SELECT reviewid, TRIM(artist) AS artist_clean, COUNT(*) c
              FROM pitchfork_review_artists
              GROUP BY reviewid, artist_clean
              HAVING c > 1
            )
            """,
        )
        if dup_pairs:
            raise RuntimeError(f"Bridge has duplicate (reviewid, artist) pairs: {dup_pairs}")

        # -----------------------------
        # View smoke tests (must execute)
        # -----------------------------
        for v in REQUIRED_VIEWS:
            con.execute(f"SELECT * FROM {v} LIMIT 1").fetchall()
        print("[ok] views query smoke test passed")

        print("[ok] warehouse validation passed")

    finally:
        con.close()


if __name__ == "__main__":
    main()
