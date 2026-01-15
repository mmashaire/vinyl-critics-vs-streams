# scripts/validate_dw.py
import sqlite3
from pathlib import Path


DB = Path("data/processed/vinyl_dw.sqlite")

REQUIRED_TABLES = [
    "pitchfork_reviews",
    "pitchfork_review_artists",
    "spotify_youtube_clean",
    "dim_artist",
]


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def count_rows(con: sqlite3.Connection, table: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def main() -> None:
    if not DB.exists():
        raise FileNotFoundError(f"Missing warehouse DB: {DB}")

    con = sqlite3.connect(DB)
    try:
        # Basic presence checks
        missing = [t for t in REQUIRED_TABLES if not table_exists(con, t)]
        if missing:
            raise RuntimeError(f"Missing required tables: {missing}")

        # Basic non-empty checks
        for t in REQUIRED_TABLES:
            n = count_rows(con, t)
            if n <= 0:
                raise RuntimeError(f"Table is empty: {t}")
            print(f"[ok] {t}: {n:,} rows")

        # dim_artist sanity: no blank norms and no duplicates
        blank_norms = con.execute(
            "SELECT COUNT(*) FROM dim_artist WHERE artist_norm IS NULL OR TRIM(artist_norm)=''"
        ).fetchone()[0]
        if blank_norms:
            raise RuntimeError(f"dim_artist has blank artist_norm rows: {blank_norms}")

        dup_norms = con.execute("""
            SELECT COUNT(*) FROM (
              SELECT artist_norm, COUNT(*) c
              FROM dim_artist
              GROUP BY artist_norm
              HAVING c > 1
            )
        """).fetchone()[0]
        if dup_norms:
            raise RuntimeError(f"dim_artist has duplicate artist_norm keys: {dup_norms}")

        # Bridge orphan check: every bridge row should have a review row
        orphans = con.execute("""
            SELECT COUNT(*)
            FROM pitchfork_review_artists pra
            LEFT JOIN pitchfork_reviews pr ON pr.reviewid = pra.reviewid
            WHERE pr.reviewid IS NULL
        """).fetchone()[0]
        if orphans:
            raise RuntimeError(f"Bridge has orphan rows (no matching review): {orphans}")

        print("[ok] warehouse validation passed")

    finally:
        con.close()


if __name__ == "__main__":
    main()
