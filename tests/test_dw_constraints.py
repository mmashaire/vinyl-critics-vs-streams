from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/vinyl_dw.sqlite")


def _connect() -> sqlite3.Connection:
    assert DB_PATH.exists(), f"Warehouse DB not found: {DB_PATH} (run scripts/run_pipeline.py)"
    return sqlite3.connect(DB_PATH)


def _scalar_int(con: sqlite3.Connection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def test_required_tables_are_nonempty() -> None:
    con = _connect()
    try:
        for table in ["pitchfork_reviews", "pitchfork_review_artists", "spotify_youtube_clean", "dim_artist"]:
            n = _scalar_int(con, f"SELECT COUNT(*) FROM {table}")
            assert n > 0, f"Expected table '{table}' to have rows, got {n}"
    finally:
        con.close()


def test_pitchfork_score_range_is_sane() -> None:
    """
    Pitchfork scores are expected to be in [0, 10].
    If your source guarantees a narrower range, you can tighten it later.
    """
    con = _connect()
    try:
        bad = _scalar_int(
            con,
            """
            SELECT COUNT(*)
            FROM pitchfork_reviews
            WHERE score IS NULL OR score < 0 OR score > 10
            """,
        )
        assert bad == 0, f"Found {bad} pitchfork_reviews rows with score outside [0, 10] or NULL"
    finally:
        con.close()


def test_dim_artist_norm_is_present_and_unique() -> None:
    con = _connect()
    try:
        blank = _scalar_int(
            con,
            """
            SELECT COUNT(*)
            FROM dim_artist
            WHERE artist_norm IS NULL OR TRIM(artist_norm) = ''
            """,
        )
        assert blank == 0, f"dim_artist has {blank} blank artist_norm values"

        dups = _scalar_int(
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
        assert dups == 0, f"dim_artist has {dups} duplicate artist_norm values"
    finally:
        con.close()


def test_bridge_has_no_orphan_reviews() -> None:
    """
    Every bridge row should point to an existing review.
    This protects against partial loads / bad joins.
    """
    con = _connect()
    try:
        orphans = _scalar_int(
            con,
            """
            SELECT COUNT(*)
            FROM pitchfork_review_artists pra
            LEFT JOIN pitchfork_reviews pr ON pr.reviewid = pra.reviewid
            WHERE pr.reviewid IS NULL
            """,
        )
        assert orphans == 0, f"Bridge has {orphans} orphan rows (no matching pitchfork_reviews row)"
    finally:
        con.close()


def test_bridge_artist_present_and_not_blank() -> None:
    """
    In this project, the bridge stores artist names as raw text (column: artist).
    Enforce that it's not missing or whitespace.
    """
    con = _connect()
    try:
        blank = _scalar_int(
            con,
            """
            SELECT COUNT(*)
            FROM pitchfork_review_artists
            WHERE artist IS NULL OR TRIM(artist) = ''
            """,
        )
        assert blank == 0, f"pitchfork_review_artists has {blank} blank artist values"
    finally:
        con.close()


def test_bridge_no_duplicate_review_artist_pairs() -> None:
    """
    Each (reviewid, artist) pair should be unique.
    Prevents accidental duplication during bridge construction.
    """
    con = _connect()
    try:
        dups = _scalar_int(
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
        assert dups == 0, f"pitchfork_review_artists has duplicate (reviewid, artist) pairs: {dups}"
    finally:
        con.close()
