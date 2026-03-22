from __future__ import annotations

import sqlite3
import pytest
from pathlib import Path

DB_PATH = Path("data/processed/vinyl_dw.sqlite")

# Core tables your validation script already expects
REQUIRED_TABLES = {
    "pitchfork_reviews",
    "pitchfork_review_artists",
    "spotify_youtube_clean",
    "dim_artist",
}

# Views you claim exist in README.
# If some are not created yet in your repo, remove them here for now.
REQUIRED_VIEWS = {
    "vw_review_with_artist",
    "vw_unmatched_artists",
    "vw_artist_summary",
    "vw_artist_streams",
    "vw_artist_critics_vs_streams",
}


def _connect() -> sqlite3.Connection:
    # fail fast with a clearer message than sqlite3 does
    if not DB_PATH.exists():
        pytest.skip(f"Warehouse DB not found: {DB_PATH} (run scripts/run_pipeline.py)")
    return sqlite3.connect(DB_PATH)


def _list_objects(con: sqlite3.Connection, obj_type: str) -> set[str]:
    rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type=?",
        (obj_type,),
    ).fetchall()
    return {r[0] for r in rows}


def test_required_tables_exist() -> None:
    con = _connect()
    try:
        tables = _list_objects(con, "table")
        missing = REQUIRED_TABLES - tables
        assert not missing, f"Missing required tables: {sorted(missing)}"
    finally:
        con.close()


def test_required_views_exist() -> None:
    con = _connect()
    try:
        views = _list_objects(con, "view")
        missing = REQUIRED_VIEWS - views
        assert not missing, f"Missing required views: {sorted(missing)}"
    finally:
        con.close()


def test_views_execute_smoke() -> None:
    """
    Smoke test: views should be queryable.
    We don't validate business meaning here — just that they run.
    """
    con = _connect()
    try:
        for view in sorted(REQUIRED_VIEWS):
            con.execute(f"SELECT * FROM {view} LIMIT 1").fetchall()
    finally:
        con.close()
