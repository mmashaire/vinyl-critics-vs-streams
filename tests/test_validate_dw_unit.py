from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

_VALIDATE_DW_PATH = Path(__file__).resolve().parents[1] / "scripts" / "validate_dw.py"
_SPEC = importlib.util.spec_from_file_location("validate_dw", _VALIDATE_DW_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load module spec from {_VALIDATE_DW_PATH}")

validate_dw = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = validate_dw
_SPEC.loader.exec_module(validate_dw)


def _make_con() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")


def _create_required_objects(con: sqlite3.Connection, *, with_rows: bool) -> None:
    con.executescript(
        """
        CREATE TABLE pitchfork_reviews (id INTEGER);
        CREATE TABLE pitchfork_review_artists (id INTEGER);
        CREATE TABLE spotify_youtube_clean (id INTEGER);
        CREATE TABLE dim_artist (id INTEGER);

        CREATE VIEW vw_review_with_artist AS SELECT 1 AS x;
        CREATE VIEW vw_unmatched_artists AS SELECT 1 AS x;
        CREATE VIEW vw_artist_summary AS SELECT 1 AS x;
        CREATE VIEW vw_artist_streams AS SELECT 1 AS x;
        CREATE VIEW vw_artist_critics_vs_streams AS SELECT 1 AS x;
        """
    )

    if with_rows:
        con.executescript(
            """
            INSERT INTO pitchfork_reviews VALUES (1);
            INSERT INTO pitchfork_review_artists VALUES (1);
            INSERT INTO spotify_youtube_clean VALUES (1);
            INSERT INTO dim_artist VALUES (1);
            """
        )


def test_presence_checks_pass_when_required_objects_exist() -> None:
    con = _make_con()
    try:
        _create_required_objects(con, with_rows=False)
        validate_dw._run_presence_checks(con)
    finally:
        con.close()


def test_presence_checks_fail_for_missing_table() -> None:
    con = _make_con()
    try:
        con.executescript(
            """
            CREATE TABLE pitchfork_reviews (id INTEGER);
            CREATE TABLE pitchfork_review_artists (id INTEGER);
            CREATE TABLE spotify_youtube_clean (id INTEGER);
            """
        )

        with pytest.raises(RuntimeError, match="Missing required tables"):
            validate_dw._run_presence_checks(con)
    finally:
        con.close()


def test_non_empty_checks_fail_when_required_table_is_empty() -> None:
    con = _make_con()
    try:
        _create_required_objects(con, with_rows=False)
        with pytest.raises(RuntimeError, match="Empty required table"):
            validate_dw._run_non_empty_checks(con)
    finally:
        con.close()


def test_non_empty_checks_pass_when_required_objects_have_rows() -> None:
    con = _make_con()
    try:
        _create_required_objects(con, with_rows=True)
        validate_dw._run_non_empty_checks(con)
    finally:
        con.close()


def test_run_sql_checks_reports_failing_check_name() -> None:
    con = _make_con()
    try:
        checks = [
            validate_dw.Check(name="should pass", sql="SELECT 0"),
            validate_dw.Check(name="should fail", sql="SELECT 2"),
        ]

        with pytest.raises(RuntimeError, match="should fail"):
            validate_dw._run_sql_checks(con, checks)
    finally:
        con.close()


def test_primary_key_column_returns_none_for_composite_key() -> None:
    con = _make_con()
    try:
        con.execute(
            """
            CREATE TABLE dim_artist (
                id INTEGER,
                source TEXT,
                PRIMARY KEY (id, source)
            )
            """
        )

        assert validate_dw._primary_key_column(con, "dim_artist") is None
    finally:
        con.close()
