# scripts/validate_dw.py
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

DB_PATH = Path("data/processed/vinyl_dw.sqlite")

REQUIRED_TABLES = (
    "pitchfork_reviews",
    "pitchfork_review_artists",
    "spotify_youtube_clean",
    "dim_artist",
)

REQUIRED_VIEWS = (
    "vw_review_with_artist",
    "vw_unmatched_artists",
    "vw_artist_summary",
    "vw_artist_streams",
    "vw_artist_critics_vs_streams",
)

# dim_artist naming can evolve. Accept a small set of sensible alternatives.
DIM_PITCHFORK_NAME_CANDIDATES = (
    "pitchfork_name",
    "pitchfork_artist",
    "pitchfork",
    "artist",  # common fallback
    "artist_name",  # common fallback
    "raw_artist",  # sometimes used for lineage
)


@dataclass(frozen=True)
class Check:
    name: str
    sql: str
    fail_if_nonzero: bool = True


def _object_exists(con: sqlite3.Connection, obj_type: str, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?",
        (obj_type, name),
    ).fetchone()
    return row is not None


def _scalar_int(con: sqlite3.Connection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _count_rows(con: sqlite3.Connection, name: str) -> int:
    return _scalar_int(con, f"SELECT COUNT(*) FROM {name}")


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _primary_key_column(con: sqlite3.Connection, table: str) -> Optional[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    pk_cols = [str(r[1]) for r in rows if int(r[5]) == 1]
    return pk_cols[0] if len(pk_cols) == 1 else None


def _first_existing(cols: set[str], candidates: Sequence[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def _run_presence_checks(con: sqlite3.Connection) -> None:
    missing_tables = [t for t in REQUIRED_TABLES if not _object_exists(con, "table", t)]
    if missing_tables:
        raise RuntimeError(f"Missing required tables: {', '.join(missing_tables)}")

    missing_views = [v for v in REQUIRED_VIEWS if not _object_exists(con, "view", v)]
    if missing_views:
        raise RuntimeError(f"Missing required views: {', '.join(missing_views)}")


def _run_non_empty_checks(con: sqlite3.Connection) -> None:
    for t in REQUIRED_TABLES:
        n = _count_rows(con, t)
        if n <= 0:
            raise RuntimeError(f"Empty required table: {t}")
        print(f"[ok] {t}: {n:,} rows")

    for v in REQUIRED_VIEWS:
        n = _count_rows(con, v)
        if n <= 0:
            raise RuntimeError(f"Empty required view: {v}")
        print(f"[ok] {v}: {n:,} rows")


def _run_sql_checks(con: sqlite3.Connection, checks: Iterable[Check]) -> None:
    failures: list[str] = []
    for chk in checks:
        value = _scalar_int(con, chk.sql)
        failed = (value != 0) if chk.fail_if_nonzero else (value == 0)
        if failed:
            failures.append(f"{chk.name} -> {value}")
        else:
            print(f"[ok] {chk.name}")

    if failures:
        raise RuntimeError("Warehouse contract checks failed:\n- " + "\n- ".join(failures))


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Missing warehouse DB: {DB_PATH}")

    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA foreign_keys = ON")

        _run_presence_checks(con)
        _run_non_empty_checks(con)

        dim_cols = _columns(con, "dim_artist")

        if "artist_norm" not in dim_cols:
            raise RuntimeError(
                "dim_artist is missing required column 'artist_norm' (used for stable identity)."
            )

        dim_pitchfork_col = _first_existing(dim_cols, DIM_PITCHFORK_NAME_CANDIDATES)
        if dim_pitchfork_col is None:
            raise RuntimeError(
                "dim_artist is missing a Pitchfork name column. "
                f"Expected one of: {', '.join(DIM_PITCHFORK_NAME_CANDIDATES)}"
            )

        dim_pk = _primary_key_column(con, "dim_artist")
        if dim_pk is None:
            raise RuntimeError(
                "dim_artist does not have a single-column primary key. "
                "Add one (recommended) or adjust validation to your schema."
            )

        checks: list[Check] = [
            # Key uniqueness
            Check(
                name="pitchfork_reviews.reviewid is unique",
                sql="""
                SELECT COUNT(*) FROM (
                  SELECT reviewid, COUNT(*) c
                  FROM pitchfork_reviews
                  GROUP BY reviewid
                  HAVING c > 1
                )
                """,
            ),
            Check(
                name=f"dim_artist.{dim_pk} is unique",
                sql=f"""
                SELECT COUNT(*) FROM (
                  SELECT {dim_pk}, COUNT(*) c
                  FROM dim_artist
                  GROUP BY {dim_pk}
                  HAVING c > 1
                )
                """,
            ),
            Check(
                name="dim_artist.artist_norm is unique",
                sql="""
                SELECT COUNT(*) FROM (
                  SELECT artist_norm, COUNT(*) c
                  FROM dim_artist
                  GROUP BY artist_norm
                  HAVING c > 1
                )
                """,
            ),
            # Required fields (non-null / non-blank)
            Check(
                name="pitchfork_reviews has no blank artist",
                sql="""
                SELECT COUNT(*)
                FROM pitchfork_reviews
                WHERE artist IS NULL OR TRIM(artist) = ''
                """,
            ),
            Check(
                name="pitchfork_reviews has no null score",
                sql="""
                SELECT COUNT(*)
                FROM pitchfork_reviews
                WHERE score IS NULL
                """,
            ),
            Check(
                name="pitchfork_reviews has no null pub_year",
                sql="""
                SELECT COUNT(*)
                FROM pitchfork_reviews
                WHERE pub_year IS NULL
                """,
            ),
            Check(
                name=f"dim_artist has no blank {dim_pitchfork_col}",
                sql=f"""
                SELECT COUNT(*)
                FROM dim_artist
                WHERE {dim_pitchfork_col} IS NULL OR TRIM({dim_pitchfork_col}) = ''
                """,
            ),
            Check(
                name="dim_artist has no blank artist_norm",
                sql="""
                SELECT COUNT(*)
                FROM dim_artist
                WHERE artist_norm IS NULL OR TRIM(artist_norm) = ''
                """,
            ),
            # Bridge integrity
            Check(
                name="pitchfork_review_artists has no orphan reviewids",
                sql="""
                SELECT COUNT(*)
                FROM pitchfork_review_artists pra
                LEFT JOIN pitchfork_reviews pr ON pr.reviewid = pra.reviewid
                WHERE pr.reviewid IS NULL
                """,
            ),
            Check(
                name="pitchfork_review_artists has no blank artist values",
                sql="""
                SELECT COUNT(*)
                FROM pitchfork_review_artists
                WHERE artist IS NULL OR TRIM(artist) = ''
                """,
            ),
            Check(
                name="pitchfork_review_artists has no duplicate (reviewid, artist) pairs",
                sql="""
                SELECT COUNT(*) FROM (
                  SELECT reviewid, TRIM(artist) AS artist_clean, COUNT(*) c
                  FROM pitchfork_review_artists
                  GROUP BY reviewid, artist_clean
                  HAVING c > 1
                )
                """,
            ),
            # Reasonable ranges
            Check(
                name="Pitchfork scores are within 0.0–10.0",
                sql="""
                SELECT COUNT(*)
                FROM pitchfork_reviews
                WHERE score < 0.0 OR score > 10.0
                """,
            ),
            Check(
                name="Spotify streams are non-negative (when present)",
                sql="""
                SELECT COUNT(*)
                FROM spotify_youtube_clean
                WHERE streams IS NOT NULL AND streams < 0
                """,
            ),
            Check(
                name="YouTube views are non-negative (when present)",
                sql="""
                SELECT COUNT(*)
                FROM spotify_youtube_clean
                WHERE yt_views IS NOT NULL AND yt_views < 0
                """,
            ),
            Check(
                name="YouTube likes are non-negative (when present)",
                sql="""
                SELECT COUNT(*)
                FROM spotify_youtube_clean
                WHERE yt_likes IS NOT NULL AND yt_likes < 0
                """,
            ),
            Check(
                name="YouTube comments are non-negative (when present)",
                sql="""
                SELECT COUNT(*)
                FROM spotify_youtube_clean
                WHERE yt_comments IS NOT NULL AND yt_comments < 0
                """,
            ),
        ]

        sy_cols = _columns(con, "spotify_youtube_clean")
        for col in ("danceability", "energy", "valence"):
            if col in sy_cols:
                checks.append(
                    Check(
                        name=f"{col} is within 0.0–1.0 when present",
                        sql=f"""
                        SELECT COUNT(*)
                        FROM spotify_youtube_clean
                        WHERE {col} IS NOT NULL AND ({col} < 0.0 OR {col} > 1.0)
                        """,
                    )
                )

        _run_sql_checks(con, checks)

        for v in REQUIRED_VIEWS:
            con.execute(f"SELECT 1 FROM {v} LIMIT 1").fetchone()
        print("[ok] views query smoke test passed")

    print("[ok] warehouse validation passed")


if __name__ == "__main__":
    main()
