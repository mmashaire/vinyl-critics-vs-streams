#!/usr/bin/env python3
"""
Build a modeling dataset from the SQLite warehouse.

Reads from vw_artist_critics_vs_streams and writes a tidy features CSV
for downstream modeling scripts.

Design goals:
- reproducible
- explicit feature selection
- minimal magic
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd


DEFAULT_DB = Path("data/processed/vinyl_dw.sqlite")
DEFAULT_OUT = Path("data/processed/model_features.csv")


FEATURE_COLS = [
    # critics
    "review_count",
    "avg_score",
    "min_score",
    "max_score",
    "first_review_year",
    "last_review_year",
    # scale / activity
    "track_count",
    # youtube engagement
    "total_yt_views",
    "avg_yt_views_per_track",
    "total_yt_likes",
    "total_yt_comments",
    # audio features
    "avg_danceability",
    "avg_energy",
    "avg_valence",
]

TARGET_COL = "total_streams"


def build_df(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB: {db_path}")

    con = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query("SELECT * FROM vw_artist_critics_vs_streams", con)
    finally:
        con.close()

    # Keep only what we need.
    keep = ["artist", TARGET_COL, *FEATURE_COLS]
    df = df[keep].copy()

    # Basic cleaning: drop rows missing target or core required fields.
    df = df.dropna(subset=[TARGET_COL, "artist"])

    # Ensure numeric columns are numeric (SQLite views can yield mixed types).
    for c in [TARGET_COL, *FEATURE_COLS]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=[TARGET_COL])  # target must be present
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to vinyl_dw.sqlite")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output CSV path")
    ap.add_argument("--min-reviews", type=int, default=2, help="Minimum review_count filter")
    ap.add_argument("--min-tracks", type=int, default=5, help="Minimum track_count filter")
    args = ap.parse_args()

    df = build_df(args.db)

    # Apply simple, explainable filters.
    df = df[df["review_count"] >= args.min_reviews]
    df = df[df["track_count"] >= args.min_tracks]

    # Sort for stability (useful for diffs/debugging).
    df = df.sort_values(["artist"]).reset_index(drop=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)

    print(f"[ok] wrote {len(df):,} rows -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
