#!/usr/bin/env python3
"""
Clean and standardize Spotify-YouTube data for downstream use.

Loads raw CSV, normalizes column names, selects relevant columns,
drops duplicates and missing essentials, and saves cleaned data.
"""

import logging
from pathlib import Path

import pandas as pd


def repo_root() -> Path:
    """Get the repository root directory."""
    return Path(__file__).resolve().parent.parent


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply column normalization, selection, renaming, and deduplication.

    Accepts a raw Spotify-YouTube DataFrame and returns a cleaned copy.
    Handles both 'stream' and 'streams' column variants.
    """
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    required_columns = {"artist", "track"}
    missing_required = sorted(required_columns - set(df.columns))
    if missing_required:
        raise ValueError(
            "Missing required Spotify/YouTube columns: " + ", ".join(missing_required)
        )

    colmap: dict[str, str] = {
        "artist": "artist",
        "track": "song",
        "danceability": "danceability",
        "energy": "energy",
        "loudness": "loudness",
        "valence": "valence",
        "views": "yt_views",
        "likes": "yt_likes",
        "comments": "yt_comments",
    }
    # streams column name differs by dataset versions → handle both
    if "stream" in df.columns:
        colmap["stream"] = "streams"
    elif "streams" in df.columns:
        colmap["streams"] = "streams"

    keep = [k for k in colmap if k in df.columns]
    clean = df[keep].rename(columns=colmap)
    clean = clean.dropna(subset=["artist", "song"]).copy()

    # Trim essential text fields and reject whitespace-only values.
    clean["artist"] = clean["artist"].astype(str).str.strip()
    clean["song"] = clean["song"].astype(str).str.strip()
    clean = clean[(clean["artist"] != "") & (clean["song"] != "")]

    return clean.drop_duplicates()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    SRC = repo_root() / "data" / "raw" / "spotify_youtube" / "Spotify_Youtube.csv"
    OUT = repo_root() / "data" / "interim" / "spotify_youtube_clean.csv"

    df = pd.read_csv(SRC, low_memory=False)
    clean = clean_df(df)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    clean.to_csv(OUT, index=False)
    logging.info(f"Saved {len(clean):,} rows -> {OUT}")


if __name__ == "__main__":
    main()
