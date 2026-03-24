from __future__ import annotations

import pandas as pd
import pytest

from scripts.clean_spotify_youtube import clean_df


def _base_row(**overrides) -> dict:
    row = {
        "Artist": "Radiohead",
        "Track": "Karma Police",
        "Danceability": 0.35,
        "Energy": 0.45,
        "Loudness": -8.0,
        "Valence": 0.22,
        "Views": 50_000_000,
        "Likes": 800_000,
        "Comments": 12_000,
        "Streams": 120_000_000,
    }
    row.update(overrides)
    return row


def _df(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def test_output_columns_are_renamed() -> None:
    result = clean_df(_df(_base_row()))
    assert set(result.columns) >= {"artist", "song", "streams", "yt_views", "yt_likes", "yt_comments"}


def test_stream_column_alias_is_accepted() -> None:
    """Dataset variant that uses 'Stream' instead of 'Streams' should still produce a 'streams' column."""
    row = _base_row()
    row.pop("Streams")
    row["Stream"] = 99_000_000
    result = clean_df(_df(row))
    assert "streams" in result.columns
    assert result["streams"].iloc[0] == 99_000_000


def test_drops_row_with_null_artist() -> None:
    rows = [_base_row(), _base_row(Artist=None)]
    result = clean_df(_df(*rows))
    assert len(result) == 1
    assert result["artist"].iloc[0] == "Radiohead"


def test_drops_row_with_null_track() -> None:
    rows = [_base_row(), _base_row(Track=None)]
    result = clean_df(_df(*rows))
    assert len(result) == 1


def test_drops_duplicate_rows() -> None:
    rows = [_base_row(), _base_row()]  # exact duplicate
    result = clean_df(_df(*rows))
    assert len(result) == 1


def test_column_names_with_spaces_and_mixed_case() -> None:
    """Raw headers that have spaces or odd casing are normalized before matching."""
    row = {
        " Artist ": "Portishead",
        "TRACK": "Glory Box",
        "Danceability": 0.4,
        "Energy": 0.5,
        "Loudness": -9.0,
        "Valence": 0.3,
        "Views": 10_000_000,
        "Likes": 200_000,
        "Comments": 5_000,
        "Streams": 30_000_000,
    }
    result = clean_df(_df(row))
    assert result["artist"].iloc[0] == "Portishead"
    assert result["song"].iloc[0] == "Glory Box"


def test_optional_columns_absent_do_not_raise() -> None:
    """Audio features and engagement columns are optional; their absence should not crash."""
    row = {"Artist": "Björk", "Track": "Hyperballad", "Streams": 5_000_000}
    result = clean_df(_df(row))
    assert result["artist"].iloc[0] == "Björk"
    assert "yt_views" not in result.columns


def test_returns_copy_not_mutating_input() -> None:
    original = _df(_base_row())
    original_cols = list(original.columns)
    clean_df(original)
    assert list(original.columns) == original_cols
