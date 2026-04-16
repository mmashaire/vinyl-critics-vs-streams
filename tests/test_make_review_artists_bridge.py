from __future__ import annotations

from scripts.make_review_artists_bridge import clean_token, split_artists


def test_clean_token_normalizes_unicode_and_whitespace() -> None:
    raw = "  J\u00E9r\u00E9my  "
    assert clean_token(raw) == "Jérémy"


def test_split_artists_splits_common_separators() -> None:
    artist_text = "Radiohead & Bj\u00F6rk, Portishead / Massive Attack"
    assert split_artists(artist_text) == ["Radiohead", "Björk", "Portishead", "Massive Attack"]


def test_split_artists_filters_small_junk_tokens() -> None:
    artist_text = "Björk, m, x, U2, a"
    assert split_artists(artist_text) == ["Björk", "m", "x", "U2"]


def test_split_artists_empty_or_blank_returns_empty_list() -> None:
    assert split_artists("") == []
    assert split_artists("   \t  ") == []
