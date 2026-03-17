# Data Contract — Vinyl Critics vs Streams

This project only analyzes and models data that meets a small set of clear checks. If data fails a check, the pipeline stops rather than producing misleading results.

**Scope:** SQLite warehouse at `data/processed/vinyl_dw.sqlite`  
**Checked by:** `scripts/validate_dw.py` and the tests in `tests/`

---

## What we guarantee

- **Required tables & views:** these must exist and contain rows:
  - Tables: `pitchfork_reviews`, `pitchfork_review_artists`, `dim_artist`, `spotify_youtube_clean`
  - Views: `vw_review_with_artist`, `vw_artist_summary`, `vw_artist_streams`, `vw_artist_critics_vs_streams`, `vw_unmatched_artists`
- **Unique identifiers:** primary keys must be unique to avoid double-counting:
  - `pitchfork_reviews.reviewid`
  - `dim_artist.artist_id`
  - No duplicate artist rows for the same `reviewid` in `pitchfork_review_artists`
- **Referential integrity:** references must point to real rows (no dangling links):
  - Every `pitchfork_review_artists.reviewid` must exist in `pitchfork_reviews.reviewid`
  - Artist keys used to join critics data to streams must be valid
- **Required fields (non-null):** essential columns must be present:
  - `pitchfork_reviews`: `reviewid`, `artist`, `score`, `pub_year`
  - `pitchfork_review_artists`: `reviewid`, `artist`
  - `dim_artist`: `artist_id`, `pitchfork_name`
  - `vw_artist_critics_vs_streams`: fields used by notebooks/models (score, streams, etc.)
  - Allowed to be null: `dim_artist.spotify_name` (not all artists match)
- **Reasonable value ranges:** numeric values must fall in sensible ranges:
  - Pitchfork `score`: 0.0 — 10.0
  - Streams, views, likes, comments: >= 0
  - Audio features (when present): typically 0.0 — 1.0 (danceability, energy, valence)

If any of the above checks fail, the dataset does not meet the contract and processing should stop.

---

## What we do not promise

- Perfect cross-platform artist matches — we prefer conservative matches over incorrect ones.
- Causal claims — correlation between critics and streams does not imply cause.
- Complete Spotify/YouTube coverage — some artists will remain unmatched.

---

## Verify locally

Run the pipeline and checks:

```bash
python scripts/run_pipeline.py
pytest tests/ -v
```
