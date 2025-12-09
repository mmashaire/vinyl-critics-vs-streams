# Data Dictionary – Vinyl Critics vs Streams

This document describes the key tables and SQL views inside the SQLite warehouse  
`data/processed/vinyl_dw.sqlite`.  
It explains the structure, fields, and purpose of each object so future users or recruiters can
understand how the analytical layer is organised.

---

# 1. Warehouse Tables

These are the core objects created or loaded during the ETL process.

---

## **pitchfork_reviews**

One row per Pitchfork album review.

| Column          | Type    | Description |
|-----------------|---------|-------------|
| reviewid        | INTEGER | Unique review identifier |
| artist          | TEXT    | Raw artist string from Pitchfork (may contain multiple artists) |
| title           | TEXT    | Album title |
| score           | REAL    | Pitchfork review score (0.0–10.0) |
| pub_date        | TEXT    | Original publication date |
| pub_year        | INTEGER | Year extracted from `pub_date` |

**Notes:**  
- `artist` may include multiple artists separated by commas or slashes (e.g., `"Kleenex / Liliput"`).  
- `pub_year` is derived during staging.

---

## **pitchfork_review_artists**

Bridge table resolving multi-artist reviews into one artist per row.

| Column   | Type    | Description |
|----------|---------|-------------|
| reviewid | INTEGER | Review ID |
| artist   | TEXT    | Normalised individual artist extracted from the raw string |

**Notes:**  
- Ensures consistency for aggregation and analysis.  
- Deduplicated so that an artist appears once per review.

---

## **dim_artist**

Unified artist dimension created after fuzzy matching between Pitchfork and Spotify names.

| Column         | Type    | Description |
|----------------|---------|-------------|
| artist_id      | INTEGER | Surrogate key |
| pitchfork_name | TEXT    | Name as seen in Pitchfork data |
| spotify_name   | TEXT    | Closest Spotify match (nullable) |
| match_type     | TEXT    | `"exact"`, `"fuzzy"`, or `"manual_override"` |
| match_score    | REAL    | Confidence score from fuzzy matcher (0–100) |

**Notes:**  
- Rows where `spotify_name` is NULL represent unresolved artists.
- Manual overrides (if added) appear clearly in this table.

---

## **spotify_youtube_clean**

Cleaned streaming dataset combining Spotify features and YouTube metrics.

| Column       | Type  | Description |
|--------------|-------|-------------|
| artist       | TEXT  | Cleaned, standardised artist name |
| song         | TEXT  | Track title |
| danceability | REAL  | Spotify audio feature |
| energy       | REAL  | Spotify audio feature |
| loudness     | REAL  | Track loudness |
| valence      | REAL  | Spotify audio feature |
| streams      | REAL  | Spotify total streams |
| yt_views     | REAL  | YouTube total views |
| yt_likes     | REAL  | YouTube likes |
| yt_comments  | REAL  | YouTube comments |

**Notes:**  
- Aggregations happen later in SQL views.  
- Only cleaned & validated tracks are kept.

---

# 2. SQL Views (Semantic Layer)

These views provide a stable interface to the notebook and any future dashboards.

---

## **vw_review_with_artist**

Expands each review into rows per artist and attaches match information.

**Purpose:**
- Normalises multi-artist reviews
- Connects to the artist dimension if available

| Column         | Notes |
|----------------|-------|
| reviewid       | From Pitchfork |
| title          | Album title |
| pub_year       | Year |
| score          | Review score |
| bridge_artist  | Artist from the bridge table |
| artist_spotify | Standardised/Matched Spotify artist |
| match_type     | `"exact"`, `"fuzzy"`, `"manual_override"` |
| match_conf     | Fuzzy matching confidence |

---

## **vw_unmatched_artists**

A backlog of artists that failed Spotify matching.

| Column | Description |
|--------|-------------|
| artist | Bridge artist not matched |
| n_reviews | Number of Pitchfork reviews involving this artist |

**Use cases:**  
- Good starting point for improving the mapping system  
- Recruiters see you're thoughtfully handling imperfect data

---

## **vw_artist_summary**

Aggregates critic information per artist.

| Column | Meaning |
|--------|---------|
| review_count | Total Pitchfork reviews |
| avg_score | Average review score |
| min_score | Lowest score |
| max_score | Highest score |
| first_review_year | First review |
| last_review_year | Most recent review |

---

## **vw_artist_streams**

Aggregates streaming data per artist.

| Column | Meaning |
|--------|---------|
| track_count | Number of valid tracks |
| total_streams | Sum of Spotify streams |
| avg_streams_per_track | total_streams / track_count |
| total_yt_views | Sum of YouTube views |
| avg_danceability | Mean of Spotify audio feature |
| avg_energy | Mean energy |
| avg_valence | Mean valence |

---

## **vw_artist_critics_vs_streams**

Final combined dataset.

Includes:
- All critic metrics  
- All streaming aggregations  
- log-scaled metrics used for visualisation  

This is the view powering the notebook's scatter plot.

---

# 3. Example Queries

Useful for dashboards or sanity checks.

```sql
SELECT *
FROM vw_unmatched_artists
ORDER BY n_reviews DESC
LIMIT 20;

SELECT artist, avg_score, total_streams
FROM vw_artist_critics_vs_streams
WHERE review_count >= 2
ORDER BY total_streams DESC;

SELECT *
FROM vw_artist_critics_vs_streams
WHERE avg_score >= 9.0
ORDER BY total_streams;
