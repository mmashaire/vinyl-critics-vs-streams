-- Analysis views for Pitchfork reviews, artist mapping and streaming data.
-- These views sit on top of the core tables and are safe to run repeatedly.

---------------------------------------------------------------------------
-- Review to artist view, including mapping metadata
---------------------------------------------------------------------------

DROP VIEW IF EXISTS vw_review_with_artist;

CREATE VIEW vw_review_with_artist AS
SELECT
  pr.reviewid,
  pr.title,
  pr.pub_year,
  pr.pub_month,
  pr.score,
  pra.artist        AS bridge_artist,
  da.artist_spotify,
  da.match_type,
  da.score          AS match_conf
FROM pitchfork_review_artists AS pra
JOIN pitchfork_reviews AS pr USING (reviewid)
LEFT JOIN dim_artist AS da
  ON da.artist = pra.artist;

---------------------------------------------------------------------------
-- Unmatched artists, backlog for manual mapping or better matching logic
---------------------------------------------------------------------------

DROP VIEW IF EXISTS vw_unmatched_artists;

CREATE VIEW vw_unmatched_artists AS
SELECT
  bridge_artist AS artist,
  COUNT(*)      AS n_reviews
FROM vw_review_with_artist
WHERE artist_spotify IS NULL
GROUP BY artist
ORDER BY n_reviews DESC, artist ASC;

---------------------------------------------------------------------------
-- Coverage by year, shows how mapping behaves across publication years
---------------------------------------------------------------------------

DROP VIEW IF EXISTS vw_artist_coverage_by_year;

CREATE VIEW vw_artist_coverage_by_year AS
WITH a AS (
  SELECT DISTINCT
    pr.pub_year,
    pra.artist
  FROM pitchfork_review_artists AS pra
  JOIN pitchfork_reviews AS pr USING (reviewid)
),
m AS (
  SELECT DISTINCT
    pr.pub_year,
    pra.artist
  FROM pitchfork_review_artists AS pra
  JOIN pitchfork_reviews AS pr USING (reviewid)
  JOIN dim_artist AS da
    ON da.artist = pra.artist
  WHERE da.artist_spotify IS NOT NULL
)
SELECT
  a.pub_year,
  COUNT(DISTINCT a.artist) AS artists_total,
  COUNT(DISTINCT m.artist) AS artists_mapped,
  ROUND(
    1.0 * COUNT(DISTINCT m.artist)
      / NULLIF(COUNT(DISTINCT a.artist), 0),
    3
  ) AS pct_mapped
FROM a
LEFT JOIN m
  ON m.pub_year = a.pub_year
 AND m.artist   = a.artist
GROUP BY a.pub_year
ORDER BY a.pub_year;

---------------------------------------------------------------------------
-- Artist level critic summary, keyed by Spotify artist name
-- Only includes artists that have a Spotify mapping
---------------------------------------------------------------------------

DROP VIEW IF EXISTS vw_artist_summary;

CREATE VIEW vw_artist_summary AS
SELECT
  artist_spotify                AS artist,
  COUNT(DISTINCT reviewid)      AS review_count,
  AVG(score)                    AS avg_score,
  MIN(score)                    AS min_score,
  MAX(score)                    AS max_score,
  MIN(pub_year)                 AS first_review_year,
  MAX(pub_year)                 AS last_review_year
FROM vw_review_with_artist
WHERE artist_spotify IS NOT NULL
GROUP BY artist_spotify;

---------------------------------------------------------------------------
-- Artist level streaming summary from spotify_youtube_clean
---------------------------------------------------------------------------

DROP VIEW IF EXISTS vw_artist_streams;

CREATE VIEW vw_artist_streams AS
SELECT
  artist,
  COUNT(*)              AS track_count,
  SUM(streams)          AS total_streams,
  AVG(streams)          AS avg_streams_per_track,
  SUM(yt_views)         AS total_yt_views,
  AVG(yt_views)         AS avg_yt_views_per_track,
  SUM(yt_likes)         AS total_yt_likes,
  SUM(yt_comments)      AS total_yt_comments,
  AVG(danceability)     AS avg_danceability,
  AVG(energy)           AS avg_energy,
  AVG(valence)          AS avg_valence
FROM spotify_youtube_clean
GROUP BY artist;

---------------------------------------------------------------------------
-- Critics vs streams mart, one row per Spotify artist
-- This is the main entry point for analysis and dashboards
---------------------------------------------------------------------------

DROP VIEW IF EXISTS vw_artist_critics_vs_streams;

CREATE VIEW vw_artist_critics_vs_streams AS
SELECT
  c.artist,
  c.review_count,
  c.avg_score,
  c.min_score,
  c.max_score,
  c.first_review_year,
  c.last_review_year,
  s.track_count,
  s.total_streams,
  s.avg_streams_per_track,
  s.total_yt_views,
  s.avg_yt_views_per_track,
  s.total_yt_likes,
  s.total_yt_comments,
  s.avg_danceability,
  s.avg_energy,
  s.avg_valence
FROM vw_artist_summary AS c
LEFT JOIN vw_artist_streams AS s
  ON c.artist = s.artist;
