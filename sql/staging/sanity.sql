-- 1) row counts
SELECT 'pitchfork_reviews' AS tbl, COUNT(*) FROM pitchfork_reviews
UNION ALL SELECT 'pitchfork_artists', COUNT(*) FROM pitchfork_artists
UNION ALL SELECT 'spotify_youtube_clean', COUNT(*) FROM spotify_youtube_clean;

-- 2) score ranges
SELECT MIN(score) AS min_score, MAX(score) AS max_score FROM pitchfork_reviews;

-- 3) basic null checks
SELECT COUNT(*) AS null_artists FROM pitchfork_artists WHERE artist IS NULL OR TRIM(artist)='';
SELECT COUNT(*) AS null_rows_spotify FROM spotify_youtube_clean WHERE artist IS NULL OR song IS NULL;
