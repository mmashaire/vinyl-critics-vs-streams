param(
  [string]$DB = "data/processed/vinyl_dw.sqlite"
)

# Whitelist ONLY the legit <=2-char artist names you saw:
$whitelist = @('m','u2','om')  # <-- add any others here

# Build a properly quoted list for SQL: 'm','u2','om'
$inList = if ($whitelist.Count -gt 0) { ($whitelist | ForEach-Object { "'" + $_.ToLower() + "'" }) -join ',' } else { '' }

# 's' must be gone
$badS = sqlite3 $DB "SELECT COUNT(*) FROM pitchfork_review_artists WHERE artist='s';"
if ([int]$badS -ne 0) { throw "'s' tokens present: $badS" }

# Short tokens excluding whitelist
$shortSql = if ($whitelist.Count -gt 0) {
@"
SELECT COUNT(*) FROM pitchfork_review_artists
WHERE length(artist) <= 2 AND lower(artist) NOT IN ($inList);
"@
} else {
@"
SELECT COUNT(*) FROM pitchfork_review_artists
WHERE length(artist) <= 2;
"@
}
$shortCount = sqlite3 $DB $shortSql

if ([int]$shortCount -ne 0) {
  $shortListSql = if ($whitelist.Count -gt 0) {
@"
SELECT DISTINCT artist FROM pitchfork_review_artists
WHERE length(artist) <= 2 AND lower(artist) NOT IN ($inList)
ORDER BY artist;
"@
  } else {
@"
SELECT DISTINCT artist FROM pitchfork_review_artists
WHERE length(artist) <= 2
ORDER BY artist;
"@
  }
  $shortList = sqlite3 $DB $shortListSql
  throw "Short tokens present (non-whitelisted): $shortCount`n$shortList"
}

# Orphan safety
$orphans = sqlite3 $DB "SELECT COUNT(*) FROM pitchfork_review_artists pra LEFT JOIN pitchfork_reviews pr USING(reviewid) WHERE pr.reviewid IS NULL;"
if ([int]$orphans -ne 0) { throw "Orphan bridge rows: $orphans" }

Write-Host "[ok] SQL checks passed"
