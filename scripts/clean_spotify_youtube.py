import pandas as pd
from pathlib import Path

SRC = Path(r"D:\Projects\vinyl-critics-vs-streams\data\raw\spotify_youtube\Spotify_Youtube.csv")
OUT = Path(r"D:\Projects\vinyl-critics-vs-streams\data\interim\spotify_youtube_clean.csv")

df = pd.read_csv(SRC, low_memory=False)

df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

colmap = {
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

keep = [k for k in colmap.keys() if k in df.columns]
clean = df[keep].rename(columns=colmap)


clean = clean.dropna(subset=["artist","song"]).drop_duplicates()

clean.to_csv(OUT, index=False)
print(f"Saved {len(clean):,} rows -> {OUT}")
