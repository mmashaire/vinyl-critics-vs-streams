# Model Card — Baseline Streaming Prediction

## Objective
Predict streaming scale from critic reception + lightweight artist metadata.

**Target:** `log1p(total_streams)`  
**Data source:** `vw_artist_critics_vs_streams` (SQLite warehouse)

## Intended Use
- Understand whether critic signals and metadata explain a meaningful share of streaming variance.
- Provide a baseline for future modeling (e.g., adding genre signals, playlist exposure proxies).

Not intended for:
- Causal claims (“reviews cause streams”)
- Production forecasting

## Data
Row unit: **artist**  
Filters used in feature build:
- `review_count >= 2`
- `track_count >= 5`

## Features
Examples:
- critic stats: `avg_score`, `min_score`, `max_score`, `review_count`
- time: `first_review_year`, `last_review_year`
- scale: `track_count`
- YouTube engagement: `total_yt_views`, `total_yt_likes`, `total_yt_comments`
- audio features: `avg_energy`, `avg_danceability`, `avg_valence`

## Models
- Linear Regression (interpretable baseline)
- Random Forest Regressor (non-linear baseline)

## Evaluation
Train/test split with fixed random seed.

Metrics reported:
- RMSE
- MAE
- R²

See `reports/metrics.json`.

## Limitations
- Streaming metrics reflect platform dynamics (playlisting, algorithmic exposure), not only taste/quality.
- Possible survivorship bias: artists in the dataset may not represent the full music ecosystem.
- Feature set is incomplete (no marketing spend, tour activity, social media, playlist adds).

## Ethical Considerations
- **Bias in Critic Data**: Pitchfork reviews may reflect cultural biases in music criticism, potentially underrepresenting certain genres or artists.
- **Data Privacy**: Ensure all data sources comply with privacy regulations; no personal user data is used here.
- **Fair Representation**: Models should not reinforce stereotypes about "critic-approved" vs. "popular" music; this is exploratory only.

## Leakage / Integrity Notes
- Features are derived from the same integrated dataset; no future information beyond the aggregated view is used.
- The model predicts scale, not future growth; this is cross-sectional.

## Next Improvements
- Add time-windowed targets (streams within N years after first review)
- Add genre/label features (one-hot or embeddings)
- Try quantile regression / gradient boosting
- Add calibration plots / residual diagnostics
