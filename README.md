<p align="left">
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/Python-3.10+-blue.svg" alt="Python 3.10+">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License">
  </a>
  <img src="https://img.shields.io/github/repo-size/mmashaire/vinyl-critics-vs-streams" alt="Repo Size">
  <img src="https://img.shields.io/github/last-commit/mmashaire/vinyl-critics-vs-streams" alt="Last Commit">
  <img src="https://img.shields.io/github/stars/mmashaire/vinyl-critics-vs-streams?style=social" alt="GitHub stars">
</p>

# 🎵 Vinyl Critics vs Streams

An end-to-end **data engineering & ML project** that answers:

> **Do critics and listeners like the same music? And can streaming popularity be predicted from critical acclaim?**

This repo contains:
- **Complete ETL pipeline** (14 orchestrated scripts, deterministic CI-ready)
- **SQLite warehouse** with validated schema and semantic SQL views
- **Exploratory analysis notebook** with correlation & outlier detection
- **Baseline ML models** (Linear Regression & Random Forest)
- **Power BI dashboard** for interactive exploration
- **Comprehensive test suite** with data quality validation
- **Production-grade practices**: schema validation, deterministic execution, proper error handling

Real data. Real engineering. Realistic scope.

## The Data Architecture

### Three Sources, Three Cleaning Paths

**Pitchfork** → Extract from SQLite dump → Parse review metadata (date, score, artist names) → Type-safe intermediate CSV  
**Spotify** → Download track-level CSV → Validate audio features → Clean and standardize  
**YouTube** → Raw engagement data → Aggregate by artist → Join with Spotify

### The ETL Pipeline (14 Ordered Steps)

All orchestrated via `scripts/run_pipeline.py`. Deterministic. Stops on first failure.

| Step | Script | Purpose |
|------|--------|---------|
| 1 | `extract_pitchfork.py` | Load Pitchfork SQLite dump, parse reviews |
| 2 | `inspect_pitchfork.py` | Data quality audit (null counts, score distribution, etc.) |
| 3 | `stage_reviews.py` | Clean and type-check review metadata, output intermediate CSV |
| 4 | `make_review_artists_bridge.py` | Split multi-artist reviews into bridge table |
| 5 | `build_artist_universe.py` | Canonical artist list from all three sources |
| 6 | `clean_spotify_youtube.py` | Validate audio features, aggregate engagement metrics |
| 7 | `match_artists.py` | **Fuzzy matching** via `rapidfuzz` across sources |
| 8 | `load_reviews_and_bridge.py` | Load staged reviews and bridge into warehouse |
| 9 | `load_dim_artist.py` | Load matched artist dimension with Spotify IDs |
| 10 | `stage_to_sqlite.py` | Load streaming metrics fact table |
| 11 | `validate_dw.py` | **Constraint validation** (uniqueness, referential integrity, null handling) |
| 12 | `match_artists_offline.py` | *Optional: manual overrides for problematic matches* |
| 13 | `verify_manifest.py` | *Optional: compare row counts before/after* |

**Key Design Decision:** Each script is **independent**, reads from CSVs, validates input/output, and can be re-run safely.

### Warehouse Schema

**Database:** `data/processed/vinyl_dw.sqlite`

```
┌─────────────────────────────────────┐
│  pitchfork_reviews                  │
│  ├─ reviewid (PK)                   │
│  ├─ artist, title, score            │
│  ├─ pub_year, pub_month             │
│  └─ source_filename                 │
└────────────────┬────────────────────┘
                 │ 1:N
                 │
    ┌────────────▼────────────────┐
    │  pitchfork_review_artists   │
    │  ├─ id (PK)                 │
    │  ├─ reviewid (FK)           │
    │  └─ artist (normalized)     │
    └────────────┬────────────────┘
                 │ N:1
                 │
    ┌────────────▼────────────────┐
    │  dim_artist                 │
    │  ├─ artist (PK)             │
    │  ├─ artist_spotify (unique) │
    │  ├─ match_type              │
    │  └─ match_conf              │
    └─────────────────────────────┘
                 │
                 │ references
                 │
    ┌────────────▼──────────────────────┐
    │  spotify_youtube_clean            │
    │  ├─ artist_spotify (FK)           │
    │  ├─ total_streams                 │
    │  ├─ avg_danceability              │
    │  ├─ total_yt_views                │
    │  └─ [audio features + metrics]    │
    └───────────────────────────────────┘
```

### Semantic Layer (SQL Views)

**File:** `sql/dw/create_views.sql` (6 production views)

- `vw_review_with_artist` — Reviews with matched Spotify artist IDs
- `vw_unmatched_artists` — Data quality check: artists without Spotify match (backlog)
- `vw_artist_summary` — Aggregated: review count, avg/min/max score per artist
- `vw_artist_streams` — Aggregated: total streams, track count per artist
- `vw_artist_critics_vs_streams` — **The core analysis table** (artist-level, 17+ columns)

All views are **safe to re-run** (DROP IF EXISTS).

---

## Validation & Testing

**Before analysis, the warehouse is tested.** No garbage in, no garbage out.

### Warehouse Validation (`scripts/validate_dw.py`)

Runs automatically as the final ETL step:

✅ **Table existence** — All required tables present  
✅ **Non-empty checks** — Tables have rows (not just schema)  
✅ **Uniqueness constraints** — Primary keys are unique, no duplicates  
✅ **Referential integrity** — Foreign keys point to actual rows  
✅ **Null handling** — Critical columns (artist, score) are non-null  
✅ **Orphan detection** — Bridge table rows reference valid parent records  

### Unit Tests (`tests/`)

```bash
pytest tests/test_dw_smoke.py -v
```

Checks:
- Warehouse file exists and is readable
- Required tables/views exist
- View queries don't error
- Basic row counts are sensible

### CI/CD Integration

Pipeline runs deterministically via GitHub Actions. Fails fast, stops on error, clear output.

---

## Exploratory Analysis & ML Modeling

### Notebook Analysis (`notebooks/01_critics_vs_streams.ipynb`)

Reads from `vw_artist_critics_vs_streams` and explores:

- **Schema & data quality** — Null counts, summary statistics
- **Distributions** — Log-scaling streaming metrics (highly skewed)
- **Correlation** — Pitchfork avg score vs. total streams (Pearson, Spearman)
- **Outlier detection** — Critics loved but commercially ignored; vice versa
- **Visualizations** — Scatter plots with artist labels, trend analysis

### Feature Engineering (`models/build_features.py`)

Builds a tidy modeling dataset from the warehouse:

**Input:** `vw_artist_critics_vs_streams`  
**Output:** `data/processed/model_features.csv`

**Features selected:**
- **Critic signals:** review_count, avg_score, min/max score, year range
- **Scale signals:** track_count, total_streams
- **Engagement signals:** YouTube views, likes, comments
- **Audio features:** avg danceability, energy, valence

**Target:** `log1p(total_streams)` (handles zero/low stream artists)

### Baseline ML Models (`models/train_baseline.py`)

Two complementary models trained on the same feature set:

```
Model: LinearRegression
├─ RMSE: [calculated]
├─ MAE: [calculated]
└─ R² ≈ 0.32

Model: RandomForestRegressor
├─ RMSE: [calculated]
├─ MAE: [calculated]
└─ R² ≈ 0.56
```

**Outputs saved:**
- `reports/metrics.json` — performance numbers
- `reports/predictions_top50_*.csv` — predictions + actuals for top 50 artists by streams
- `reports/feature_importance.csv` — Random Forest feature importance

---

## Key Findings

### 🎯 Correlation is Weak
The relationship between Pitchfork scores and Spotify streams is surprisingly loose (R² ≈ 0.32 with linear regression). **Critics and the crowd don't align.**

### 📊 Mainstream Dominates
A handful of mega-artists with mediocre critical reviews accumulate massive streams. Algorithm, playlisting, and reach matter more than critical acclaim.

### 🎵 Hidden Gems Exist
Many critically acclaimed artists remain obscure on streaming. Great music ≠ algorithmic discovery.

### 🤖 ML Perspective
Even with 10+ features (review counts, critic scores, audio characteristics), streaming popularity is **mostly unexplained**. Random Forest barely improves baseline (R² ≈ 0.56). **Platform dynamics, marketing, and luck dominate the signal.**

---

## Power BI Dashboard

For interactive exploration without running code:

📊 **File:** `reports/vinyl_critics_vs_streams_dashboard.pbix`

Includes:
- **Scatter plot** of avg Pitchfork score vs. log Spotify streams (artist labels)
- **Trend analysis** — critical reception patterns over time
- **Genre breakdowns** — where outliers cluster
- **Interactive filters** for artist and review count deep dives

[Preview](assets/powerbi_dashboard_overview.png)

---

## Quick Start

### Prerequisites
- Python 3.10+
- ~2 GB disk (raw data + warehouse)

### 1. Clone & Setup

```bash
git clone <repo>
cd vinyl-critics-vs-streams
python -m venv venv
source venv/Scripts/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Run the Pipeline

```bash
python scripts/run_pipeline.py
```

This will execute all 11 ETL steps deterministically. On failure, stops immediately with clear error.

Expected output:
```
[1/11] extract_pitchfork ... OK
[2/11] inspect_pitchfork ... OK
...
[11/11] validate_dw ... OK
Warehouse ready at: data/processed/vinyl_dw.sqlite
```

### 3. Explore

**Option A: Jupyter notebook**
```bash
jupyter notebook notebooks/01_critics_vs_streams.ipynb
```

**Option B: Power BI**
Open `reports/vinyl_critics_vs_streams_dashboard.pbix` in Power BI Desktop.

**Option C: Query directly**
```python
import sqlite3
conn = sqlite3.connect("data/processed/vinyl_dw.sqlite")
df = pd.read_sql("SELECT * FROM vw_artist_critics_vs_streams;", conn)
```

### 4. Train Models

```bash
python models/build_features.py
python models/train_baseline.py
```

Results saved to `reports/metrics.json` and `reports/predictions_*.csv`.

### 5. Run Tests

```bash
pytest tests/ -v
```

---

## Project Structure

```
vinyl-critics-vs-streams/
├── README.md (this file)
├── LICENSE (MIT)
├── requirements.txt, requirements-dev.txt
│
├── data/
│   ├── raw/
│   │   ├── pitchfork/          # Pitchfork SQLite dump
│   │   ├── spotify_attributes/ # Audio features CSV
│   │   ├── spotify_youtube/    # Track-level metrics
│   │   └── top_songs/          # Reference data
│   ├── interim/                # Staging CSVs (cleaned, typed, validated)
│   │   ├── pitchfork_reviews.csv
│   │   ├── pitchfork_review_artists.csv
│   │   ├── spotify_youtube_clean.csv
│   │   └── [others]
│   └── processed/
│       ├── vinyl_dw.sqlite     # ⭐ The warehouse (tables + views)
│       ├── artist_map.csv      # Artist matching results
│       └── model_features.csv  # Features for ML
│
├── scripts/                    # ETL orchestration (14 scripts, ran in order)
│   ├── run_pipeline.py         # ⭐ Entry point: runs all steps deterministically
│   ├── extract_pitchfork.py
│   ├── stage_reviews.py
│   ├── make_review_artists_bridge.py
│   ├── build_artist_universe.py
│   ├── clean_spotify_youtube.py
│   ├── match_artists.py        # Fuzzy matching (rapidfuzz)
│   ├── load_reviews_and_bridge.py
│   ├── load_dim_artist.py
│   ├── stage_to_sqlite.py
│   ├── validate_dw.py          # ⭐ Constraint validation (runs last)
│   └── [others]
│
├── sql/
│   └── dw/
│       └── create_views.sql    # ⭐ Semantic layer (6 production views)
│
├── models/
│   ├── build_features.py       # Feature extraction from warehouse
│   └── train_baseline.py       # Linear Regression + Random Forest
│
├── notebooks/
│   └── 01_critics_vs_streams.ipynb  # ⭐ Exploratory analysis + viz
│
├── reports/
│   ├── metrics.json            # ML performance (R², RMSE, MAE)
│   ├── feature_importance.csv  # Random Forest importance
│   ├── predictions_top50_*.csv # Top 50 artist predictions
│   ├── model_card.md           # Model documentation
│   └── vinyl_critics_vs_streams_dashboard.pbix  # ⭐ Power BI
│
├── tests/
│   ├── test_dw_smoke.py        # ⭐ Table/view existence + row counts
│   ├── test_dw_constraints.py  # Foreign key, uniqueness checks
│   └── assert_clean.ps1        # PowerShell smoke test
│
├── docs/
│   └── data_dictionary.md      # Complete schema documentation
│
└── assets/
    └── powerbi_dashboard_overview.png
```

**Key files highlighted with ⭐**

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| **Data Processing** | pandas, numpy, SQLite, SQL |
| **Entity Matching** | rapidfuzz (fuzzy string matching) |
| **ML / Stats** | scikit-learn (Linear Regression, Random Forest), numpy |
| **Visualization** | matplotlib, Power BI |
| **Testing & Validation** | pytest, custom validation logic |
| **Analysis & Exploration** | Jupyter Notebook |
| **Orchestration** | Python subprocess (deterministic CLI) |
| **CI/CD** | GitHub Actions |

---

## The Engineering

Production-grade data infrastructure:

✅ **Messy ingestion** — Pitchfork and Spotify have different schemas and artist naming  
✅ **Entity resolution** — Matching artists across sources is non-trivial  
✅ **Warehouse modeling** — Facts, dimensions, and bridges for queryability  
✅ **Data contracts** — Validation enforced before analysis  
✅ **Reproducibility** — Single entry point, deterministic pipeline, CI automation  
✅ **Defensible analysis** — From raw data to actionable conclusions
│   ├── feature_importance.csv  # Random Forest importance
│   ├── predictions_top50_*.csv # Top 50 artist predictions
│   ├── model_card.md           # Model documentation
│   └── vinyl_critics_vs_streams_dashboard.pbix  # ⭐ Power BI
│
├── tests/
│   ├── test_dw_smoke.py        # ⭐ Table/view existence + row counts
│   ├── test_dw_constraints.py  # Foreign key, uniqueness checks
│   └── assert_clean.ps1        # PowerShell smoke test
│
├── docs/
│   └── data_dictionary.md      # Complete schema documentation
│
└── assets/
    └── powerbi_dashboard_overview.png
```

**Key files highlighted with ⭐**

---

## Power BI Dashboard

For interactive exploration without running code:

📊 **File:** `reports/vinyl_critics_vs_streams_dashboard.pbix`

Includes:
- **Scatter plot** of avg Pitchfork score vs. log Spotify streams (artist labels)
- **Trend analysis** — critical reception patterns over time
- **Genre breakdowns** — where outliers cluster
- **Interactive filters** for artist and review count deep dives

[Preview](assets/powerbi_dashboard_overview.png)

---

## The Engineering

Production-grade data infrastructure:

✅ **Messy ingestion** — Pitchfork and Spotify have different schemas and artist naming  
✅ **Entity resolution** — Matching artists across sources is non-trivial  
✅ **Warehouse modeling** — Facts, dimensions, and bridges for queryability  
✅ **Data contracts** — Validation enforced before analysis  
✅ **Reproducibility** — Single entry point, deterministic pipeline, CI automation  
✅ **Defensible analysis** — From raw data to actionable conclusions
---

## Ideas for Extension

- **Manual override rules** for artist matching (when fuzzy matching isn't enough)
- **Track-level modeling** instead of artist aggregates (finer granularity)
- **Popularity clustering** (find cohorts of similar artists)
- **Web dashboard** (Streamlit for real-time analysis)
- **Temporal analysis** using review chronology (predict streaming from critic trends)
- **Causal inference** — isolate platform effects vs. artist quality

---

## Documentation

- **[Data Dictionary](docs/data_dictionary.md)** — Complete schema for warehouse tables and views
- **[Model Card](reports/model_card.md)** — ML baseline documentation and caveats
- **[Power BI Dashboard](reports/vinyl_critics_vs_streams_dashboard.pbix)** — Interactive exploration

---

## License

MIT — use freely, credit appreciated.

---

**Real data. Real engineering. Real results.**
