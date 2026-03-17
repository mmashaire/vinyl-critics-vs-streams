<p align="left">
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/Python-3.10+-blue.svg" alt="Python 3.10+">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License">
  </a>
</p>

# Vinyl — Critics vs Streams

A compact, reproducible data engineering + ML project that explores whether critical acclaim (Pitchfork) lines up with listener popularity (Spotify/YouTube).

This repo is intended as a portfolio piece: clean ETL, a validated warehouse, clear tests, and a small modeling experiment. The code is readable and safe to run locally.

Quick highlights
- Warehouse: a small, self-contained SQLite warehouse at `data/processed/vinyl_dw.sqlite`.
- Reproducible pipeline: `scripts/run_pipeline.py` builds the warehouse end-to-end.
- Maintenance: `scripts/maintenance/repair_warehouse.py` is a safe, idempotent tool for common fixes.
- Tests: `pytest tests/` enforces data contracts and smoke checks.

Tech Stack
- **Language**: Python 3.10+
- **Data Processing**: Pandas, SQLite
- **Modeling**: Scikit-learn
- **Testing**: Pytest
- **Linting**: Flake8, Black, Isort

Why this project
- Shows multi-source ingestion and entity resolution.
- Demonstrates production-minded practices: backups, changelogs, and validation.
- Small, focused modeling to illustrate limitations of simple predictors.

Getting started

Prerequisites
- Python 3.10+
- ~2 GB disk

Setup
```bash
git clone <repo-url>
cd vinyl-critics-vs-streams
python -m venv venv
venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

Build the warehouse (recommended)
```bash
python scripts/run_pipeline.py
```
This runs the full ETL: extract → stage → match → load → validate. The pipeline is deterministic and stops on failure.

Quick checks
```bash
python scripts/maintenance/repair_warehouse.py    # dry-run by default
python scripts/maintenance/repair_warehouse.py --apply  # apply changes
python scripts/maintenance/debug_blanks.py       # inspect blank-artist rows
pytest tests/ -q
```

What to look at
- `notebooks/01_critics_vs_streams.ipynb` — exploration and visuals.
- `models/build_features.py` + `models/train_baseline.py` — feature pipeline and baseline models.
- `sql/dw/create_views.sql` — semantic views that power analysis.

Project layout (short)

```
data/                # raw, interim, processed (warehouse)
scripts/             # ETL orchestration + maintenance
scripts/maintenance/ # safe, documented maintenance helpers
models/              # feature building and training
notebooks/           # analysis and visualization
reports/             # metrics, feature importance, PBIX dashboard
tests/               # automated checks for the warehouse
```

Design notes (for reviewers)
- Safety first: maintenance scripts back up the DB and write a `maintenance_changelog` before mutating data.
- Idempotency: `repair_warehouse.py` runs in dry-run mode by default. Operations are recorded so repeated runs are safe.
- Readability: functions are small and have short docstrings. Aim was clarity over cleverness.

If you clone this repo
- Run the pipeline, inspect `data/processed/vinyl_dw.sqlite`, and open the notebook.
- Tests are lightweight — they should pass quickly on an up-to-date warehouse.

License

MIT — see `LICENSE`.

Contributing

If you want to contribute: open an issue describing the change, or a small PR. Keep logic clear and add tests for any behavior changes.

Contact

Owner: mmashaire (GitHub)

— end —

# 🎵 Vinyl Critics vs Streams

**An end-to-end data engineering & ML project** that investigates whether critics and listeners like the same music.

## The Question

Do Pitchfork critics and Spotify listeners agree? Can we predict streaming success from critical acclaim? **Spoiler: not really.**

This repo demonstrates production-grade data work:
- Extract & clean data from 3 heterogeneous sources
- Resolve artist identity across Pitchfork, Spotify, and YouTube
- Build a validated SQLite warehouse
- Explore correlations and outliers  
- Train baseline ML models
- Deliver interactive dashboards

---

## What's Inside

### 📊 Data Sources

| Source | Size | Format | Key Fields |
|--------|------|--------|-----------|
| **Pitchfork** | ~18K reviews | SQLite dump | review_id, artist, score, year, month |
| **Spotify** | ~48K tracks | CSV | artist, track, streams, audio features |
| **YouTube** | Track engagement | CSV | views, likes, comments |

### 🔄 The ETL Pipeline

**Entry point:** `scripts/run_pipeline.py`

Runs 11 deterministic steps in order. Stops on failure. No magic.

```
Extract        Clean           Match           Load           Validate
Pitchfork  →  Reviews      →  Artists     →  Warehouse   →  Tests & Constraints
Spotify    →  Features     →  (fuzzy)     →  (SQLite)    →  
YouTube    →  Metrics      →  [3-way]     →  (views)     →  ✅ Pass or Fail
```

**Key scripts:**
- `extract_pitchfork.py` — Parse Pitchfork SQLite dump
- `stage_reviews.py` — Type-safe review metadata
- `make_review_artists_bridge.py` — Handle multi-artist reviews
- `clean_spotify_youtube.py` — Validate audio features, aggregate metrics
- `match_artists.py` — **Fuzzy matching** (rapidfuzz) across 3 sources
- `stage_to_sqlite.py` — Load into warehouse
- `validate_dw.py` — **Final constraint checks** (uniqueness, referential integrity, nulls)

### 🗄️ Warehouse Schema

**File:** `data/processed/vinyl_dw.sqlite`

```
pitchfork_reviews ──┐
                    ├─→ pitchfork_review_artists ──→ dim_artist ──→ spotify_youtube_clean
                    │
            (bridge table for many-to-many reviews)
```

**Core tables:**
- `pitchfork_reviews` — 1 row per review
- `pitchfork_review_artists` — Reviews split into artist rows
- `dim_artist` — Canonical artist dimension with Spotify IDs
- `spotify_youtube_clean` — Streaming metrics & audio features

**Semantic views** (in `sql/dw/create_views.sql`):
- `vw_review_with_artist` — Reviews + matched artist info
- `vw_artist_summary` — Aggregated: review counts, scores
- `vw_artist_streams` — Aggregated: streaming data
- `vw_artist_critics_vs_streams` — ⭐ **Core analysis table** (17 columns, artist-level)
- `vw_unmatched_artists` — Quality check: artists without Spotify match

### ✅ Validation & Testing

Before analysis, constraints are enforced:

```python
validate_dw.py checks:
  ✅ Tables exist and have rows
  ✅ Primary keys are unique
  ✅ Foreign keys are valid (no orphans)
  ✅ Critical columns are non-null
  ✅ Bridge table references exist
```

**Tests:** `pytest tests/test_dw_smoke.py -v`

### 📈 Analysis & Modeling

**Notebook:** `notebooks/01_critics_vs_streams.ipynb`
- Load data from warehouse views
- Correlations: Pitchfork score vs. log(Spotify streams)
- Outlier detection: critically loved but commercially ignored
- Visualizations: scatter plots with artist labels

**Feature engineering:** `models/build_features.py`
- Critic signals: review_count, avg_score, year range
- Scale signals: track_count, total_streams  
- Engagement: YouTube views/likes/comments
- Audio features: danceability, energy, valence

**Baseline models:** `models/train_baseline.py`

| Model | R² | RMSE | MAE |
|-------|-----|------|-----|
| Linear Regression | 0.32 | [calc] | [calc] |
| Random Forest | 0.56 | [calc] | [calc] |

**Key insight:** Even with 15 features, streaming popularity is mostly unexplained. Platform dynamics and luck dominate.

**Outputs:**
- `reports/metrics.json` — Performance metrics
- `reports/predictions_top50_*.csv` — Predictions on top 50 artists
- `reports/feature_importance.csv` — Random Forest feature importance

### 📊 Interactive Dashboard

**File:** `reports/vinyl_critics_vs_streams_dashboard.pbix`

Power BI dashboard with:
- Scatter plot: Pitchfork score vs. log Spotify streams
- Genre & time trends
- Interactive filters for deep dives
- Artist labels on outliers

---

## Key Findings

### 🎯 Weak Correlation
Critics and listeners **don't align strongly** (R² ≈ 0.32). High Pitchfork scores don't guarantee streams.

### 📊 Mainstream Dominates
A few mega-artists get most streams, regardless of critical reception. **Algorithm and playlisting > critical acclaim.**

### 🎵 Hidden Gems Exist
Many critically acclaimed artists have tiny streaming numbers. **Great reviews ≠ discoverability.**

### 🤖 Models Can't Explain It
ML models barely improve over baseline. **Platform effects, marketing, and chance matter more than music quality.**

---

## Quick Start

### Prerequisites
- Python 3.10+
- ~2 GB disk

### 1. Setup

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

**Expected output:**
```
[1/11] extract_pitchfork ........................ OK
[2/11] inspect_pitchfork ........................ OK
[3/11] stage_reviews ............................ OK
...
[11/11] validate_dw ............................ OK
✓ Warehouse ready at: data/processed/vinyl_dw.sqlite
```

On error, stops immediately with clear message.

### 3. Explore

**Option A: Jupyter**
```bash
jupyter notebook notebooks/01_critics_vs_streams.ipynb
```

**Option B: Power BI**
Open `reports/vinyl_critics_vs_streams_dashboard.pbix` in Power BI Desktop.

**Option C: Query directly**
```python
import sqlite3
conn = sqlite3.connect("data/processed/vinyl_dw.sqlite")
df = pd.read_sql("""
  SELECT artist, review_count, avg_score, total_streams 
  FROM vw_artist_critics_vs_streams
  WHERE review_count >= 2
  ORDER BY total_streams DESC
  LIMIT 50
""", conn)
```

### 4. Train Models (Optional)

```bash
python models/build_features.py
python models/train_baseline.py
```

Results → `reports/metrics.json`, `reports/predictions_*.csv`

### 5. Run Tests

```bash
pytest tests/ -v
```

---

## Project Layout

```
vinyl-critics-vs-streams/
├── README.md
├── LICENSE
├── requirements.txt, requirements-dev.txt
│
├── data/
│   ├── raw/                 # Original data (not in git)
│   │   ├── pitchfork/
│   │   ├── spotify_attributes/
│   │   ├── spotify_youtube/
│   │   └── top_songs/
│   ├── interim/             # Staging CSVs (from ETL)
│   │   ├── pitchfork_reviews.csv
│   │   ├── pitchfork_review_artists.csv
│   │   ├── spotify_youtube_clean.csv
│   │   └── ...
│   └── processed/           # Final outputs
│       ├── vinyl_dw.sqlite  # ⭐ Warehouse
│       ├── artist_map.csv   # Artist matching results
│       └── model_features.csv
│
├── scripts/                 # ETL orchestration
│   ├── run_pipeline.py      # ⭐ Entry point
│   ├── extract_pitchfork.py
│   ├── stage_reviews.py
│   ├── make_review_artists_bridge.py
│   ├── build_artist_universe.py
│   ├── clean_spotify_youtube.py
│   ├── match_artists.py     # Fuzzy matching
│   ├── load_reviews_and_bridge.py
│   ├── load_dim_artist.py
│   ├── stage_to_sqlite.py
│   └── validate_dw.py       # ⭐ Final validation
│
├── sql/
│   └── dw/
│       └── create_views.sql # ⭐ Semantic layer
│
├── models/
│   ├── build_features.py    # Feature extraction
│   └── train_baseline.py    # Linear Regression + Random Forest
│
├── notebooks/
│   └── 01_critics_vs_streams.ipynb  # ⭐ Analysis & viz
│
├── reports/
│   ├── metrics.json
│   ├── feature_importance.csv
│   ├── predictions_top50_*.csv
│   ├── model_card.md
│   └── vinyl_critics_vs_streams_dashboard.pbix  # ⭐ Power BI
│
├── tests/
│   ├── test_dw_smoke.py     # Table/view checks
│   └── test_dw_constraints.py
│
├── docs/
│   └── data_dictionary.md   # Complete schema docs
│
└── assets/
    └── powerbi_dashboard_overview.png
```

---

## How We Built This

✅ **Multi-source ingestion** — Parsed 3 different data formats  
✅ **Entity resolution** — Fuzzy-matched artists across platforms  
✅ **Warehouse modeling** — Facts, dimensions, bridges, semantic views  
✅ **Data contracts** — Validation enforced, no bad data downstream  
✅ **Reproducibility** — Single CLI entry point, deterministic execution  
✅ **Testing & CI** — Pytest suite, GitHub Actions pipeline  
✅ **Exploration** — Jupyter + Power BI for insights  
✅ **ML baseline** — Clean feature pipeline, defensible models

---

## Tech Stack

- **Data:** pandas, numpy, SQLite, SQL
- **Entity matching:** rapidfuzz (fuzzy string similarity)
- **ML:** scikit-learn (Linear Regression, Random Forest)
- **Visualization:** matplotlib, Power BI
- **Testing:** pytest
- **Orchestration:** Python CLI (subprocess)
- **CI/CD:** GitHub Actions

---

## Next Steps & Ideas

- **Manual artist matching rules** for problem cases
- **Track-level modeling** (instead of artist aggregates)
- **Popularity clustering** (find similar artist cohorts)
- **Web dashboard** (Streamlit for real-time exploration)
- **Temporal analysis** (do critic trends predict streaming trends?)
- **Causal inference** (isolate platform effects from artist quality)

---

## Documentation

- **[Data Dictionary](docs/data_dictionary.md)** — Table/view schema details
- **[Model Card](reports/model_card.md)** — Model documentation & limitations
- **[Power BI Dashboard](reports/vinyl_critics_vs_streams_dashboard.pbix)** — Interactive exploration

---

## License

MIT — use, modify, and distribute freely.

---

**Real data. Real problems. Real solutions.**
