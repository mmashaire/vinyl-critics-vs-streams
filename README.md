<p align="left">
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/Python-3.10+-blue.svg" alt="Python 3.10+">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License">
  </a>
</p>

# Vinyl: Critics vs Streams

This project asks a simple question: when critics love an album, do listeners show up too?

To answer it, the repo pulls together Pitchfork reviews and Spotify/YouTube listening data, resolves artist identity across sources, builds a SQLite warehouse, trains a couple of baseline models, and packages the results in a notebook, a Power BI file, and a Streamlit dashboard.

The point is not to pretend music success can be reduced to one number. The point is to show the full path from messy source data to something another person can inspect, test, and run.

## Why this project is worth reading

- It deals with the unglamorous part of data work: cleaning, matching, validation, and making tradeoffs explicit.
- It keeps the stack simple on purpose: Python, SQLite, Pandas, scikit-learn.
- It ends in working outputs, not just scripts: a warehouse, a notebook, model reports, and an interactive dashboard.
- It is small enough to understand in one sitting, but substantial enough to show engineering judgment.

## What the repo does, end to end

1. Extracts and stages Pitchfork review data.
2. Cleans streaming and engagement data from Spotify/YouTube exports.
3. Matches artists across sources, including fuzzy matching where names do not line up cleanly.
4. Loads the cleaned result into a SQLite warehouse and builds analysis views.
5. Trains baseline models to see how far critic and streaming signals can go.
6. Surfaces the result in analysis artifacts that are easy to browse.

The main entry point for the build is `scripts/run_pipeline.py`. It runs the ETL in a fixed order and stops on the first failure.

## Snapshot of the current project state

- 18,389 Pitchfork reviews in the current warehouse-backed dashboard.
- 8,797 artists available in the current artist dimension.
- 282 artist-level rows in the modeling dataset.
- 14 engineered features used in the baseline models.
- Linear Regression: R² = 0.322, RMSE = 0.798, MAE = 0.591.
- Random Forest: R² = 0.560, RMSE = 0.643, MAE = 0.423.

The broad takeaway is straightforward: critic scores and streaming success are related, but only weakly. A better model helps, but it still leaves a lot unexplained.

## What is in the repo

| Area | Purpose |
| --- | --- |
| `scripts/` | ETL steps, warehouse loading, validation, and maintenance helpers |
| `sql/dw/create_views.sql` | Analysis views on top of the warehouse |
| `models/` | Feature building and baseline model training |
| `notebooks/01_critics_vs_streams.ipynb` | Exploratory analysis and visual inspection |
| `dashboard.py` | Streamlit dashboard for quick, interactive exploration |
| `reports/` | Metrics, predictions, model card, feature importance, Power BI file |
| `tests/` | Unit tests, smoke tests, and warehouse constraint checks |

## Architecture

For a visual overview of the data pipeline flow, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Data flow

The project is organized around a clear sequence rather than one large script.

- `extract_pitchfork.py` exports the source review data.
- `stage_reviews.py` normalizes review fields and types.
- `make_review_artists_bridge.py` handles reviews that belong to more than one artist.
- `clean_spotify_youtube.py` cleans streaming and engagement inputs.
- `build_artist_universe.py` and `match_artists.py` create a shared artist layer.
- `load_reviews_and_bridge.py`, `load_dim_artist.py`, and `stage_to_sqlite.py` build the SQLite warehouse.
- `validate_dw.py` checks that the warehouse is internally consistent before anything downstream uses it.

The semantic layer in `sql/dw/create_views.sql` makes the warehouse easier to query for analysis and modeling.

## What I found

This is not a story where the model discovers a hidden law of music.

- Strong reviews do not reliably translate into strong streaming numbers.
- Listener scale is heavily concentrated among a small number of very large artists.
- Better features help, but they do not erase the gap between critical reception and platform popularity.
- The interesting work is upstream: getting the data trustworthy enough that the result means anything at all.

That is why this repo leans as hard on validation, matching, and reproducibility as it does on modeling.

## Recent quality hardening

Recent updates focused on safety and trustworthiness over adding more features:

- Warehouse validation now handles primary-key checks more safely for evolving schemas.
- Spotify/YouTube cleaning now trims essential text fields and drops whitespace-only artist/track values.
- Cleaner input now fails fast when required columns are missing, so bad exports do not silently flow downstream.
- Added focused validator and cleaner regression tests to lock these behaviors in.

## Run it locally

### 1. Set up a virtual environment

```bash
git clone <repo-url>
cd vinyl-critics-vs-streams
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 2. Explore what is already in the repo

If you just want to inspect the project, you do not need to rebuild everything first.

Run the dashboard:

```bash
python -m streamlit run dashboard.py --server.port 8502
```

Then open `http://localhost:8502`.

Other quick entry points:

- Notebook: `notebooks/01_critics_vs_streams.ipynb`
- Power BI: `reports/vinyl_critics_vs_streams_dashboard.pbix`
- Model metrics: `reports/metrics.json`
- Warehouse file: `data/processed/vinyl_dw.sqlite`

### 3. Rebuild the pipeline

```bash
python scripts/run_pipeline.py
```

This runs the ETL in order and stops on the first failing step.

Note: the repo includes processed outputs for exploration, but a full rebuild still depends on the raw source files being present under `data/raw/`.

### 4. Rebuild the modeling outputs

```bash
python models/build_features.py
python models/train_baseline.py
```

This updates the feature set, metrics, feature importance report, and top-50 prediction files.

### 5. Run the checks

```bash
pytest -q
```

There is also a GitHub Actions workflow in `.github/workflows/ci.yml` that installs dependencies and runs tests automatically.

## Warehouse shape

The warehouse is intentionally compact.

- `pitchfork_reviews` stores review-level metadata.
- `pitchfork_review_artists` breaks reviews out to artist rows.
- `dim_artist` holds the matched artist dimension.
- `spotify_youtube_clean` holds cleaned streaming, engagement, and audio-feature data.

The views in `sql/dw/create_views.sql` roll those tables up into more usable analysis outputs such as artist summaries, artist stream totals, unmatched artists, and yearly coverage.

## Dashboard and reporting

The project now has two different reporting surfaces:

- A Power BI report for polished desktop exploration.
- A Streamlit dashboard for local, interactive browsing without leaving Python.

The Streamlit dashboard focuses on the core story:

- overview metrics
- artist exploration
- review score distributions
- model prediction plots
- project context and summary

It reads from the current SQLite warehouse and the generated model report CSVs, so it is tightly coupled to the rest of the repo instead of being a separate demo.

## Project layout

```text
data/                raw, interim, and processed datasets
scripts/             ETL pipeline, loading, validation, maintenance
models/              feature engineering and baseline training
sql/                 analysis views for the warehouse
notebooks/           exploratory analysis
reports/             metrics, predictions, model card, Power BI output
tests/               smoke tests and warehouse checks
dashboard.py         Streamlit dashboard
```

## If you are reviewing this as a portfolio project

The strongest part of the repo is the connective tissue.

It is easy to build a notebook on top of a clean CSV. It is harder, and more representative of real work, to move from raw source files to matching logic, to warehouse checks, to model outputs, and finally to something another person can open and interrogate.

That is the story this project is trying to tell.

## Documentation

- `ARCHITECTURE.md` for the pipeline and warehouse overview
- `docs/data_dictionary.md` for schema details
- `reports/model_card.md` for model limitations and reporting context

## License

MIT
