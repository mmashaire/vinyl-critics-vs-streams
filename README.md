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

An end-to-end **data engineering & ML project** that answers a surprisingly tricky question:

> **Do critics and listeners like the same music?**

This repo demonstrates real-world data pipeline practices — messy ingestion, entity resolution, warehouse modeling, validation, and actionable insights — all with actual music industry data.

### The Question

Critics reward artistry, innovation, and cultural influence. Streaming platforms amplify reach, algorithmic discovery, and playlist placement. How closely do these worlds overlap? Where do they clash?

---

## What This Project Does

**Data pipeline side:** Ingest Pitchfork review data, Spotify metrics, and YouTube stats. Clean them. Match artists across sources. Build a SQLite warehouse with validated dimensions and facts. Expose a semantic layer.

**Analysis side:** Find the correlation (spoiler: it's weak). Identify outliers — critically loved but commercially ignored artists, and vice versa. Train ML models to see how much of streaming popularity can be explained by critical reception.

**Engineering side:** Prove it works. Automated validation, deterministic CI pipeline, schema contracts, comprehensive testing.

## The Pipeline

```mermaid
flowchart LR
    RAW["Raw Data"] --> ETL["Extract & Clean"]
    ETL --> STAGE["Staging CSVs"]
    STAGE --> DW["SQLite Warehouse"]
    DW --> VIEWS["Semantic Views"]
    VIEWS --> ANALYSIS["Analysis & ML"]
```

### 1️⃣ Raw Data
- **Pitchfork**: Review scores, artists, genres, labels
- **Spotify + YouTube**: Track metrics, streaming counts, audio features

### 2️⃣ Staging & Cleaning
All ETL lives in `scripts/`. The pipeline handles:
- **Type-safe parsing** of review metadata
- **Multi-artist splitting** (some reviews cover multiple acts)
- **Fuzzy matching** via `rapidfuzz` to resolve artists across platforms
- **Intermediate validation** before warehouse load

### 3️⃣ Data Warehouse
A single SQLite database (`data/processed/vinyl_dw.sqlite`) containing:
- **Fact tables** for reviews and streaming metrics
- **Canonical artist dimension** with matched IDs
- **Bridge tables** for many-to-many relationships

The semantic layer (in `sql/dw/create_views.sql`) exposes clean views:
- `vw_review_with_artist` — enriched review data
- `vw_artist_summary` — aggregated critic scores per artist
- `vw_artist_streams` — streaming metrics per artist
- `vw_artist_critics_vs_streams` — the core analysis table
- `vw_unmatched_artists` — data quality check

### 4️⃣ Validation & Testing
Before any analysis, the warehouse is tested:
- Table presence and schema checks
- Dimension uniqueness and referential integrity
- Null handling on critical fields
- Orphan detection in bridge tables
- Automated smoke tests in GitHub Actions

See `scripts/validate_dw.py` and `tests/` for details.

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

## How to Run It

### Setup
```bash
python -m venv venv
source venv/Scripts/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Run the full pipeline
```bash
python scripts/run_pipeline.py
```

This will:
- Extract and clean Pitchfork data
- Match artists across Spotify/YouTube
- Load the warehouse
- Validate constraints
- Stop on failure (deterministic CI-ready)

### Explore the analysis
Open `notebooks/01_critics_vs_streams.ipynb` to see the correlation analysis and visualizations.

### Run tests
```bash
pytest tests/
```

---

## Tech Stack

| Component | Tool |
|-----------|------|
| **Data Processing** | pandas, numpy |
| **Database** | SQLite + SQL views |
| **Entity Matching** | rapidfuzz (fuzzy string matching) |
| **ML** | scikit-learn (Linear Regression, Random Forest) |
| **Viz** | matplotlib, Power BI |
| **Testing** | pytest, GitHub Actions |
| **Analysis** | Jupyter Notebook |

---

## Why This Matters

This isn't a toy project. It demonstrates **real data engineering**:

✅ **Messy ingestion** — Pitchfork and Spotify have different schemas and artist naming  
✅ **Entity resolution** — Matching artists across sources is non-trivial  
✅ **Warehouse modeling** — Facts, dimensions, and bridges for queryability  
✅ **Data contracts** — Validation enforced before analysis  
✅ **Reproducibility** — Single entry point, deterministic pipeline, CI automation  
✅ **Defensible analysis** — From raw data to actionable conclusions

The focus is **clarity and rigor**, not flashy frameworks.
---

## Next Steps

Possible extensions (and why they'd be cool):

- **Manual override rules** for artist matching (when fuzzy matching isn't enough)
- **Track-level modeling** instead of artist aggregates (finer granularity)
- **Popularity clustering** (find cohorts of similar artists)
- **Lightweight dashboard** (Streamlit or Metabase for interactive exploration)
- **Temporal models** using review chronology (do critic trends predict streaming trends?)

---

## License

MIT — use freely, credit appreciated.

---

**Made to demonstrate how real data projects are built.** Questions? Open an issue or reach out.
