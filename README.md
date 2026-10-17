# Vinyl: Critics vs Streams

This project asks whether artists with higher Pitchfork review scores also tend to have more Spotify and YouTube activity. It combines review and listening data, matches artist names, and compares a few simple models.

## What happens

1. Clean review and listening data.
2. Match artists across the two sources.
3. Store the results in a local SQLite database.
4. Compare critic scores and listening activity in reports, a notebook, and dashboards.

The main pipeline is `scripts/run_pipeline.py`. It runs each step in order and stops if one fails.

## What the results say

The modeling data has 282 artists. Results below use the same 57-artist test group:

| Model | R² |
| --- | ---: |
| Predict the average (simple baseline) | 0.000 |
| Linear regression | 0.322 |
| Random forest | 0.560 |

R² is a rough measure of how much the model explains differences between artists; higher is better. These results come from one small test group. They do not show that reviews cause listening activity or predict future success. More detail is in [the model card](reports/model_card.md) and [the metrics report](reports/metrics.json).

## Try it

The raw input files and SQLite database are not included in Git. You need local copies of the source files before you can rebuild the database or run the dashboard.

Set up Python and install the packages:

```bash
python -m venv .venv
```

Activate the environment, then install the project packages:

```bash
# Windows
.venv\Scripts\activate

# macOS or Linux
source .venv/bin/activate

python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

With the required input files in place, rebuild the database and check the project:

```bash
python scripts/run_pipeline.py
python -m pytest -q
```

To open the dashboard, first make sure `data/processed/vinyl_dw.sqlite` exists, then run:

```bash
python -m streamlit run dashboard.py
```

You can browse the notebook at `notebooks/01_critics_vs_streams.ipynb`, the Power BI report at `reports/vinyl_critics_vs_streams_dashboard.pbix`, or the saved results in `reports/` without rebuilding the database.

## Project files

- `scripts/`: data cleaning, matching, and database steps
- `models/`: feature building and model training
- `reports/`: saved results and model limitations
- `notebooks/`: analysis
- `tests/`: automated checks

See [ARCHITECTURE.md](ARCHITECTURE.md) for the pipeline overview, [the data dictionary](docs/data_dictionary.md) for table descriptions, and [CONTRIBUTING.md](CONTRIBUTING.md) for contributor setup.
