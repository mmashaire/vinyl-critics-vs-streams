# Contributing

Thanks for helping improve this project.

## Local setup

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
   - `pip install -r requirements-dev.txt`
3. Run the test suite before opening a PR:
   - `pytest -q`

## Pipeline checks

Use the pipeline entry point to verify wiring without running the full ETL:

- `python scripts/run_pipeline.py --list`
- `python scripts/run_pipeline.py --dry-run`

If you do need to rebuild outputs locally, run:

- `python scripts/run_pipeline.py`

## Review expectations

- Keep changes focused and easy to review.
- Prefer small, readable diffs.
- Update documentation when behavior or workflow changes.
- Call out assumptions clearly when data or model limitations matter.
