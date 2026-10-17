from __future__ import annotations

import json

import numpy as np
import pandas as pd

from models import train_baseline


def test_main_reports_mean_baseline_and_holdout_size(
    tmp_path, monkeypatch
) -> None:
    feature_path = tmp_path / "model_features.csv"
    row_count = 12
    pd.DataFrame(
        {
            "artist": [f"Artist {index}" for index in range(row_count)],
            "total_streams": np.expm1(np.arange(row_count) * 0.3),
            "signal": np.arange(row_count),
        }
    ).to_csv(feature_path, index=False)

    reports_dir = tmp_path / "reports"
    monkeypatch.setattr(train_baseline, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        "sys.argv",
        [
            "train_baseline.py",
            "--features",
            str(feature_path),
            "--seed",
            "7",
            "--test-size",
            "0.25",
            "--rf-trees",
            "5",
        ],
    )

    assert train_baseline.main() == 0

    metrics = json.loads((reports_dir / "metrics.json").read_text(encoding="utf-8"))
    by_model = {metric["model"]: metric for metric in metrics}

    assert set(by_model) == {"mean_baseline", "linear_regression", "random_forest"}
    assert by_model["linear_regression"]["r2"] > by_model["mean_baseline"]["r2"]
    assert not (reports_dir / "predictions_top50_mean_baseline.csv").exists()
    for metric in metrics:
        assert metric["n_train_rows"] == 9
        assert metric["n_test_rows"] == 3
        assert metric["seed"] == 7
        assert metric["test_size"] == 0.25
