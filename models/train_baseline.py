#!/usr/bin/env python3
"""
Train baseline models to predict streaming scale from critics + metadata.

Goal: a clean, defensible ML baseline.
- target: log1p(total_streams)
- models: LinearRegression, RandomForestRegressor
- metrics: RMSE, MAE, R2
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline


DEFAULT_FEATURES = Path("data/processed/model_features.csv")
REPORTS_DIR = Path("reports")
TARGET = "total_streams"


@dataclass
class Metrics:
    n_rows: int
    n_features: int
    target_transform: str
    model: str
    rmse: float
    mae: float
    r2: float


def _rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(math.sqrt(mean_squared_error(y_true, y_pred)))


def _evaluate(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    model_name: str,
    n_rows: int,
    n_features: int,
) -> Metrics:
    return Metrics(
        n_rows=n_rows,
        n_features=n_features,
        target_transform="log1p(total_streams)",
        model=model_name,
        rmse=_rmse(y_true, y_pred),
        mae=float(mean_absolute_error(y_true, y_pred)),
        r2=float(r2_score(y_true, y_pred)),
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", type=Path, default=DEFAULT_FEATURES, help="CSV from build_features.py")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--test-size", type=float, default=0.2)
    ap.add_argument("--rf-trees", type=int, default=300)
    ap.add_argument("--rf-max-depth", type=int, default=None)
    args = ap.parse_args()

    if not args.features.exists():
        raise FileNotFoundError(f"Missing features CSV: {args.features} (run models/build_features.py first)")

    df = pd.read_csv(args.features)

    if "artist" not in df.columns:
        raise RuntimeError("Expected 'artist' column in features CSV.")
    if TARGET not in df.columns:
        raise RuntimeError(f"Expected '{TARGET}' column in features CSV.")

    # Target: log1p(total_streams) to reduce skew
    y_raw = df[TARGET].astype(float).to_numpy()
    y = np.log1p(y_raw)

    # Features: everything except artist + target
    feature_cols = [c for c in df.columns if c not in ("artist", TARGET)]
    X = df[feature_cols].copy()

    # Train/test split (artist kept only for reporting)
    X_train, X_test, y_train, y_test, artist_train, artist_test = train_test_split(
        X,
        y,
        df["artist"].astype(str),
        test_size=args.test_size,
        random_state=args.seed,
    )

    preproc = ColumnTransformer(
        transformers=[
            ("num", Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))]), feature_cols),
        ],
        remainder="drop",
    )

    models = [
        ("linear_regression", LinearRegression()),
        (
            "random_forest",
            RandomForestRegressor(
                n_estimators=args.rf_trees,
                random_state=args.seed,
                n_jobs=-1,
                max_depth=args.rf_max_depth,
            ),
        ),
    ]

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    all_metrics: list[Metrics] = []
    fi_rows: list[dict] = []

    for name, estimator in models:
        pipe = Pipeline(steps=[("preprocess", preproc), ("model", estimator)])
        pipe.fit(X_train, y_train)
        preds = pipe.predict(X_test)

        m = _evaluate(y_test, preds, name, n_rows=len(df), n_features=len(feature_cols))
        all_metrics.append(m)

        model = pipe.named_steps["model"]
        if hasattr(model, "coef_"):
            for f, w in zip(feature_cols, model.coef_):
                fi_rows.append({"model": name, "feature": f, "importance": float(w)})
        elif hasattr(model, "feature_importances_"):
            for f, w in zip(feature_cols, model.feature_importances_):
                fi_rows.append({"model": name, "feature": f, "importance": float(w)})

        print(f"[ok] {name}: RMSE={m.rmse:.4f}  MAE={m.mae:.4f}  R2={m.r2:.4f}")

        # Save a small prediction sample for sanity-checking
        pred_df = pd.DataFrame(
            {
                "artist": artist_test.to_numpy(),
                "y_true_log1p": y_test,
                "y_pred_log1p": preds,
            }
        ).sort_values("y_true_log1p", ascending=False).head(50)
        pred_df.to_csv(REPORTS_DIR / f"predictions_top50_{name}.csv", index=False)

    # Write metrics JSON
    (REPORTS_DIR / "metrics.json").write_text(
        json.dumps([asdict(m) for m in all_metrics], indent=2),
        encoding="utf-8",
    )
    print(f"[ok] wrote {REPORTS_DIR / 'metrics.json'}")

    # Write feature importance/coefs
    if fi_rows:
        fi = pd.DataFrame(fi_rows).sort_values(["model", "importance"], ascending=[True, False])
        fi.to_csv(REPORTS_DIR / "feature_importance.csv", index=False)
        print(f"[ok] wrote {REPORTS_DIR / 'feature_importance.csv'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
