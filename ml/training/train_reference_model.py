"""Full-feature IEEE-CIS reference model (NOT deployed).

Uses every column in train_transaction + train_identity, including Vesta's engineered
C/D/M/V aggregates that a streaming engine could not recompute from raw events. It shows
how much signal the deployable fs-1.1 stream model gives up. Same chronological split,
same threshold procedure (validation only), test scored once.

Usage:  uv run python training/train_reference_model.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evaluation.metrics import full_report, threshold_max_f1  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "ieee-cis"
SEED = 20260917


def main() -> None:
    t0 = time.time()
    tx = pd.read_csv(RAW / "train_transaction.csv")
    ident = pd.read_csv(RAW / "train_identity.csv")
    df = tx.merge(ident, on="TransactionID", how="left")
    df = df.sort_values(["TransactionDT", "TransactionID"], kind="mergesort").reset_index(drop=True)
    del tx, ident

    y = df["isFraud"].to_numpy()
    drop = ["TransactionID", "isFraud", "TransactionDT"]  # raw time offset excluded: it only encodes the split
    x = df.drop(columns=drop)
    cat_cols = [c for c in x.columns if x[c].dtype == object or pd.api.types.is_string_dtype(x[c])]
    for c in cat_cols:
        x[c] = x[c].astype("category")
    for c in x.columns.difference(cat_cols):
        if x[c].dtype == np.float64:
            x[c] = x[c].astype(np.float32)

    n = len(df)
    b1, b2 = int(n * 0.70), int(n * 0.85)
    tr, va, te = slice(0, b1), slice(b1, b2), slice(b2, n)

    gbm = lgb.LGBMClassifier(
        n_estimators=5000, learning_rate=0.03, num_leaves=255, min_child_samples=100,
        subsample=0.8, subsample_freq=1, colsample_bytree=0.4, reg_lambda=1.0,
        random_state=SEED, n_jobs=-1, verbose=-1,
    )
    gbm.fit(x.iloc[tr], y[tr], eval_X=(x.iloc[va],), eval_y=(y[va],), eval_metric="average_precision",
            callbacks=[lgb.early_stopping(150, verbose=False)])
    s_va = gbm.predict_proba(x.iloc[va])[:, 1]
    thr = threshold_max_f1(y[va], s_va)
    s_te = gbm.predict_proba(x.iloc[te])[:, 1]
    report = full_report(y[te], s_te, thr)

    importance = sorted(
        [{"feature": f, "gain": float(g)} for f, g in
         zip(x.columns, gbm.booster_.feature_importance(importance_type="gain"))],
        key=lambda d: -d["gain"])[:25]
    results = {
        "model": "LightGBM full-feature reference (not deployed)",
        "purpose": "upper-bound comparison; uses dataset-provided aggregates unavailable to a raw-event stream",
        "n_features": int(x.shape[1]),
        "categorical_features": cat_cols,
        "split": "chronological 70/15/15 by TransactionDT (identical row order to the stream model)",
        "best_iteration": int(gbm.best_iteration_),
        "validation_pr_auc": float(average_precision_score(y[va], s_va)),
        "validation_selected_threshold": thr,
        "test": report,
        "top_features_gain": importance,
        "seed": SEED,
        "lightgbm": lgb.__version__,
        "seconds": round(time.time() - t0, 1),
    }
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    (out / "reference_model_results.json").write_text(json.dumps(results, indent=2))
    print(json.dumps({k: results[k] for k in ("n_features", "best_iteration", "validation_pr_auc", "seconds")}
                     | {"test_pr_auc": report["pr_auc"], "test_roc_auc": report["roc_auc"],
                        "test_at_threshold": {k: report["at_threshold"][k] for k in ("precision", "recall", "f1")}},
                     indent=2))


if __name__ == "__main__":
    main()
