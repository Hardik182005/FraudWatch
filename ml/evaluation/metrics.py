"""Evaluation helpers shared by the FraudWatch training scripts."""
from __future__ import annotations

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)


def threshold_max_f1(y: np.ndarray, s: np.ndarray) -> float:
    p, r, t = precision_recall_curve(y, s)
    denom = p[:-1] + r[:-1]
    f1 = np.divide(2 * p[:-1] * r[:-1], denom, out=np.zeros_like(denom), where=denom > 0)
    return float(t[int(np.argmax(f1))])


def threshold_top_fraction(s: np.ndarray, fraction: float) -> float:
    return float(np.quantile(s, 1 - fraction))


def at_threshold(y: np.ndarray, s: np.ndarray, thr: float) -> dict:
    pred = (s >= thr).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "threshold": float(thr),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "accuracy": float((tp + tn) / len(y)),
        "flagged_rate": float(pred.mean()),
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
    }


def calibration(y: np.ndarray, s: np.ndarray, bins: int = 10) -> dict:
    """Quantile-binned reliability table + Brier score + expected calibration error."""
    edges = np.unique(np.quantile(s, np.linspace(0, 1, bins + 1)))
    ids = np.clip(np.searchsorted(edges, s, side="right") - 1, 0, len(edges) - 2)
    table, ece = [], 0.0
    for b in range(len(edges) - 1):
        m = ids == b
        if not m.any():
            continue
        mean_s, frac = float(s[m].mean()), float(y[m].mean())
        ece += m.mean() * abs(mean_s - frac)
        table.append({"bin": b, "count": int(m.sum()), "mean_score": mean_s, "observed_fraud_rate": frac})
    return {"brier": float(brier_score_loss(y, s)), "ece": float(ece), "bins": table}


def downsample_curve(xs: np.ndarray, ys: np.ndarray, n: int = 200) -> list[list[float]]:
    if len(xs) <= n:
        idx = np.arange(len(xs))
    else:
        idx = np.unique(np.linspace(0, len(xs) - 1, n).astype(int))
    return [[round(float(xs[i]), 5), round(float(ys[i]), 5)] for i in idx]


def full_report(y: np.ndarray, s: np.ndarray, thr: float) -> dict:
    p, r, _ = precision_recall_curve(y, s)
    fpr, tpr, _ = roc_curve(y, s)
    return {
        "n": int(len(y)),
        "positives": int(y.sum()),
        "prevalence": float(y.mean()),
        "pr_auc": float(average_precision_score(y, s)),
        "roc_auc": float(roc_auc_score(y, s)) if 0 < y.sum() < len(y) else None,
        "at_threshold": at_threshold(y, s, thr),
        "all_legitimate_baseline_accuracy": float(1 - y.mean()),
        "calibration": calibration(y, s),
        "curves": {"pr": downsample_curve(r, p), "roc": downsample_curve(fpr, tpr)},
    }
