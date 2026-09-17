"""Train and select the deployed FraudWatch stream model on IEEE-CIS (fs-1.1 features, amounts in source units).

Candidates: logistic regression (class-weighted) and LightGBM (early-stopped on
validation PR-AUC). Selection and thresholds use validation only; the test split is
scored once at the end.

Usage:  uv run python training/train_stream_model.py
"""
from __future__ import annotations

import json
import platform
import sys
import time
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evaluation.metrics import at_threshold, full_report, threshold_max_f1, threshold_top_fraction  # noqa: E402
from fwcore import AMOUNT_UNIT, FEATURE_SPEC_VERSION, POLICY_VERSION, RULESET_VERSION  # noqa: E402
from fwcore import features as F  # noqa: E402
from fwcore import policy, rules  # noqa: E402
from fwcore.schema import CARD_NETWORKS, CARD_TYPES, DEVICE_TYPES, EMAIL_DOMAINS, PRODUCT_CODES  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SEED = 20260917
MODEL_VERSION = "fw-ieee-stream-1.1.0"
REVIEW_CAPACITY = 0.10  # assumed analyst capacity: review band flags ~10% of validation traffic
CATEGORIES = {
    "product_cd": list(PRODUCT_CODES),
    "card_network": list(CARD_NETWORKS),
    "card_type": list(CARD_TYPES),
    "email_domain": list(EMAIL_DOMAINS),
    "device_type": list(DEVICE_TYPES),
    "has_identity": ["no", "yes"],
}


def load() -> tuple[pd.DataFrame, pd.DataFrame]:
    t = pd.read_parquet(ROOT / "data" / "processed" / "ieee_stream_features.parquet")
    x = t[[f"x_{c}" for c in F.MODEL_FEATURES]].copy()
    x.columns = F.MODEL_FEATURES
    for c, cats in CATEGORIES.items():
        x[c] = pd.Categorical(x[c], categories=cats)
    return t, x


def make_lr() -> Pipeline:
    pre = ColumnTransformer([
        ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), F.NUMERIC_FEATURES),
        ("cat", OneHotEncoder(categories=[CATEGORIES[c] for c in F.CATEGORICAL_FEATURES], handle_unknown="ignore",
                              sparse_output=False), F.CATEGORICAL_FEATURES),
    ])
    return Pipeline([("pre", pre), ("clf", LogisticRegression(class_weight="balanced", max_iter=3000, C=1.0,
                                                               random_state=SEED))])


def main() -> None:
    t0 = time.time()
    table, x = load()
    y = table["label"].to_numpy()
    tr, va, te = (table["split"] == s for s in ("train", "validation", "test"))

    results: dict = {"candidates": {}}

    # --- Logistic regression -------------------------------------------------
    lr = make_lr()
    lr.fit(x[tr], y[tr])
    s_va_lr = lr.predict_proba(x[va])[:, 1]
    results["candidates"]["logistic_regression"] = {
        "imbalance_handling": "class_weight=balanced (training split only)",
        "validation_pr_auc": float(average_precision_score(y[va], s_va_lr)),
    }

    # --- LightGBM --------------------------------------------------------------
    gbm = lgb.LGBMClassifier(
        n_estimators=3000, learning_rate=0.03, num_leaves=63, min_child_samples=100,
        subsample=0.8, subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0,
        random_state=SEED, n_jobs=-1, verbose=-1,
    )
    gbm.fit(x[tr], y[tr], eval_X=(x[va],), eval_y=(y[va],), eval_metric="average_precision",
            callbacks=[lgb.early_stopping(150, verbose=False)])
    s_va_gbm = gbm.predict_proba(x[va])[:, 1]
    results["candidates"]["lightgbm"] = {
        "imbalance_handling": "no reweighting; decision threshold tuned on validation; early stopping on validation PR-AUC",
        "best_iteration": int(gbm.best_iteration_),
        "validation_pr_auc": float(average_precision_score(y[va], s_va_gbm)),
    }

    selected = max(results["candidates"], key=lambda k: results["candidates"][k]["validation_pr_auc"])
    model = gbm if selected == "lightgbm" else lr
    s_va = s_va_gbm if selected == "lightgbm" else s_va_lr

    alert_thr = threshold_max_f1(y[va], s_va)
    review_thr = min(alert_thr, threshold_top_fraction(s_va, REVIEW_CAPACITY))

    # --- Held-out test (touched once) ----------------------------------------
    test = {}
    for name, m in (("logistic_regression", lr), ("lightgbm", gbm)):
        sv = s_va_lr if name == "logistic_regression" else s_va_gbm
        thr = threshold_max_f1(y[va], sv)
        st = m.predict_proba(x[te])[:, 1]
        test[name] = full_report(y[te], st, thr)
        test[name]["validation_selected_threshold"] = thr
        if name == selected:
            s_te = st
    # Accounts never seen in training: less optimistic view (IEEE labels propagate within accounts).
    train_uids = set(table.loc[tr, "uid"])
    unseen = te & ~table["uid"].isin(train_uids)
    results["test_unseen_accounts"] = full_report(y[unseen], model.predict_proba(x[unseen])[:, 1], alert_thr)
    results["test_unseen_accounts"].pop("curves")

    # --- Policy evaluation (model + rules) on test ------------------------------
    raw_cols = ["amount", "prior_count_60s", "prior_count_10m", "prior_count_1h", "prior_amount_sum_1h",
                "small_count_10m", "secs_since_last", "prior_count_total", "prior_mean_amount", "amount_ratio"]
    raw_te = table.loc[te, ["amount"] + [f"raw_{c}" for c in raw_cols[1:]]].copy()
    raw_te.columns = raw_cols
    actions, fired_counts = [], {r["rule_id"]: 0 for r in rules.RULES}
    for (_, rr), sc in zip(raw_te.iterrows(), s_te):
        d = rr.to_dict()
        for k in ("secs_since_last", "prior_mean_amount", "amount_ratio"):
            if pd.isna(d[k]):
                d[k] = None
        for k in ("prior_count_60s", "prior_count_10m", "prior_count_1h", "small_count_10m", "prior_count_total"):
            d[k] = int(d[k])
        ev = rules.evaluate(d)
        for r in ev:
            fired_counts[r["rule_id"]] += int(r["fired"])
        actions.append(policy.decide(float(sc), alert_thr, review_thr, ev, late=False)["action"])
    actions = np.array(actions)
    yt = y[te]
    pol = {}
    for a in ("alert", "review", "allow"):
        m = actions == a
        pol[a] = {"count": int(m.sum()), "share": float(m.mean()), "fraud_rate_within": float(yt[m].mean()) if m.any() else None}
    flagged = np.isin(actions, ["alert", "review"])
    pol["flagged_recall"] = float(yt[flagged].sum() / yt.sum())
    pol["rule_fire_counts"] = fired_counts
    results["test_policy"] = pol

    # --- Artifacts ---------------------------------------------------------------
    art = ROOT / "artifacts" / MODEL_VERSION
    art.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, art / "model.joblib")
    importance = None
    if selected == "lightgbm":
        importance = sorted(
            [{"feature": f, "gain": float(g)} for f, g in
             zip(F.MODEL_FEATURES, gbm.booster_.feature_importance(importance_type="gain"))],
            key=lambda d: -d["gain"])

    # CA1 evaluation artifacts (aggregates only; no row-level data) ---------------------------------
    splits = {"train": tr, "validation": va, "test": te}
    class_balance = {k: {"rows": int(m.sum()), "fraud": int(y[m].sum()), "legit": int((y[m] == 0).sum()),
                         "fraud_rate": float(y[m].mean())} for k, m in splits.items()}
    sweep_thr = np.unique(np.concatenate([np.quantile(s_va, np.linspace(0.5, 0.999, 60)), [alert_thr, review_thr]]))
    sweep = []
    for thr in sweep_thr:
        r = at_threshold(y[va], s_va, float(thr))
        sweep.append({"threshold": r["threshold"], "precision": r["precision"], "recall": r["recall"], "f1": r["f1"],
                      "flagged": int(round(r["flagged_rate"] * int(va.sum()))), "flagged_rate": r["flagged_rate"]})
    shap_global = None
    if selected == "lightgbm":
        rng = np.random.default_rng(SEED)
        te_idx = np.flatnonzero(te)
        sample = np.sort(rng.choice(te_idx, size=min(20000, len(te_idx)), replace=False))
        contrib = gbm.booster_.predict(x.iloc[sample], pred_contrib=True)  # TreeSHAP, log-odds; last column = bias
        shap_global = {
            "method": "mean |TreeSHAP contribution| (LightGBM pred_contrib=True, log-odds units) on a seeded random "
                      f"sample of {len(sample)} held-out test rows",
            "expected_value_log_odds": float(contrib[:, -1].mean()),
            "features": sorted([{"feature": f, "mean_abs_shap": float(np.abs(contrib[:, i]).mean())}
                                for i, f in enumerate(F.MODEL_FEATURES)], key=lambda d: -d["mean_abs_shap"]),
        }
    evaluation = {
        "model_version": MODEL_VERSION,
        "dataset": "IEEE-CIS Fraud Detection, provided by Vesta Corporation through the Kaggle competition ieee-fraud-detection",
        "split_reference": "chronological 70/15/15 by TransactionDT (ties by TransactionID); see dataset.split in metadata.json",
        "amount_unit": AMOUNT_UNIT,
        "class_balance": class_balance,
        "thresholds": {"alert": alert_thr, "review": review_thr, "selected_on": "validation"},
        "held_out_test": {
            "n": int(te.sum()),
            "confusion_matrix_at_alert_threshold": test[selected]["at_threshold"]["confusion_matrix"],
            "at_alert_threshold": {k: v for k, v in test[selected]["at_threshold"].items() if k != "confusion_matrix"},
            "at_review_threshold": at_threshold(y[te], s_te, review_thr),
            "pr_auc": test[selected]["pr_auc"], "roc_auc": test[selected]["roc_auc"],
            "pr_curve": {"points": test[selected]["curves"]["pr"], "axes": ["recall", "precision"]},
            "roc_curve": {"points": test[selected]["curves"]["roc"], "axes": ["fpr", "tpr"]},
            "calibration": test[selected]["calibration"],
        },
        "validation_threshold_sweep": sweep,
        "baseline_comparison": {
            name: {"validation_pr_auc": results["candidates"][name]["validation_pr_auc"],
                   "test_pr_auc": test[name]["pr_auc"], "test_roc_auc": test[name]["roc_auc"],
                   "test_at_validation_threshold": test[name]["at_threshold"]}
            for name in ("logistic_regression", "lightgbm")},
        "global_importance": {
            "gain": {"method": "LightGBM total split gain (feature_importance(importance_type='gain'))", "features": importance},
            "shap": shap_global},
        "per_prediction_explanations": "served live by the scoring service: LightGBM pred_contrib=True (TreeSHAP), "
                                       "contributions in log-odds, bias term = expected value",
        "test_unseen_accounts": results["test_unseen_accounts"],
        "test_policy": pol,
    }
    (art / "evaluation.json").write_text(json.dumps(evaluation, indent=2))

    prepare = json.loads((ROOT / "data" / "processed" / "prepare_manifest.json").read_text())
    metadata = {
        "model_version": MODEL_VERSION,
        "model_type": selected,
        "score_semantics": "model score in [0,1]; not a calibrated probability unless calibration results support it",
        "feature_spec": FEATURE_SPEC_VERSION,
        "features": {"numeric": F.NUMERIC_FEATURES, "categorical": F.CATEGORICAL_FEATURES, "categories": CATEGORIES},
        "thresholds": {"alert": alert_thr, "review": review_thr,
                       "alert_method": "max F1 on validation", "review_method": f"min(alert, top {REVIEW_CAPACITY:.0%} of validation scores)"},
        "ruleset_version": RULESET_VERSION,
        "policy_version": POLICY_VERSION,
        "amount_unit": AMOUNT_UNIT,
        "seed": SEED,
        "dataset": prepare,
        "explanations": "LightGBM pred_contrib (TreeSHAP, log-odds)" if selected == "lightgbm" else "LR coefficient x standardised value (log-odds)",
        "feature_importance_gain": importance,
        "training_command": "cd ml && uv run python training/ieee_data.py && uv run python training/train_stream_model.py",
        "environment": {"python": sys.version.split()[0], "platform": platform.platform(),
                        "scikit_learn": sklearn.__version__, "lightgbm": lgb.__version__,
                        "pandas": pd.__version__, "numpy": np.__version__},
        "trained_at_unix": int(time.time()),
    }
    (art / "metadata.json").write_text(json.dumps(metadata, indent=2))

    results.update({
        "model_version": MODEL_VERSION, "selected": selected, "selection_rule": "highest validation PR-AUC",
        "thresholds": metadata["thresholds"], "test": test, "seconds": round(time.time() - t0, 1),
    })
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    (out / "stream_model_results.json").write_text(json.dumps(results, indent=2))
    summary = {k: {"val_pr_auc": v["validation_pr_auc"]} for k, v in results["candidates"].items()}
    for k in test:
        summary[k].update({m: test[k][m] for m in ("pr_auc", "roc_auc")})
        summary[k].update({m: test[k]["at_threshold"][m] for m in ("precision", "recall", "f1")})
    print(json.dumps({"selected": selected, "summary": summary, "thresholds": metadata["thresholds"],
                      "policy": {k: pol[k] for k in ("alert", "review", "flagged_recall")},
                      "unseen": {k: results["test_unseen_accounts"][k] for k in ("n", "pr_auc")}}, indent=2))


if __name__ == "__main__":
    main()
