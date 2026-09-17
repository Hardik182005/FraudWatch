"""Versioned model loading, feature transformation and explanation."""
from __future__ import annotations

import json
import math
import time
from pathlib import Path

import joblib
import numpy as np

from fwcore import features as F
from fwcore import policy, rules

RULE_TITLES = {r["rule_id"]: r["title"] for r in rules.RULES}
RAW_INT_FIELDS = ("prior_count_60s", "prior_count_10m", "prior_count_1h", "small_count_10m", "prior_count_total")
RAW_FLOAT_FIELDS = ("amount_inr", "prior_amount_sum_1h")
RAW_OPTIONAL_FIELDS = ("secs_since_last", "prior_mean_amount", "amount_ratio")


class RawFeatureError(ValueError):
    pass


def normalise_raw(raw: object) -> dict:
    if not isinstance(raw, dict):
        raise RawFeatureError("raw must be an object")
    out: dict = {}
    for k in RAW_INT_FIELDS:
        v = raw.get(k)
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0 or (isinstance(v, float) and not v.is_integer()):
            raise RawFeatureError(f"raw.{k} must be a non-negative integer")
        out[k] = int(v)
    for k in RAW_FLOAT_FIELDS:
        v = raw.get(k)
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0:
            raise RawFeatureError(f"raw.{k} must be a non-negative number")
        out[k] = float(v)
    for k in RAW_OPTIONAL_FIELDS:
        v = raw.get(k)
        if v is not None and (isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0):
            raise RawFeatureError(f"raw.{k} must be null or a non-negative number")
        out[k] = None if v is None else float(v)
    return out


class Model:
    def __init__(self, artifact_dir: Path):
        self.dir = artifact_dir
        self.metadata = json.loads((artifact_dir / "metadata.json").read_text())
        self.version = self.metadata["model_version"]
        self.kind = self.metadata["model_type"]
        self.alert = float(self.metadata["thresholds"]["alert"])
        self.review = float(self.metadata["thresholds"]["review"])
        self.estimator = joblib.load(artifact_dir / "model.joblib")
        cats = self.metadata["features"]["categories"]
        self.categories = {c: {v: i for i, v in enumerate(cats[c])} for c in F.CATEGORICAL_FEATURES}
        if self.kind == "lightgbm":
            self.booster = self.estimator.booster_
            expected = [cats[c] for c in F.CATEGORICAL_FEATURES]
            if self.booster.feature_name() != F.MODEL_FEATURES or self.booster.pandas_categorical != expected:
                raise RuntimeError("model artifact does not match feature spec fs-1.0")
        self.loaded_at = int(time.time() * 1000)

    # -- inference ---------------------------------------------------------------------
    def _matrix(self, inputs: list[dict]) -> np.ndarray:
        x = np.full((len(inputs), len(F.MODEL_FEATURES)), np.nan)
        for i, row in enumerate(inputs):
            for j, name in enumerate(F.MODEL_FEATURES):
                v = row[name]
                if name in self.categories:
                    code = self.categories[name].get(v)
                    x[i, j] = np.nan if code is None else code
                elif v is not None:
                    x[i, j] = v
        return x

    def score(self, events: list[dict], raws: list[dict], lates: list[bool], top_k: int = 5) -> list[dict]:
        inputs = [F.model_inputs(r, e) for r, e in zip(raws, events)]
        started = time.perf_counter()
        if self.kind == "lightgbm":
            x = self._matrix(inputs)
            scores = self.booster.predict(x)
            contrib = self.booster.predict(x, pred_contrib=True)
            method = "treeshap_log_odds"
        else:  # logistic regression pipeline fallback
            import pandas as pd

            frame = pd.DataFrame(inputs, columns=F.MODEL_FEATURES).astype({c: "object" for c in F.CATEGORICAL_FEATURES})
            scores = self.estimator.predict_proba(frame)[:, 1]
            contrib = None
            method = "not_available"
        per_item_ms = (time.perf_counter() - started) * 1000 / max(len(inputs), 1)
        now = int(time.time() * 1000)
        out = []
        for i, (event, raw, late, mi) in enumerate(zip(events, raws, lates, inputs)):
            score = float(scores[i])
            explanation = {"method": method, "base_value": None, "top": []}
            if contrib is not None:
                row = contrib[i]
                explanation["base_value"] = float(row[-1])
                order = np.argsort(-np.abs(row[:-1]))[:top_k]
                explanation["top"] = [
                    {"feature": F.MODEL_FEATURES[j], "value": mi[F.MODEL_FEATURES[j]], "contribution": round(float(row[j]), 6)}
                    for j in order
                ]
            rule_results = rules.evaluate(raw)
            for r in rule_results:
                r["title"] = RULE_TITLES[r["rule_id"]]
            decision = policy.decide(score, self.alert, self.review, rule_results, late)
            out.append({
                "event_id": event["event_id"],
                "model": {
                    "version": self.version,
                    "score": round(score, 6),
                    "thresholds": {"alert": self.alert, "review": self.review},
                    "explanation": explanation,
                },
                "rules": {"version": rules.RULESET_VERSION, "results": rule_results},
                "policy": decision,
                "model_inputs": mi,
                "scored_at": now,
                "scoring_ms": round(per_item_ms, 3),
            })
        return out
