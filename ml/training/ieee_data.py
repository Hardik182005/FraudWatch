"""IEEE-CIS Fraud Detection: load, map to the FraudWatch event vocabulary, compute
fs-1.0 stream features in arrival (TransactionDT) order, and assign time-based splits.

Usage:  uv run python training/ieee_data.py
Output: data/processed/ieee_stream_features.parquet  (+ data/processed/prepare_manifest.json)
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from fwcore import FEATURE_SPEC_VERSION, REFERENCE_INR_PER_USD
from fwcore import features as F

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "ieee-cis"
OUT = ROOT / "data" / "processed"

SPLIT_FRACTIONS = (0.70, 0.15, 0.15)  # train / validation / test by TransactionDT order

TXN_COLS = ["TransactionID", "isFraud", "TransactionDT", "TransactionAmt", "ProductCD",
            "card1", "card4", "card6", "addr1", "P_emaildomain"]


def email_group(v: object) -> str:
    if not isinstance(v, str):
        return "missing"
    if v == "gmail.com":
        return "gmail.com"
    if v.startswith("yahoo."):
        return "yahoo.com"
    if v.startswith(("hotmail.", "outlook.", "live.")) or v == "msn.com":
        return "hotmail.com"
    if v in ("anonymous.com", "aol.com"):
        return v
    return "other"


def sha256_prefix(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_events() -> pd.DataFrame:
    tx = pd.read_csv(RAW / "train_transaction.csv", usecols=TXN_COLS)
    ident = pd.read_csv(RAW / "train_identity.csv", usecols=["TransactionID", "DeviceType"])
    df = tx.merge(ident, on="TransactionID", how="left", indicator=True)
    df["has_identity"] = df["_merge"].eq("both")
    df = df.drop(columns="_merge")
    df = df.sort_values(["TransactionDT", "TransactionID"], kind="mergesort").reset_index(drop=True)

    out = pd.DataFrame({
        "transaction_id": df["TransactionID"].astype("int64"),
        "label": df["isFraud"].astype("int8"),
        "event_time": (df["TransactionDT"].astype("int64") * 1000),
        "amount_inr": df["TransactionAmt"].astype("float64") * REFERENCE_INR_PER_USD,
        "product_cd": df["ProductCD"].astype(str),
        "card_network": df["card4"].fillna("unknown").astype(str),
        "card_type": df["card6"].map({"debit": "debit", "credit": "credit"}).fillna(
            df["card6"].map(lambda v: "unknown" if not isinstance(v, str) else "other")),
        "email_domain": df["P_emaildomain"].map(email_group),
        "device_type": df["DeviceType"].fillna("unknown").astype(str),
        "has_identity": df["has_identity"].astype(bool),
    })
    # Pseudo-account key: the dataset has no true account identifier.
    out["uid"] = (df["card1"].astype("Int64").astype(str) + "_" + df["addr1"].astype("Int64").astype(str)
                  + "_" + df["P_emaildomain"].fillna("na").astype(str))
    return out


def stream_features(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorised fs-1.0 features; must equal fwcore.features on sorted, on-time data."""
    n = len(df)
    cols = {k: np.zeros(n) for k in ["prior_count_60s", "prior_count_10m", "prior_count_1h",
                                     "prior_amount_sum_1h", "small_count_10m", "prior_count_total"]}
    secs = np.full(n, np.nan)
    mean = np.full(n, np.nan)
    cap = F.MAX_ENTRIES
    for _, idx in df.groupby("uid", sort=False).indices.items():
        idx = np.sort(idx)
        ts = df["event_time"].to_numpy()[idx]
        amt = df["amount_inr"].to_numpy()[idx]
        m = len(idx)
        pos = np.arange(m)
        csum = np.concatenate([[0.0], np.cumsum(amt)])
        small = np.concatenate([[0], np.cumsum(amt < F.SMALL_AMOUNT_INR)])
        floor_cap = np.maximum(pos - cap, 0)
        for name, w in (("prior_count_60s", F.W_60S), ("prior_count_10m", F.W_10M), ("prior_count_1h", F.W_1H)):
            lo = np.maximum(np.searchsorted(ts, ts - w, side="left"), floor_cap)
            cols[name][idx] = pos - lo
            if name == "prior_count_1h":
                cols["prior_amount_sum_1h"][idx] = csum[pos] - csum[lo]
            if name == "prior_count_10m":
                cols["small_count_10m"][idx] = small[pos] - small[lo]
        cols["prior_count_total"][idx] = pos
        prev_max = np.concatenate([[np.nan], np.maximum.accumulate(ts)[:-1]])
        secs[idx] = np.maximum(0, ts - prev_max) / 1000.0
        with np.errstate(invalid="ignore", divide="ignore"):
            mean[idx] = np.where(pos > 0, csum[pos] / np.maximum(pos, 1), np.nan)

    feats = pd.DataFrame(cols)
    for c in ["prior_count_60s", "prior_count_10m", "prior_count_1h", "small_count_10m", "prior_count_total"]:
        feats[c] = feats[c].astype("int64")
    feats["secs_since_last"] = secs
    feats["prior_mean_amount"] = mean
    with np.errstate(invalid="ignore", divide="ignore"):
        feats["amount_ratio"] = np.where(feats["prior_count_total"] > 0, df["amount_inr"] / mean, np.nan)
    return feats


def to_model_inputs(df: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    x = pd.DataFrame(index=df.index)
    x["log_amount"] = np.log1p(df["amount_inr"])
    for c in ["prior_count_60s", "prior_count_10m", "prior_count_1h"]:
        x[c] = raw[c].astype(float)
    x["log_prior_amount_sum_1h"] = np.log1p(raw["prior_amount_sum_1h"])
    x["log_secs_since_last"] = np.log1p(raw["secs_since_last"])
    x["log_prior_count_total"] = np.log1p(raw["prior_count_total"])
    with np.errstate(invalid="ignore", divide="ignore"):
        x["log_amount_ratio"] = np.where(raw["amount_ratio"] > 0, np.log(raw["amount_ratio"]), np.nan)
    x["has_history"] = (raw["prior_count_total"] > 0).astype(float)
    for c in ["product_cd", "card_network", "card_type", "email_domain", "device_type"]:
        x[c] = df[c]
    x["has_identity"] = np.where(df["has_identity"], "yes", "no")
    return x[F.MODEL_FEATURES]


def parity_check(df: pd.DataFrame, x: pd.DataFrame, n_uids: int = 400, seed: int = 7) -> dict:
    """Recompute features with the reference state machine for sampled accounts."""
    rng = np.random.default_rng(seed)
    counts = df["uid"].value_counts()
    busy = counts[counts >= 20].index.to_numpy()
    sample = set(rng.choice(busy, size=min(n_uids, len(busy)), replace=False))
    sample |= set(counts.index[:5])  # include the busiest keys (exercises the entry cap)
    sub = df[df["uid"].isin(sample)]
    states: dict[str, F.AccountState] = {}
    max_abs = 0.0
    checked = 0
    for i, row in zip(sub.index, sub.itertuples(index=False)):
        st = states.setdefault(row.uid, F.AccountState())
        raw = F.compute_raw(st, int(row.event_time), float(row.amount_inr))
        ref = F.model_inputs(raw, row._asdict())
        for k in F.NUMERIC_FEATURES:
            a, b = ref[k], x.at[i, k]
            if a is None:
                if not (b is None or (isinstance(b, float) and np.isnan(b))):
                    raise AssertionError(f"parity {k} row {i}: ref None vs {b}")
                continue
            d = abs(a - b) / max(1.0, abs(a))
            max_abs = max(max_abs, d)
            if d > 1e-9:
                raise AssertionError(f"parity {k} row {i}: ref {a} vs vectorised {b}")
        F.update(st, int(row.event_time), float(row.amount_inr))
        checked += 1
    return {"uids": len(sample), "events": checked, "max_relative_diff": max_abs}


def main() -> None:
    t0 = time.time()
    OUT.mkdir(parents=True, exist_ok=True)
    df = load_events()
    raw = stream_features(df)
    x = to_model_inputs(df, raw)
    parity = parity_check(df, x)
    print("parity check passed:", parity)

    n = len(df)
    b1 = int(n * SPLIT_FRACTIONS[0])
    b2 = int(n * (SPLIT_FRACTIONS[0] + SPLIT_FRACTIONS[1]))
    split = np.array(["train"] * n, dtype=object)
    split[b1:b2] = "validation"
    split[b2:] = "test"

    table = pd.concat([df, raw.add_prefix("raw_"), x.add_prefix("x_")], axis=1)
    table["split"] = split
    table.to_parquet(OUT / "ieee_stream_features.parquet", index=False)

    manifest = {
        "dataset": "IEEE-CIS Fraud Detection (Kaggle competition ieee-fraud-detection, provided by Vesta Corporation)",
        "files": {p.name: {"bytes": p.stat().st_size, "sha256": sha256_prefix(p)} for p in
                  [RAW / "train_transaction.csv", RAW / "train_identity.csv"]},
        "feature_spec": FEATURE_SPEC_VERSION,
        "reference_inr_per_usd": REFERENCE_INR_PER_USD,
        "rows": n,
        "fraud_rate": float(df["label"].mean()),
        "uid_definition": "card1 + addr1 + P_emaildomain (pseudo-account; dataset has no account id)",
        "split": {
            "method": "chronological by TransactionDT (ties by TransactionID), no shuffling",
            "fractions": SPLIT_FRACTIONS,
            "boundaries_event_time_s": {
                "validation_start": int(df["event_time"].iloc[b1] // 1000),
                "test_start": int(df["event_time"].iloc[b2] // 1000),
            },
            "counts": {s: int((split == s).sum()) for s in ("train", "validation", "test")},
            "fraud_rate": {s: float(df["label"][split == s].mean()) for s in ("train", "validation", "test")},
        },
        "feature_history_note": "State features use each account's earlier transactions in any split as history only; labels are never used as features.",
        "parity_check": parity,
        "seconds": round(time.time() - t0, 1),
        "python": sys.version.split()[0],
    }
    (OUT / "prepare_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest["split"], indent=2))


if __name__ == "__main__":
    main()
