"""Build the bounded IEEE-CIS replay pack used as live-demo / Kafka / Flink / Spark input.

Source: the held-out TEST split of data/processed/ieee_stream_features.parquet (chronological
70/15/15 by TransactionDT). Records carry only the raw fs-1.1 input fields plus a stable source
reference; fraud labels are written to a SEPARATE file that only the evaluation path reads.

Outputs (row-level, derived from Kaggle competition data -> kept out of git, see .gitignore):
  artifacts/replay/ieee-test-replay-1.0/records.json     replay records (no labels)
  artifacts/replay/ieee-test-replay-1.0/selections.json  selected-example index lists (label-derived)
  artifacts/replay/ieee-test-replay-1.0/labels.json      {source_ref: isFraud} for the evaluation path
  artifacts/replay/ieee-test-replay-1.0/manifest.json    counts and definitions (no row-level data)

Usage:  uv run python training/build_replay_pack.py
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PACK_ID = "ieee-test-replay-1.0"
OUT = ROOT / "artifacts" / "replay" / PACK_ID
SEED = 20260917
CONTIGUOUS = 30_000        # first N test-split records in TransactionDT order
SELECTED_FRAUD = 300       # verified-fraud records (label isFraud=1) from the whole test split
SELECTED_ORDINARY = 300    # verified-ordinary records (isFraud=0) from the whole test split
BURST_ACCOUNTS = 40        # pseudo-accounts with >= BURST_MIN records in the test split
BURST_MIN, BURST_MAX = 6, 12

FIELDS = ["amount", "product_cd", "card_network", "card_type", "email_domain", "device_type", "has_identity"]


def record(row) -> dict:
    return {
        "source_ref": f"ieee-cis:{int(row.transaction_id)}",
        "source_dt_s": int(row.event_time // 1000),
        "account_id": f"uid:{row.uid}"[:64],
        **{f: (bool(getattr(row, f)) if f == "has_identity" else float(row.amount) if f == "amount" else str(getattr(row, f)))
           for f in FIELDS},
    }


def main() -> None:
    cols = ["transaction_id", "label", "event_time", "uid", "split"] + FIELDS
    t = pd.read_parquet(ROOT / "data" / "processed" / "ieee_stream_features.parquet", columns=cols)
    test = t[t["split"] == "test"].reset_index(drop=True)  # already in TransactionDT/TransactionID order
    rng = np.random.default_rng(SEED)

    idx: list[int] = list(range(min(CONTIGUOUS, len(test))))
    fraud_rows = np.flatnonzero(test["label"].to_numpy() == 1)
    legit_rows = np.flatnonzero(test["label"].to_numpy() == 0)
    sel_fraud = np.sort(rng.choice(fraud_rows, size=min(SELECTED_FRAUD, len(fraud_rows)), replace=False))
    sel_legit = np.sort(rng.choice(legit_rows, size=min(SELECTED_ORDINARY, len(legit_rows)), replace=False))
    counts = test.groupby("uid").size()
    # "na" parts mean card1/addr1/email missing: such keys merge unrelated cardholders, so exclude them.
    eligible = [u for u, c in counts.items() if c >= BURST_MIN and "_<NA>" not in u and not u.endswith("_na")]
    burst_uids = sorted(rng.choice(eligible, size=min(BURST_ACCOUNTS, len(eligible)), replace=False).tolist())
    uid_rows = test.groupby("uid").indices
    bursts = [sorted(uid_rows[u].tolist())[:BURST_MAX] for u in burst_uids]

    wanted = sorted(set(idx) | set(sel_fraud.tolist()) | set(sel_legit.tolist()) | {i for b in bursts for i in b})
    pos = {src: k for k, src in enumerate(wanted)}
    sub = test.iloc[wanted]
    records = [record(r) for r in sub.itertuples(index=False)]
    labels = {f"ieee-cis:{int(r.transaction_id)}": int(r.label) for r in sub.itertuples(index=False)}
    selections = {
        "contiguous": [pos[i] for i in idx],
        "selected_fraud": [pos[i] for i in sel_fraud.tolist()],
        "selected_ordinary": [pos[i] for i in sel_legit.tolist()],
        "pseudo_account_bursts": [[pos[i] for i in b] for b in bursts],
    }
    OUT.mkdir(parents=True, exist_ok=True)
    rec_bytes = json.dumps({"pack_id": PACK_ID, "records": records}, separators=(",", ":")).encode()
    (OUT / "records.json").write_bytes(rec_bytes)
    (OUT / "selections.json").write_text(json.dumps(selections, separators=(",", ":")))
    (OUT / "labels.json").write_text(json.dumps(labels, separators=(",", ":")))
    y = sub["label"].to_numpy()
    manifest = {
        "pack_id": PACK_ID,
        "dataset": "IEEE-CIS Fraud Detection, provided by Vesta Corporation through the Kaggle competition ieee-fraud-detection",
        "source_split": "held-out test split (chronological 70/15/15 by TransactionDT); never used for training or threshold selection",
        "test_split_rows": int(len(test)),
        "records": len(records),
        "records_sha256": hashlib.sha256(rec_bytes).hexdigest(),
        "selections": {"contiguous": len(selections["contiguous"]), "selected_fraud": len(selections["selected_fraud"]),
                       "selected_ordinary": len(selections["selected_ordinary"]),
                       "pseudo_account_bursts": len(bursts),
                       "pseudo_account_burst_records": sum(len(b) for b in bursts)},
        "contiguous_first_source_dt_s": int(test["event_time"].iloc[0] // 1000),
        "contiguous_last_source_dt_s": int(test["event_time"].iloc[len(idx) - 1] // 1000),
        "labelled_fraud_in_pack": int(y.sum()),
        "label_file": "labels.json (evaluation path only; never in Kafka events or scoring requests)",
        "selection_note": "selected_fraud / selected_ordinary are chosen with verified source labels; they are selected "
                          "examples, not representative performance estimates, and predictions are never forced.",
        "pseudo_account_note": "account_id = uid:<card1>_<addr1>_<P_emaildomain>, a derived key; the dataset has no "
                               "account identifier. Keys with missing parts are excluded from burst selections.",
        "fields": ["source_ref", "source_dt_s", "account_id"] + FIELDS,
        "seed": SEED,
        "terms": "Row-level data derived from Kaggle competition data: subject to the ieee-fraud-detection competition "
                 "rules; not committed to git and not bundled into frontends.",
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest["selections"] | {"records": len(records), "bytes": len(rec_bytes)}, indent=2))


if __name__ == "__main__":
    main()
