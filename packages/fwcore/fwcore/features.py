"""Reference implementation of the per-account stream features (feature spec fs-1.1).

Semantics (implemented identically in the Flink job, the Spark job and training):

* State is keyed by ``demo_session_id:account_id`` (``uid`` for IEEE-CIS training).
* Events are processed in arrival order per key. For training data arrival order
  is the dataset's TransactionDT order (ties broken by TransactionID).
* A window feature for an event at time ``t`` counts previously processed,
  non-late events with ``t - W <= ts <= t``. Out-of-order events that arrived
  earlier but carry a later timestamp are therefore excluded, deterministically.
* Late events (``event_time < current engine watermark``) are scored with a
  read-only view of state and never update it.
* Retained history is pruned to entries with ``ts >= max_ts_seen - 1 h`` and at
  most ``MAX_ENTRIES`` entries, so window counts are capped at ``MAX_ENTRIES``.
* ``secs_since_last`` uses the maximum event time processed so far for the key.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

W_60S = 60_000
W_10M = 600_000
W_1H = 3_600_000
SMALL_AMOUNT = 2.5  # source units; rule-only (small_count_10m), never a model input
MAX_ENTRIES = 500  # hard bound on retained entries per key

NUMERIC_FEATURES = [
    "log_amount",
    "prior_count_60s",
    "prior_count_10m",
    "prior_count_1h",
    "log_prior_amount_sum_1h",
    "log_secs_since_last",
    "log_prior_count_total",
    "log_amount_ratio",
    "has_history",
]
CATEGORICAL_FEATURES = ["product_cd", "card_network", "card_type", "email_domain", "device_type", "has_identity"]
MODEL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


@dataclass
class AccountState:
    entries: list[tuple[int, float]] = field(default_factory=list)  # (event_time_ms, amount in source units)
    total_count: int = 0
    amount_mean: float = 0.0
    max_ts: int = -1


def compute_raw(state: AccountState, event_time: int, amount: float) -> dict:
    """Raw (untransformed) state features for one event. Does not mutate state."""
    c60 = c10m = c1h = small10m = 0
    sum1h = 0.0
    for ts, amt in state.entries:
        if ts > event_time:
            continue
        age = event_time - ts
        if age <= W_1H:
            c1h += 1
            sum1h += amt
            if age <= W_10M:
                c10m += 1
                if amt < SMALL_AMOUNT:
                    small10m += 1
                if age <= W_60S:
                    c60 += 1
    # Seconds since the latest processed event time; 0 for an out-of-order event.
    secs_since_last = None if state.max_ts < 0 else max(0, event_time - state.max_ts) / 1000.0
    ratio = None
    if state.total_count >= 1 and state.amount_mean > 0:
        ratio = amount / state.amount_mean
    # No rounding here: engines must feed identical unrounded values to the model.
    return {
        "amount": float(amount),
        "prior_count_60s": c60,
        "prior_count_10m": c10m,
        "prior_count_1h": c1h,
        "prior_amount_sum_1h": sum1h,
        "small_count_10m": small10m,
        "secs_since_last": secs_since_last,
        "prior_count_total": state.total_count,
        "prior_mean_amount": state.amount_mean if state.total_count else None,
        "amount_ratio": ratio,
    }


def update(state: AccountState, event_time: int, amount: float) -> None:
    """Apply a non-late event to state."""
    state.entries.append((event_time, float(amount)))
    state.total_count += 1
    state.amount_mean += (float(amount) - state.amount_mean) / state.total_count
    if event_time > state.max_ts:
        state.max_ts = event_time
    cutoff = state.max_ts - W_1H
    if state.entries and (state.entries[0][0] < cutoff or len(state.entries) > MAX_ENTRIES):
        kept = [e for e in state.entries if e[0] >= cutoff]
        state.entries = kept[-MAX_ENTRIES:]


def model_inputs(raw: dict, event: dict) -> dict:
    """Transform raw state features + event attributes into model inputs.

    Missing values are None (NaN for the model); ``has_history`` makes missingness explicit.
    """
    def log1p(v):
        return None if v is None else math.log1p(max(float(v), 0.0))

    ratio = raw["amount_ratio"]
    return {
        "log_amount": log1p(raw["amount"]),
        "prior_count_60s": float(raw["prior_count_60s"]),
        "prior_count_10m": float(raw["prior_count_10m"]),
        "prior_count_1h": float(raw["prior_count_1h"]),
        "log_prior_amount_sum_1h": log1p(raw["prior_amount_sum_1h"]),
        "log_secs_since_last": log1p(raw["secs_since_last"]),
        "log_prior_count_total": log1p(raw["prior_count_total"]),
        "log_amount_ratio": None if ratio is None or ratio <= 0 else math.log(ratio),
        "has_history": 1.0 if raw["prior_count_total"] > 0 else 0.0,
        "product_cd": event["product_cd"],
        "card_network": event["card_network"],
        "card_type": event["card_type"],
        "email_domain": event["email_domain"],
        "device_type": event["device_type"],
        "has_identity": "yes" if event["has_identity"] else "no",
    }
