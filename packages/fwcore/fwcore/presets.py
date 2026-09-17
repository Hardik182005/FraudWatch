"""Scenario presets offered by GET /api/v1/scenarios (contract: docs/contracts.md section 7.1).

A preset only supplies defaults for POST /runs (scenario, seed, rate, size, delivery options). ``expected``
describes the input conditions the generator creates; it never promises a model output. ``features.model``
lists only features the deployed model consumes; ``rules`` / ``rule_only_fields`` are used by the explicit
rules and are never model inputs.

``status``:
* ``supported``   - unaltered IEEE-CIS test-split records (only transport timing / redelivery changes)
* ``modified``    - "Modified IEEE-CIS demo scenario": record timing is changed, so window feature values
                    differ from the source; such events carry ``modified=true`` and are excluded from evaluation
* ``unsupported`` - the dataset cannot represent this behaviour faithfully; listed with the reason, not runnable
"""
from __future__ import annotations

from .features import CATEGORICAL_FEATURES, NUMERIC_FEATURES
from .replay import REPLAY_LABEL
from .rules import RULES

MODEL_FEATURES = list(NUMERIC_FEATURES) + list(CATEGORICAL_FEATURES)
RULE_ONLY_FIELDS = ["small_count_10m"]  # count of amounts < SMALL_AMOUNT in 10 min; used by R3 only
RULE_IDS = [r["rule_id"] for r in RULES]

_COMMON_CAVEATS = [
    REPLAY_LABEL,
    "event_time is replay time (run start + uniform spacing at the chosen rate); the source TransactionDT is kept "
    "separately in source_dt_s",
    "amount is IEEE-CIS TransactionAmt in source units (unit unspecified); no currency conversion",
    "account_id is a pseudo-account key (card1, addr1, email domain); the dataset has no account identifier",
]

PRESETS: list[dict] = [
    {
        "id": "ieee_replay", "status": "supported", "name": "IEEE-CIS replay",
        "description": "Held-out test-split records replayed in source order at the chosen rate.",
        "scenario": "replay", "seed": 20260917, "max_events": 600, "rate_eps": 10.0,
        "delivery": {"duplicate_every": None},
        "expected": ["Unaltered test-split records in TransactionDT order",
                     "Per-pseudo-account window features computed on replay time"],
        "evidence": ["Persisted events and latency percentiles", "Action mix (alert / review / allow)"],
    },
    {
        "id": "selected_examples", "status": "supported", "name": "Selected examples",
        "description": "Selected examples: records chosen with verified source labels (fraud and ordinary), shuffled. "
                       "Not a representative sample; predictions are never forced.",
        "scenario": "selected_examples", "seed": 20260917, "max_events": 300, "rate_eps": 5.0,
        "delivery": {"duplicate_every": None},
        "expected": ["About half of the records are labelled fraud in the source data",
                     "The score is whatever the model produces for each record"],
        "evidence": ["Per-event scores and rule evidence", "Label-based evaluation (evaluation path only)"],
    },
    {
        "id": "delayed_arrival", "status": "supported", "name": "Delayed arrival",
        "description": "Unaltered replay; 10% of records delivered 15-30 s late (beyond the 5 s watermark bound) and "
                       "10% out of order within the bound.",
        "scenario": "late_events", "seed": 20260917, "max_events": 600, "rate_eps": 10.0,
        "delivery": {"duplicate_every": None},
        "expected": ["About 10% of events arrive after the engine watermark and are marked late",
                     "Late events are scored but excluded from state and windows"],
        "evidence": ["Late counts and examples with watermark lag", "Window aggregates without late records"],
    },
    {
        "id": "replay_duplicates", "status": "supported", "name": "Duplicate delivery",
        "description": "Unaltered replay; every 10th record is produced twice (at-least-once redelivery).",
        "scenario": "duplicates", "seed": 20260917, "max_events": 600, "rate_eps": 10.0,
        "delivery": {"duplicate_every": 10},
        "expected": ["Some records reach Kafka twice with the same event_id",
                     "Unique persisted results stay equal to generated events"],
        "evidence": ["duplicates_sent vs. duplicate deliveries collapsed by the (engine, event_id) upsert"],
    },
    {
        "id": "pseudo_account_burst", "status": "modified", "name": "Pseudo-account burst (modified)",
        "description": "Modified IEEE-CIS demo scenario: every ~15 s one pseudo-account's real records are replayed a "
                       "few seconds apart, which changes their window features.",
        "scenario": "account_burst", "seed": 20260917, "max_events": 600, "rate_eps": 10.0,
        "delivery": {"duplicate_every": None},
        "expected": ["Bursts of 6-12 records on one pseudo-account within about a minute",
                     "Burst events carry modified=true"],
        "evidence": ["Window counts per pseudo-account", "R1_BURST rule evidence where it fires"],
        "extra_caveats": ["Modified events are excluded from label-based evaluation"],
    },
    {
        "id": "small_tests_then_large", "status": "unsupported", "name": "Small tests then large",
        "description": "Not offered: the dataset does not contain a verifiable card-testing sequence, and synthesising "
                       "one would present invented behaviour as source data.",
        "scenario": None, "seed": None, "max_events": None, "rate_eps": None,
        "delivery": {"duplicate_every": None}, "expected": [], "evidence": [],
    },
    {
        "id": "spending_deviation", "status": "unsupported", "name": "Spending deviation",
        "description": "Not offered: amounts are replayed unchanged; inflating amounts would fabricate records.",
        "scenario": None, "seed": None, "max_events": None, "rate_eps": None,
        "delivery": {"duplicate_every": None}, "expected": [], "evidence": [],
    },
]
PRESET_IDS = tuple(p["id"] for p in PRESETS)


def preset_out(p: dict, max_events_cap: int, max_rate_cap: float) -> dict:
    out = {k: v for k, v in p.items() if k != "extra_caveats"}
    if p["max_events"] is not None:
        out["max_events"] = min(p["max_events"], max_events_cap)
        out["rate_eps"] = min(p["rate_eps"], max_rate_cap)
    out["label"] = "Modified IEEE-CIS demo scenario" if p["status"] == "modified" else (
        "Selected examples" if p["id"] == "selected_examples" else
        "Unsupported" if p["status"] == "unsupported" else "IEEE-CIS replay")
    out["engines"] = ["flink"]
    out["features"] = {"model": MODEL_FEATURES, "rules": RULE_IDS, "rule_only_fields": RULE_ONLY_FIELDS}
    out["bounds"] = {"max_events": max_events_cap, "max_rate_eps": max_rate_cap}
    out["caveats"] = _COMMON_CAVEATS + p.get("extra_caveats", [])
    return out


def get_preset(preset_id: str) -> dict | None:
    return next((p for p in PRESETS if p["id"] == preset_id), None)
