"""Explicit behavioural rules (rules-1.1).

Rules are evidence, not model attributions. They operate on raw stream features in
source amount units (IEEE-CIS TransactionAmt, no conversion) and are intentionally independent of how any label was created.
"""
from __future__ import annotations

from . import RULESET_VERSION

RULES = [
    {
        "rule_id": "R1_BURST",
        "title": "Rapid burst",
        "description": "4 or more earlier transactions from the same account within the previous 60 s of event time.",
    },
    {
        "rule_id": "R2_PROFILE_DEVIATION",
        "title": "Spend-profile deviation",
        "description": "Account has at least 5 earlier transactions and this amount is at least 8x its running mean and at least 60 (source amount units).",
    },
    {
        "rule_id": "R3_TEST_THEN_LARGE",
        "title": "Small test transactions then a larger one",
        "description": "3 or more earlier transactions under 2.5 (source amount units) within 10 min, followed by this transaction of at least 25 (source amount units).",
    },
]


def evaluate(raw: dict) -> list[dict]:
    amt = raw["amount"]
    ratio = raw["amount_ratio"]
    out = []
    out.append({
        "rule_id": "R1_BURST",
        "fired": raw["prior_count_60s"] >= 4,
        "evidence": f"{raw['prior_count_60s']} earlier transactions in 60 s",
    })
    fired2 = raw["prior_count_total"] >= 5 and ratio is not None and ratio >= 8 and amt >= 60
    out.append({
        "rule_id": "R2_PROFILE_DEVIATION",
        "fired": bool(fired2),
        "evidence": (
            "no spend history yet" if ratio is None
            else f"{ratio:.1f}x running mean over {raw['prior_count_total']} earlier transactions"
        ),
    })
    out.append({
        "rule_id": "R3_TEST_THEN_LARGE",
        "fired": raw["small_count_10m"] >= 3 and amt >= 25,
        "evidence": f"{raw['small_count_10m']} small transactions in 10 min before amount {amt:,.2f}",
    })
    return out


__all__ = ["RULES", "RULESET_VERSION", "evaluate"]
