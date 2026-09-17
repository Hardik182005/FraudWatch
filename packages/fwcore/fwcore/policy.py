"""Final review policy (policy-1.0). Kept separate from model and rule evidence.

Decisions are review routing for an educational demo. ``alert`` means "flagged for
priority review", never "confirmed fraud" and never "payment blocked".
"""
from __future__ import annotations

from . import POLICY_VERSION


def decide(score: float | None, alert_threshold: float, review_threshold: float, rules: list[dict], late: bool) -> dict:
    fired = [r["rule_id"] for r in rules if r["fired"]]
    reasons: list[str] = []
    if score is None:
        # Scoring unavailable: never silently allow a rule hit, never invent a score.
        action = "review" if fired else "unscored"
        reasons.append("model score unavailable")
    elif score >= alert_threshold:
        action = "alert"
        reasons.append(f"model score {score:.3f} >= alert threshold {alert_threshold:.3f}")
    elif len(fired) >= 2:
        action = "alert"
    elif score >= review_threshold:
        action = "review"
        reasons.append(f"model score {score:.3f} >= review threshold {review_threshold:.3f}")
    elif fired:
        action = "review"
    else:
        action = "allow"
    for rid in fired:
        reasons.append(f"rule {rid} fired")
    if late:
        reasons.append("late event: scored without updating account state")
    return {"action": action, "policy_version": POLICY_VERSION, "reasons": reasons}
