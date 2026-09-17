"""Row -> contract (docs/contracts.md section 6) mappers. Kept in one place so REST and SSE agree."""
from __future__ import annotations

import base64
import json

from .core import ApiError, now_ms


def session_out(r: dict) -> dict:
    expired = r["expires_at"] <= now_ms() or r.get("replaced_by") is not None
    return {"id": r["id"], "label": r["label"], "created_at": r["created_at"], "expires_at": r["expires_at"],
            "status": "expired" if expired else "active", "event_cap": r["event_cap"], "events_used": r["events_used"],
            "rate_cap_eps": round(float(r["rate_cap_eps"]), 3), "active_run_id": r["active_run_id"]}


def run_out(r: dict) -> dict:
    return {k: r[k] for k in ("id", "session_id", "scenario", "seed", "engines", "status", "max_events", "events_sent",
                              "started_at", "ended_at", "replay_of", "control_log", "created_by", "error")} \
        | {"rate_eps": round(float(r["rate_eps"]), 3), "engines": list(r["engines"])}


def txn_summary(r: dict) -> dict:
    return {
        "event_id": r["event_id"], "engine": r["engine"], "session_id": r["session_id"], "run_id": r["run_id"],
        "seq": r["seq"], "account_id": r["account_id"], "event_time": r["event_time"], "amount_inr": r["amount_inr"],
        "merchant_name": r["merchant_name"], "merchant_category": r["merchant_category"], "city": r["city"],
        "action": r["action"], "score": r["score"], "late": r["late"], "pattern": r["pattern"],
        "injected": r["injected"], "rules_fired": list(r["rules_fired"]), "persisted_at": r["persisted_at"],
        "e2e_ms": r["e2e_ms"],
    }


def txn_detail(r: dict) -> dict:
    d = r["detail"] or {}
    scoring = d.get("scoring") or {}
    return {
        "engine": r["engine"], "engine_job_id": r["engine_job_id"], "kafka": d.get("kafka") or None,
        "late": r["late"], "watermark_ms": r["watermark_ms"], "raw_features": d.get("raw_features") or {},
        "model_inputs": d.get("model_inputs"), "model": d.get("model"), "rules": d.get("rules"),
        "policy": d.get("policy"),
        "scoring": {"status": scoring.get("status") or r["scoring_status"], "attempts": scoring.get("attempts") or 0,
                    "error": scoring.get("error")},
        "timing": {k: r[k] for k in ("produced_at", "kafka_ts", "engine_received_at", "scoring_started_at",
                                     "scoring_completed_at", "emitted_at", "persisted_at")},
        "duplicates": r["duplicates"],
        "engine_run_label": r["engine_run_label"],
        "spark": d.get("spark"),
    }


def trace(rows: list[dict]) -> list[dict]:
    """Per-event pipeline trace. Shared stages first, then each engine's own chain."""
    if not rows:
        return []
    first = rows[0]
    steps: list[dict] = []

    def add(stage, engine, at, prev, detail):
        if at is None:
            return prev
        steps.append({"stage": stage, "engine": engine, "at_ms": at,
                      "delta_ms": None if prev is None else at - prev, "detail": detail})
        return at

    base = add("generated", None, first["produced_at"], None, "generator produced the event (Kafka producer, acks=all)")
    kts = next((r["kafka_ts"] for r in rows if r["kafka_ts"] is not None), None)
    kafka = (first["detail"] or {}).get("kafka") or {}
    base = add("kafka_ingest", None, kts, base,
               f"broker LogAppendTime, partition {kafka.get('partition')} offset {kafka.get('offset')}" if kts else None)
    for r in rows:
        e = r["engine"]
        scoring = (r["detail"] or {}).get("scoring") or {}
        prev = add("engine_received", e, r["engine_received_at"], base,
                   "micro-batch start (Spark)" if e == "spark" else "keyed operator handled the record (Flink)")
        prev = add("scoring", e, r["scoring_completed_at"], prev,
                   {"status": scoring.get("status") or r["scoring_status"], "attempts": scoring.get("attempts"),
                    "started_at": r["scoring_started_at"],
                    "duration_ms": (r["scoring_completed_at"] - r["scoring_started_at"])
                    if r["scoring_completed_at"] and r["scoring_started_at"] else None})
        prev = add("engine_emitted", e, r["emitted_at"], prev, f"written to results topic ({e})")
        add("persisted", e, r["persisted_at"], prev,
            f"result consumer upsert (duplicates seen: {r['duplicates']})")
    return steps


def window_out(r: dict) -> dict:
    return {k: r[k] for k in ("engine", "session_id", "window_start", "window_end", "txn_count", "amount_sum_inr",
                              "alert_count", "review_count", "unscored_count", "emitted_at", "persisted_at")}


def alert_out(r: dict) -> dict:
    return {k: r[k] for k in ("event_id", "engine", "session_id", "run_id", "account_id", "amount_inr",
                              "merchant_name", "action", "score", "created_at", "status", "decision_note",
                              "updated_at")} | {"id": str(r["id"]), "rules_fired": list(r["rules_fired"])}


def audit_out(r: dict) -> dict:
    return {k: r[k] for k in ("at", "actor", "action", "entity_type", "entity_id", "from_status", "to_status", "note",
                              "prev_hash", "hash")} | {"id": str(r["id"])}


def experiment_out(r: dict) -> dict:
    return {k: r[k] for k in ("id", "kind", "status", "params", "created_at", "started_at", "finished_at",
                              "created_by", "results", "error", "environment", "progress")} \
        | {"run_ids": list(r["run_ids"])}


# -- keyset cursors ------------------------------------------------------------------------------
def encode_cursor(*parts) -> str:
    return base64.urlsafe_b64encode(json.dumps(parts, separators=(",", ":")).encode()).decode().rstrip("=")


def decode_cursor(cursor: str | None, n: int) -> list | None:
    if not cursor:
        return None
    try:
        parts = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        if not isinstance(parts, list) or len(parts) != n:
            raise ValueError
        return parts
    except Exception as exc:
        raise ApiError(422, "invalid_cursor", "cursor is malformed") from exc
