"""Transaction event contract (schema 1.1: replayed IEEE-CIS records, amounts in source units) with dependency-free validation.

Times are integer epoch milliseconds so that Java, Python and SQL agree exactly.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from . import SCHEMA_VERSION

EVENT_NAMESPACE = uuid.UUID("6f1c5a7e-3b0d-4f7a-9d5e-2a8c1b4e9f10")

PRODUCT_CODES = ("W", "C", "R", "H", "S")
CARD_NETWORKS = ("visa", "mastercard", "american express", "discover", "unknown")
CARD_TYPES = ("debit", "credit", "other", "unknown")
EMAIL_DOMAINS = ("gmail.com", "yahoo.com", "hotmail.com", "anonymous.com", "aol.com", "other", "missing")
DEVICE_TYPES = ("desktop", "mobile", "unknown")
ENGINES = ("flink", "spark")
SCENARIOS = ("replay", "selected_examples", "late_events", "duplicates", "account_burst")

MAX_AMOUNT = 1_000_000.0  # source units; IEEE-CIS TransactionAmt max is ~31,937


class EventValidationError(ValueError):
    pass


def make_event_id(run_id: str, seq: int) -> str:
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{run_id}:{seq}"))


def state_key(event: dict[str, Any]) -> str:
    """Per-account state is isolated per demo session."""
    return f"{event['demo_session_id']}:{event['account_id']}"


_REQUIRED: dict[str, type | tuple[type, ...]] = {
    "schema_version": str,
    "kind": str,
    "event_id": str,
    "demo_session_id": str,
    "run_id": str,
    "seq": int,
    "account_id": str,
    "event_time": int,
    "produced_at": int,
    "amount": (int, float),
    "product_cd": str,
    "card_network": str,
    "card_type": str,
    "email_domain": str,
    "device_type": str,
    "has_identity": bool,
    "target_engines": list,
}


def validate_event(event: Any) -> dict[str, Any]:
    """Validate a decoded transaction event. Raises EventValidationError."""
    if not isinstance(event, dict):
        raise EventValidationError("event must be a JSON object")
    if event.get("kind") == "heartbeat":
        for f in ("event_time", "produced_at"):
            if not isinstance(event.get(f), int) or isinstance(event.get(f), bool):
                raise EventValidationError(f"heartbeat.{f} must be an integer epoch ms")
        return event
    for field, typ in _REQUIRED.items():
        if field not in event:
            raise EventValidationError(f"missing field: {field}")
        value = event[field]
        if typ is int and isinstance(value, bool):
            raise EventValidationError(f"{field} must be an integer")
        if not isinstance(value, typ):
            raise EventValidationError(f"{field} has wrong type")
    if event["schema_version"] != SCHEMA_VERSION:
        raise EventValidationError("unsupported schema_version")
    if event["kind"] != "txn":
        raise EventValidationError("kind must be txn or heartbeat")
    amount = float(event["amount"])
    if not (0 < amount <= MAX_AMOUNT):
        raise EventValidationError("amount out of range")
    if event["product_cd"] not in PRODUCT_CODES:
        raise EventValidationError("invalid product_cd")
    if event["card_network"] not in CARD_NETWORKS:
        raise EventValidationError("invalid card_network")
    if event["card_type"] not in CARD_TYPES:
        raise EventValidationError("invalid card_type")
    if event["email_domain"] not in EMAIL_DOMAINS:
        raise EventValidationError("invalid email_domain")
    if event["device_type"] not in DEVICE_TYPES:
        raise EventValidationError("invalid device_type")
    if not event["target_engines"] or any(e not in ENGINES for e in event["target_engines"]):
        raise EventValidationError("invalid target_engines")
    if len(event["account_id"]) > 64 or len(event["demo_session_id"]) > 64:
        raise EventValidationError("identifier too long")
    return event


def decode_event(raw: bytes | str) -> dict[str, Any]:
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise EventValidationError(f"invalid JSON: {exc}") from exc
    return validate_event(data)


JSON_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://fraudwatch.example/schemas/transaction-event-1.1.json",
    "title": "FraudWatch TransactionEvent 1.1",
    "type": "object",
    "required": list(_REQUIRED.keys()),
    "properties": {
        "schema_version": {"const": SCHEMA_VERSION},
        "kind": {"enum": ["txn", "heartbeat"]},
        "event_id": {"type": "string", "description": "uuid5(run_id:seq); stable across duplicate deliveries"},
        "demo_session_id": {"type": "string", "maxLength": 64},
        "run_id": {"type": "string"},
        "seq": {"type": "integer", "minimum": 0},
        "account_id": {"type": "string", "maxLength": 64, "description": "pseudo-account uid:<card1>_<addr1>_<P_emaildomain> derived from the dataset (not a real account id)"},
        "event_time": {"type": "integer", "description": "epoch ms when the transaction occurred (event time)"},
        "produced_at": {"type": "integer", "description": "epoch ms immediately before the Kafka produce call (ingestion boundary)"},
        "amount": {"type": "number", "exclusiveMinimum": 0, "maximum": MAX_AMOUNT,
                   "description": "IEEE-CIS TransactionAmt as provided (unit unspecified in the files); no conversion"},
        "product_cd": {"enum": list(PRODUCT_CODES)},
        "card_network": {"enum": list(CARD_NETWORKS)},
        "card_type": {"enum": list(CARD_TYPES)},
        "email_domain": {"enum": list(EMAIL_DOMAINS)},
        "device_type": {"enum": list(DEVICE_TYPES)},
        "has_identity": {"type": "boolean"},
        "source_ref": {"type": "string", "description": "stable source reference, ieee-cis:<TransactionID>"},
        "source_dt_s": {"type": "integer", "description": "source TransactionDT (seconds from an unspecified reference time)"},
        "scenario": {"enum": list(SCENARIOS)},
        "pattern": {"type": ["string", "null"], "description": "delivery/selection tag: late_delivery | out_of_order | duplicate_delivery | selected_fraud_example | pseudo_account_burst"},
        "injected": {"type": "boolean"},
        "modified": {"type": "boolean", "description": "true = Modified IEEE-CIS demo scenario (timing changes feature values); excluded from evaluation"},
        "arrival_delay_ms": {"type": "integer", "minimum": 0},
        "target_engines": {"type": "array", "items": {"enum": list(ENGINES)}, "minItems": 1},
    },
}
