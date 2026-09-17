"""Deterministic parity fixture for the Flink and Spark jobs (test-only).

Writes two files to tests/streaming/fixtures/:

* ``parity_fixture.jsonl`` - an ordered script of steps the integration producer replays:
  ``{"op":"txn","phase":1,"late":false,"event":{...}}``, ``{"op":"heartbeat","event_time":...}``,
  ``{"op":"invalid","raw":"..."}``, ``{"op":"pause","ms":6000}``.
  Event times use a fixed base (``FIXTURE_BASE_MS``); the producer shifts them to "now".
* ``expected_features.jsonl`` - for every txn, per engine that it targets, the expected
  ``late`` flag and raw features computed with ``fwcore.features`` (compute_raw, then update
  only when not late), replaying events in produce order (= Kafka offset order per key).

Coverage: many accounts across two demo sessions, bursts, card-testing small amounts, a busy key
with more than 500 entries inside one hour (MAX_ENTRIES cap), a >1 h event-time jump (time-based
prune), out-of-order events within the 5 s bound (at most 3 s behind the global maximum, so they
are on time in both engines), clearly late events (1-20 min behind, produced after a pause with
heartbeats so both engines' watermarks have advanced), engine-specific target_engines, invalid
records, and closing events that let both engines finalize windows.

Usage: python tests/streaming/make_fixture.py
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "packages" / "fwcore"))

from fwcore.features import AccountState, compute_raw, update  # noqa: E402
from fwcore.schema import make_event_id, validate_event  # noqa: E402

SEED = 20260917
FIXTURE_BASE_MS = 1_760_000_000_000
RUN_ID = "parity-fixture"
SESSIONS = ["ps-fixture-A", "ps-fixture-B"]
OUT = Path(__file__).resolve().parent / "fixtures"

PRODUCTS = ["W", "C", "R", "H", "S"]
NETWORKS = ["visa", "mastercard", "american express", "discover", "unknown"]
CTYPES = ["debit", "credit", "other", "unknown"]
DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "anonymous.com", "aol.com", "other", "missing"]
DEVICES = ["desktop", "mobile", "unknown"]


def main() -> None:
    rng = random.Random(SEED)
    steps: list[dict] = []
    seq = 0
    clock = 0  # ms relative to base; the global maximum event time produced so far

    accounts = [(SESSIONS[i % 2], f"acct-{i:03d}") for i in range(24)]
    busy = (SESSIONS[0], "acct-busy")
    tester = (SESSIONS[1], "acct-cardtest")

    def targets() -> list[str]:
        r = rng.random()
        if r < 0.85:
            return ["flink", "spark"]
        return ["flink"] if r < 0.925 else ["spark"]

    def amount(small: bool = False):
        if small:
            return round(rng.uniform(0.25, 2.49), 2)
        r = rng.random()
        if r < 0.05:
            return rng.choice([5, 10, 25])  # JSON integers are valid amounts
        if r < 0.07:
            return rng.choice([0.01, 2.49, 2.5, 999_999.99])
        return round(rng.lognormvariate(4.2, 1.1), 2)

    def txn(key, event_time: int, phase: int, late: bool, small: bool = False, tg=None, pattern="normal"):
        nonlocal seq
        sess, acct = key
        ev = {
            "schema_version": "1.1", "kind": "txn",
            "event_id": make_event_id(RUN_ID, seq), "demo_session_id": sess, "run_id": RUN_ID, "seq": seq,
            "account_id": acct, "event_time": FIXTURE_BASE_MS + event_time, "produced_at": FIXTURE_BASE_MS + event_time,
            "amount": amount(small), "product_cd": rng.choice(PRODUCTS), "card_network": rng.choice(NETWORKS),
            "card_type": rng.choice(CTYPES), "email_domain": rng.choice(DOMAINS), "device_type": rng.choice(DEVICES),
            "has_identity": rng.random() < 0.3, "target_engines": tg or targets(),
            "source_ref": f"fixture:{seq}", "source_dt_s": event_time // 1000, "modified": True,
            "scenario": "replay", "pattern": pattern,
            "injected": pattern != "normal",
        }
        validate_event(ev)
        seq += 1
        steps.append({"op": "txn", "phase": phase, "late": late, "event": ev})

    def heartbeat():
        steps.append({"op": "heartbeat", "event_time": FIXTURE_BASE_MS + clock})

    def pause(ms: int):
        steps.append({"op": "pause", "ms": ms})

    # ---- phase 1: on-time traffic, bursts, busy key, out-of-order within bound, invalid ----
    for i in range(1500):
        if i == 800:
            clock += 65 * 60_000  # > 1 h jump: exercises time-based pruning on every key
            heartbeat()
        clock += rng.randint(50, 1500)
        r = rng.random()
        busy_share = 0.72 if i < 800 else 0.30
        if r < busy_share:
            key = busy
        elif r < busy_share + 0.08:
            key = tester
        else:
            key = rng.choice(accounts)
        if rng.random() < 0.08 and i > 5:
            et = clock - rng.randint(500, 3000)  # out of order, within the bound
            pat = "out_of_order"
        else:
            et = clock
            pat = "normal"
        txn(key, et, 1, False, small=(key == tester and rng.random() < 0.8), pattern=pat)
        if i % 25 == 24:
            # burst: 6 rapid events on one account within ~1 s
            bkey = rng.choice(accounts)
            for _ in range(6):
                clock += rng.randint(20, 200)
                txn(bkey, clock, 1, False, pattern="burst")
        if i % 40 == 39:
            heartbeat()
        if i in (100, 700, 1200):
            steps.append({"op": "invalid", "raw": "{not json"})
            steps.append({"op": "invalid", "raw": json.dumps({"schema_version": "1.1", "kind": "txn", "event_id": "x"})})
            steps.append({"op": "invalid", "raw": json.dumps({"schema_version": "1.1", "kind": "heartbeat", "event_time": 1.5, "produced_at": 1})})
    heartbeat()
    pause(8000)

    # ---- phase 2: clearly late events (1-20 minutes behind the watermark) ----
    late_keys = [busy, tester] + rng.sample(accounts, 10)
    for j in range(40):
        key = late_keys[j % len(late_keys)]
        txn(key, clock - rng.randint(60_000, 20 * 60_000), 2, True, pattern="late_burst")
    heartbeat()
    pause(4000)

    # ---- phase 3: on-time traffic again, then closers that finalize windows ----
    for i in range(300):
        clock += rng.randint(50, 1500)
        key = busy if rng.random() < 0.3 else rng.choice(accounts)
        txn(key, clock, 3, False)
        if i % 40 == 39:
            heartbeat()
    clock += 3 * 60_000
    for sess in SESSIONS:
        txn((sess, "acct-closer"), clock, 3, False, tg=["flink", "spark"], pattern="closer")
    heartbeat()

    # ---- expected features per engine ----
    expected = []
    states = {"flink": {}, "spark": {}}
    for st in steps:
        if st["op"] != "txn":
            continue
        ev = st["event"]
        rec = {"seq": ev["seq"], "state_key": f"{ev['demo_session_id']}:{ev['account_id']}", "engines": {}}
        for eng in ("flink", "spark"):
            if eng not in ev["target_engines"]:
                continue
            state = states[eng].setdefault(rec["state_key"], AccountState())
            raw = compute_raw(state, ev["event_time"], float(ev["amount"]))
            if not st["late"]:
                update(state, ev["event_time"], float(ev["amount"]))
            rec["engines"][eng] = {"late": st["late"], "raw": raw}
        expected.append(rec)

    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "parity_fixture.jsonl", "w", encoding="utf-8", newline="\n") as f:
        for st in steps:
            f.write(json.dumps(st, separators=(",", ":")) + "\n")
    with open(OUT / "expected_features.jsonl", "w", encoding="utf-8", newline="\n") as f:
        for rec in expected:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")

    n_txn = sum(1 for s in steps if s["op"] == "txn")
    busy_max = max((r["engines"].get("flink", {}).get("raw", {}).get("prior_count_1h", 0) for r in expected), default=0)
    print(f"steps={len(steps)} txns={n_txn} late={sum(1 for s in steps if s['op']=='txn' and s['late'])} "
          f"invalid={sum(1 for s in steps if s['op']=='invalid')} max_prior_count_1h(flink)={busy_max}")


if __name__ == "__main__":
    main()
