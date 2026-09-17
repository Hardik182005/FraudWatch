"""Smoke test: IEEE-CIS replay stream -> reference feature state -> scoring service.

Runs the fwcore reference semantics (per-record watermark, late = read-only) over a
virtual-time IEEE-CIS replay stream and posts batches to a running scoring service.
Usage: python tests/integration/smoke_scoring.py [http://localhost:18001] [scenario] [n]
"""
from __future__ import annotations

import collections
import json
import sys
import urllib.request
from pathlib import Path

from fwcore import SCHEMA_VERSION
from fwcore import features as F
from fwcore.schema import make_event_id, validate_event
from fwcore.replay import ScenarioConfig, ScenarioStream

ROOT = Path(__file__).resolve().parents[2]
PACK_DIR = ROOT / "ml/artifacts/replay/ieee-test-replay-1.0"
PACK = {"records": json.loads((PACK_DIR / "records.json").read_text())["records"],
        "selections": json.loads((PACK_DIR / "selections.json").read_text())}
BOUND_MS = 5_000


def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:18001"
    scenario = sys.argv[2] if len(sys.argv) > 2 else "late_events"
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 1500
    stream = ScenarioStream(ScenarioConfig.for_scenario(scenario, 20260917, 5.0), PACK, "smoke")
    t0, run_id = 1_789_000_000_000, "run_smoke"
    states: dict[str, F.AccountState] = collections.defaultdict(F.AccountState)
    max_ts, items = None, []
    for seq in range(n):
        p = stream.next()
        event = {"schema_version": SCHEMA_VERSION, "kind": "txn", "event_id": make_event_id(run_id, seq),
                 "demo_session_id": "smoke", "run_id": run_id, "seq": seq,
                 "event_time": t0 + int(p.event_offset_ms), "produced_at": t0 + int(p.arrival_offset_ms),
                 "target_engines": ["flink"], **p.body}
        event = {k: v for k, v in event.items() if v is not None}
        validate_event(event)
        wm = None if max_ts is None else max_ts - BOUND_MS
        late = wm is not None and event["event_time"] < wm
        st = states[f"smoke:{event['account_id']}"]
        raw = F.compute_raw(st, event["event_time"], event["amount"])
        if not late:
            F.update(st, event["event_time"], event["amount"])
        max_ts = event["event_time"] if max_ts is None else max(max_ts, event["event_time"])
        items.append({"event": event, "raw": raw, "late": late, "watermark_ms": wm})

    results = []
    for i in range(0, len(items), 200):
        body = json.dumps({"engine": "flink", "items": items[i:i + 200]}).encode()
        req = urllib.request.Request(url + "/v1/score", body, {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            results += json.load(r)["items"]

    by_pattern = collections.defaultdict(collections.Counter)
    rules = collections.Counter()
    for it, res in zip(items, results):
        assert it["event"]["event_id"] == res["event_id"]
        by_pattern[it["event"].get("pattern") or "base"][res["policy"]["action"]] += 1
        rules.update(r["rule_id"] for r in res["rules"]["results"] if r["fired"])
    print("scenario", scenario, "events", len(results), "late", sum(i["late"] for i in items))
    for pat, c in sorted(by_pattern.items()):
        print(f"  {pat:22s} {dict(c)}")
    print("  rules fired", dict(rules))
    print("  sample", json.dumps(results[-1]["model"]["explanation"])[:300])


if __name__ == "__main__":
    main()
