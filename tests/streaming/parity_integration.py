"""End-to-end parity test: replays the deterministic fixture into Kafka and compares Flink vs Spark output.

Runs inside the fraudwatch/spark image on the test Compose network (it has confluent-kafka, requests
and fwcore). Invoked by tests/streaming/run_integration.sh; see that file for the full sequence.

Subcommands
  parity   wait for both engines, send a canary, replay parity_fixture.jsonl (heartbeats to every
           partition, pauses honoured), collect results/windows/DLQ/metrics, compare against
           expected_features.jsonl and across engines, write out/parity_report.{json,md}
  failure  (scoring stopped by the caller) produce a few txns and check both engines emit
           scoring.status="failed" with attempts=3 instead of dropping records
  collect-restore <run_id>  check a run's records after an engine restart (duplicates, gaps)

All numbers in the report come from the Kafka topics; nothing is hardcoded.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path

import requests
from confluent_kafka import Consumer, Producer, TopicPartition

from fwcore.schema import make_event_id, validate_event

HERE = Path(__file__).resolve().parent
FIX = HERE / "fixtures"
OUT = HERE / "out"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FLINK_REST = os.getenv("FLINK_REST", "http://flink-jobmanager:8081")
FIXTURE_BASE_MS = 1_760_000_000_000
PARTITIONS = 4
TOPICS = {
    "txn": "fraudwatch.transactions",
    "flink": "fraudwatch.results.flink",
    "spark": "fraudwatch.results.spark",
    "wflink": "fraudwatch.windows.flink",
    "wspark": "fraudwatch.windows.spark",
    "dlq": "fraudwatch.dlq",
    "metrics": "fraudwatch.metrics.spark",
}


def now_ms() -> int:
    return int(time.time() * 1000)


def log(msg: str) -> None:
    print(f"[parity {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True, "linger.ms": 2})


def read_topic(topic: str, from_ts: int | None = None) -> list[dict]:
    """Read a whole topic (or from a timestamp) up to its current end. Returns decoded records."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"parity-reader-{uuid.uuid4()}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    md = c.list_topics(topic, timeout=10)
    parts = sorted(md.topics[topic].partitions)
    tps = [TopicPartition(topic, p, from_ts if from_ts is not None else 0) for p in parts]
    if from_ts is not None:
        tps = c.offsets_for_times(tps, timeout=10)
    ends = {}
    for p in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        ends[p] = hi
    start = {tp.partition: (tp.offset if tp.offset >= 0 else ends[tp.partition]) for tp in tps}
    todo = {p for p in parts if start[p] < ends[p]}
    c.assign([TopicPartition(topic, p, start[p]) for p in parts])
    out = []
    deadline = time.time() + 120
    while todo and time.time() < deadline:
        for m in c.consume(1000, timeout=1.0):
            if m.error():
                continue
            if m.offset() < ends[m.partition()]:
                val = m.value()
                try:
                    rec = json.loads(val) if val is not None else None
                except ValueError:
                    rec = {"_undecodable": val[:200].decode("utf-8", "replace")}
                out.append({"partition": m.partition(), "offset": m.offset(), "timestamp": m.timestamp()[1],
                            "key": m.key().decode() if m.key() else None, "value": rec})
            if m.offset() >= ends[m.partition()] - 1:
                todo.discard(m.partition())
    c.close()
    return out


# ------------------------------------------------------------------------------------------------
def flink_job() -> dict | None:
    try:
        jobs = requests.get(f"{FLINK_REST}/jobs/overview", timeout=30).json()["jobs"]
    except Exception:
        return None
    running = [j for j in jobs if j["name"] == "fraudwatch-flink" and j["state"] == "RUNNING"]
    return running[0] if running else None


def flink_all_tasks_running(jid: str) -> bool:
    try:
        d = requests.get(f"{FLINK_REST}/jobs/{jid}", timeout=30).json()
    except Exception as exc:  # noqa: BLE001 - transient REST slowness while the machine is busy
        log(f"Flink REST not ready: {exc}")
        return False
    return all(v["status"] == "RUNNING" for v in d["vertices"])


def flink_metrics(jid: str) -> dict:
    names = ["events_valid", "events_invalid", "events_late", "events_featurized", "heartbeats",
             "events_not_targeted", "scoring_retries", "scoring_failures", "scoring_ok", "window_late_records"]
    totals: dict[str, float] = defaultdict(float)
    d = requests.get(f"{FLINK_REST}/jobs/{jid}", timeout=30).json()
    for v in d["vertices"]:
        ids = [m["id"] for m in requests.get(f"{FLINK_REST}/jobs/{jid}/vertices/{v['id']}/metrics", timeout=30).json()]
        wanted = [i for i in ids if i.split(".")[-1] in names]
        if not wanted:
            continue
        vals = requests.get(f"{FLINK_REST}/jobs/{jid}/vertices/{v['id']}/metrics",
                            params={"get": ",".join(wanted)}, timeout=30).json()
        for m in vals:
            totals[m["id"].split(".")[-1]] += float(m["value"])
    return {k: int(v) for k, v in sorted(totals.items())}


def flink_checkpoints(jid: str) -> dict:
    d = requests.get(f"{FLINK_REST}/jobs/{jid}/checkpoints", timeout=30).json()
    c = d.get("counts", {})
    latest = (d.get("latest") or {}).get("completed") or {}
    return {"completed": c.get("completed"), "failed": c.get("failed"), "restored": c.get("restored"),
            "latest_completed_id": latest.get("id"), "latest_external_path": latest.get("external_path")}


def spark_main_progress(since_ms: int) -> list[dict]:
    recs = read_topic(TOPICS["metrics"], from_ts=since_ms)
    return [r["value"] for r in recs if isinstance(r["value"], dict)]


# ------------------------------------------------------------------------------------------------
def make_txn(run_id: str, seq: int, session: str, account: str, et: int, amount: float, targets: list[str]) -> dict:
    ev = {"schema_version": "1.1", "kind": "txn", "event_id": make_event_id(run_id, seq), "demo_session_id": session,
          "run_id": run_id, "seq": seq, "account_id": account, "event_time": et, "produced_at": now_ms(),
          "amount": amount, "product_cd": "W", "card_network": "visa", "card_type": "debit",
          "email_domain": "gmail.com", "device_type": "mobile", "has_identity": False, "target_engines": targets,
          "scenario": "replay", "pattern": "normal", "injected": False}
    return validate_event(ev)


def send_txn(p: Producer, ev: dict) -> None:
    p.produce(TOPICS["txn"], key=f"{ev['demo_session_id']}:{ev['account_id']}".encode(),
              value=json.dumps(ev, separators=(",", ":")).encode())


def send_heartbeat(p: Producer, et: int) -> None:
    hb = json.dumps({"schema_version": "1.1", "kind": "heartbeat", "event_time": et, "produced_at": now_ms(),
                     "generator_id": "parity-test"}, separators=(",", ":")).encode()
    for part in range(PARTITIONS):
        p.produce(TOPICS["txn"], value=hb, partition=part)


def wait_for_results(run_id: str, want: dict[str, int], since_ms: int, timeout_s: int) -> dict[str, list[dict]]:
    deadline = time.time() + timeout_s
    got: dict[str, list[dict]] = {}
    while True:
        got = {}
        for eng in want:
            recs = read_topic(TOPICS[eng], from_ts=since_ms)
            got[eng] = [r for r in recs if isinstance(r["value"], dict)
                        and r["value"].get("event", {}).get("run_id") == run_id]
        counts = {e: len({r["value"]["event"]["event_id"] for r in got[e]}) for e in want}
        log(f"waiting for results of {run_id}: {counts} / {want}")
        if all(counts[e] >= want[e] for e in want) or time.time() > deadline:
            return got
        time.sleep(5)


def wait_engines_ready(timeout_s: int = 600) -> dict:
    t0 = time.time()
    jid = None
    while time.time() - t0 < timeout_s:
        j = flink_job()
        if j and flink_all_tasks_running(j["jid"]):
            jid = j["jid"]
            break
        log("waiting for Flink job RUNNING")
        time.sleep(5)
    if not jid:
        raise SystemExit("Flink job not running")
    log(f"Flink job RUNNING: {jid}")
    # canary proves both engines consume from the current end of the topic
    since = now_ms() - 1000
    nonce = uuid.uuid4().hex[:8]
    run_id = f"canary-{nonce}"
    p = producer()
    deadline = time.time() + timeout_s
    seq = 0
    while True:
        et = now_ms()
        send_heartbeat(p, et)
        send_txn(p, make_txn(run_id, seq, f"canary-{nonce}", "acct-canary", et, 123.45, ["flink", "spark"]))
        p.flush(10)
        seq += 1
        got = wait_for_results(run_id, {"flink": 1, "spark": 1}, since, 45)
        if got["flink"] and got["spark"]:
            spark_app = got["spark"][0]["value"]["engine_job_id"]
            log(f"canary seen by both engines after {seq} attempt(s); spark app {spark_app}")
            return {"flink_job_id": jid, "spark_app_id": spark_app, "canary_run_id": run_id, "canary_attempts": seq}
        if time.time() > deadline:
            raise SystemExit(f"engines not ready: flink={len(got['flink'])} spark={len(got['spark'])}")


# ------------------------------------------------------------------------------------------------
def replay_fixture(nonce: str, rate: float) -> dict:
    steps = [json.loads(l) for l in (FIX / "parity_fixture.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    run_id = f"parity-{nonce}"
    span = max(st.get("event", st).get("event_time", 0) for st in steps) - FIXTURE_BASE_MS
    base = now_ms() - span - 10_000  # whole fixture lies in the recent past; raw features are shift-invariant
    shift = base - FIXTURE_BASE_MS
    invalid_raws: list[str] = []
    p = producer()
    sent = Counter()
    interval = 1.0 / rate
    t_next = time.time()
    for st in steps:
        op = st["op"]
        if op == "pause":
            p.flush(10)
            time.sleep(st["ms"] / 1000.0)
            t_next = time.time()
            continue
        if op == "txn":
            ev = dict(st["event"])
            ev["run_id"] = run_id
            ev["event_id"] = make_event_id(run_id, ev["seq"])
            ev["demo_session_id"] = f"{ev['demo_session_id']}-{nonce}"
            ev["event_time"] += shift
            ev["produced_at"] = now_ms()
            validate_event(ev)
            send_txn(p, ev)
        elif op == "heartbeat":
            send_heartbeat(p, st["event_time"] + shift)
        elif op == "invalid":
            raw = st["raw"].replace('"x"', f'"x-{nonce}"')
            invalid_raws.append(raw)
            p.produce(TOPICS["txn"], value=raw.encode("utf-8"), partition=sent["invalid"] % PARTITIONS)
        sent[op] += 1
        p.poll(0)
        t_next += interval
        delay = t_next - time.time()
        if delay > 0:
            time.sleep(delay)
    p.flush(30)
    return {"run_id": run_id, "shift_ms": shift, "sent": dict(sent), "invalid_raws": invalid_raws}


def compare(nonce: str, run_id: str, since_ms: int, results: dict[str, list[dict]], invalid_raws: list[str]) -> dict:
    expected = {}
    for line in (FIX / "expected_features.jsonl").read_text(encoding="utf-8").splitlines():
        if line.strip():
            r = json.loads(line)
            expected[make_event_id(run_id, r["seq"])] = r
    report: dict = {"engines": {}, "cross": {}}
    by_engine: dict[str, dict[str, dict]] = {}
    for eng in ("flink", "spark"):
        recs = results[eng]
        ids = Counter(r["value"]["event"]["event_id"] for r in recs)
        dedup: dict[str, dict] = {}
        for r in recs:
            dedup.setdefault(r["value"]["event"]["event_id"], r["value"])
        by_engine[eng] = dedup
        exp_ids = {eid for eid, e in expected.items() if eng in e["engines"]}
        missing = sorted(exp_ids - dedup.keys())
        unexpected = sorted(dedup.keys() - exp_ids)
        late_mismatch, raw_mismatch, examples = [], [], []
        late_true = 0
        statuses = Counter()
        attempts = Counter()
        lat_score, lat_e2e = [], []
        wm_null = 0
        for eid, v in dedup.items():
            if eid not in expected or eng not in expected[eid]["engines"]:
                continue
            exp = expected[eid]["engines"][eng]
            late_true += bool(v["late"])
            if v["late"] != exp["late"]:
                late_mismatch.append(eid)
                if len(examples) < 5:
                    examples.append({"event_id": eid, "seq": v["event"]["seq"], "field": "late", "expected": exp["late"],
                                     "actual": v["late"], "watermark_ms": v["watermark_ms"], "event_time": v["event"]["event_time"]})
            if v["raw_features"] != exp["raw"]:
                raw_mismatch.append(eid)
                if len(examples) < 5:
                    diff = {k: (exp["raw"][k], v["raw_features"].get(k)) for k in exp["raw"]
                            if exp["raw"][k] != v["raw_features"].get(k)}
                    examples.append({"event_id": eid, "seq": v["event"]["seq"], "late": v["late"], "diff": diff})
            statuses[v["scoring"]["status"]] += 1
            attempts[v["scoring"]["attempts"]] += 1
            wm_null += v["watermark_ms"] is None
            t = v["timing"]
            lat_score.append(t["scoring_completed_at"] - t["scoring_started_at"])
            lat_e2e.append(t["emitted_at"] - v["kafka"]["timestamp"])
        report["engines"][eng] = {
            "records_raw": len(recs), "unique_event_ids": len(dedup), "duplicates": sum(c - 1 for c in ids.values()),
            "expected": len(exp_ids), "missing": len(missing), "unexpected": len(unexpected),
            "late_true": late_true, "late_expected": sum(1 for e in exp_ids if expected[e]["engines"][eng]["late"]),
            "late_mismatches": len(late_mismatch), "raw_feature_mismatches": len(raw_mismatch),
            "mismatch_examples": examples, "missing_examples": missing[:5],
            "scoring_status": dict(statuses), "scoring_attempts": {str(k): v for k, v in attempts.items()},
            "watermark_null": wm_null,
            "engine_job_ids": sorted({v["engine_job_id"] for v in dedup.values()}),
            "latency_ms": {
                "scoring_p50": _pct(lat_score, 50), "scoring_p95": _pct(lat_score, 95),
                "kafka_append_to_emit_p50": _pct(lat_e2e, 50), "kafka_append_to_emit_p95": _pct(lat_e2e, 95)},
        }
    # cross-engine
    both = [eid for eid, e in expected.items() if "flink" in e["engines"] and "spark" in e["engines"]]
    same_raw = same_late = same_score = same_action = compared = 0
    diffs = []
    for eid in both:
        f, s = by_engine["flink"].get(eid), by_engine["spark"].get(eid)
        if not f or not s:
            continue
        compared += 1
        same_raw += f["raw_features"] == s["raw_features"]
        same_late += f["late"] == s["late"]
        fr, sr = f["scoring"]["response"], s["scoring"]["response"]
        if fr and sr:
            same_score += fr["model"]["score"] == sr["model"]["score"]
            same_action += fr["policy"]["action"] == sr["policy"]["action"]
        if (f["raw_features"] != s["raw_features"] or f["late"] != s["late"]) and len(diffs) < 5:
            diffs.append({"event_id": eid, "seq": f["event"]["seq"], "flink_late": f["late"], "spark_late": s["late"],
                          "flink_wm": f["watermark_ms"], "spark_wm": s["watermark_ms"]})
    report["cross"] = {"events_targeting_both": len(both), "compared": compared, "raw_features_equal": same_raw,
                       "late_equal": same_late, "model_score_equal": same_score, "policy_action_equal": same_action,
                       "differences": diffs}
    # DLQ
    dlq = [r["value"] for r in read_topic(TOPICS["dlq"], from_ts=since_ms) if isinstance(r["value"], dict)]
    raws = Counter(invalid_raws)
    mine = [d for d in dlq if d.get("raw") in raws]
    report["dlq"] = {"sent_invalid": len(invalid_raws), **{eng: {"records": sum(1 for d in mine if d["engine"] == eng),
                           "errors": dict(Counter(d["error"] for d in mine if d["engine"] == eng))}
                     for eng in ("flink", "spark")}}
    report["windows"] = compare_windows(nonce, since_ms, expected, by_engine, run_id)
    return report


def compare_windows(nonce: str, since_ms: int, expected: dict, by_engine: dict, run_id: str) -> dict:
    out = {}
    for eng, topic in (("flink", "wflink"), ("spark", "wspark")):
        recs = [r["value"] for r in read_topic(TOPICS[topic], from_ts=since_ms)
                if isinstance(r["value"], dict) and r["value"].get("demo_session_id", "").endswith(nonce)]
        exp: dict[tuple, dict] = {}
        for eid, v in by_engine[eng].items():
            if eid not in expected or eng not in expected[eid]["engines"] or v["late"]:
                continue
            ev = v["event"]
            k = (ev["demo_session_id"], ev["event_time"] // 60_000 * 60_000)
            w = exp.setdefault(k, {"txn_count": 0, "amount_sum": 0.0, "alert_count": 0, "review_count": 0, "unscored_count": 0})
            action = (v["scoring"]["response"] or {}).get("policy", {}).get("action")
            w["txn_count"] += 1
            w["amount_sum"] += float(ev["amount"])
            w["alert_count"] += action == "alert"
            w["review_count"] += action == "review"
            w["unscored_count"] += action in (None, "unscored")
        got = Counter()
        emitted = {}
        for w in recs:
            k = (w["demo_session_id"], w["window_start"])
            got[k] += 1
            emitted[k] = w
        match = mism = 0
        mism_ex = []
        for k, w in emitted.items():
            e = exp.get(k)
            ok = e is not None and all(w[f] == e[f] for f in ("txn_count", "alert_count", "review_count", "unscored_count")) \
                and math.isclose(w["amount_sum"], e["amount_sum"], rel_tol=1e-9, abs_tol=1e-6)
            match += ok
            if not ok:
                mism += 1
                if len(mism_ex) < 5:
                    mism_ex.append({"key": list(k), "emitted": w, "expected": e})
        last_by_session = {}
        for (sess, ws) in exp:
            last_by_session[sess] = max(ws, last_by_session.get(sess, 0))
        not_emitted = sorted(k for k in exp if k not in emitted)
        out[eng] = {"expected_windows": len(exp), "emitted_windows": len(emitted),
                    "emitted_records": sum(got.values()), "duplicate_emissions": sum(c - 1 for c in got.values()),
                    "matching": match, "mismatching": mism, "mismatch_examples": mism_ex,
                    "not_emitted": len(not_emitted),
                    "not_emitted_are_last_window_of_session": all(ws == last_by_session[s] for s, ws in not_emitted),
                    "not_emitted_examples": [list(k) for k in not_emitted[:4]]}
    return out


def _pct(xs: list[float], q: int):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q / 100 * (len(xs) - 1))))]


def cmd_parity(args) -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    since = now_ms() - 5000
    ready = wait_engines_ready()
    nonce = uuid.uuid4().hex[:8]
    t0 = time.time()
    log(f"replaying fixture nonce={nonce} rate={args.rate}/s")
    info = replay_fixture(nonce, args.rate)
    log(f"replay done in {time.time() - t0:.1f}s: {info}")
    exp_counts = {"flink": 0, "spark": 0}
    for line in (FIX / "expected_features.jsonl").read_text(encoding="utf-8").splitlines():
        if line.strip():
            for eng in json.loads(line)["engines"]:
                exp_counts[eng] += 1
    results = wait_for_results(info["run_id"], exp_counts, since, args.timeout)
    time.sleep(15)  # let windows finalize / late duplicates arrive
    results = wait_for_results(info["run_id"], exp_counts, since, 5)
    report = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "nonce": nonce, **ready, **info,
              "replay_seconds": round(time.time() - t0, 1)}
    report.update(compare(nonce, info["run_id"], since, results, info.pop("invalid_raws")))
    jid = ready["flink_job_id"]
    report["flink_metrics"] = flink_metrics(jid)
    report["flink_checkpoints"] = flink_checkpoints(jid)
    prog = spark_main_progress(since)
    main_q = [p for p in prog if p.get("name", "").startswith("fraudwatch-spark-main")]
    win_q = [p for p in prog if p.get("name", "").startswith("fraudwatch-spark-windows")]
    report["spark_progress"] = {
        "records": len(prog), "main_batches_with_input": sum(1 for p in main_q if p.get("numInputRows", 0) > 0),
        "main_numInputRows": sum(p.get("numInputRows", 0) for p in main_q),
        "main_numRowsDroppedByWatermark": sum(so.get("numRowsDroppedByWatermark", 0) for p in main_q for so in p.get("stateOperators", [])),
        "windows_numRowsDroppedByWatermark": sum(so.get("numRowsDroppedByWatermark", 0) for p in win_q for so in p.get("stateOperators", [])),
        "main_state_rows_last": (main_q[-1].get("stateOperators") or [{}])[0].get("numRowsTotal") if main_q else None,
        "last_main_watermark": (main_q[-1].get("eventTime") or {}).get("watermark") if main_q else None,
        "batch_duration_ms_p50": _pct([p["durationMs"].get("triggerExecution", 0) for p in main_q if p.get("numInputRows", 0) > 0], 50),
    }
    (OUT / "parity_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_markdown(report)
    ok = all(report["engines"][e]["missing"] == 0 and report["engines"][e]["raw_feature_mismatches"] == 0
             and report["engines"][e]["late_mismatches"] == 0 for e in ("flink", "spark"))
    log("PARITY " + ("PASS" if ok else "FAIL"))
    print(json.dumps({e: {k: report["engines"][e][k] for k in ("unique_event_ids", "expected", "missing", "duplicates",
                                                                 "late_true", "late_mismatches", "raw_feature_mismatches",
                                                                 "scoring_status")} for e in ("flink", "spark")}, indent=1))
    return 0 if ok else 1


def write_markdown(r: dict) -> None:
    L = [f"# Streaming parity report ({r['generated_at']})", "",
         f"* nonce `{r['nonce']}`, run_id `{r['run_id']}`, replay {r['replay_seconds']} s, sent {r['sent']}",
         f"* Flink job id `{r['flink_job_id']}`; Spark app id `{r['spark_app_id']}`", "",
         "## Per engine vs fwcore reference (tests/streaming/fixtures/expected_features.jsonl)", "",
         "| metric | flink | spark |", "|---|---|---|"]
    for k in ("records_raw", "unique_event_ids", "duplicates", "expected", "missing", "unexpected", "late_true",
              "late_expected", "late_mismatches", "raw_feature_mismatches", "scoring_status", "scoring_attempts",
              "watermark_null", "latency_ms"):
        L.append(f"| {k} | {r['engines']['flink'][k]} | {r['engines']['spark'][k]} |")
    L += ["", "## Cross-engine (events targeting both engines)", "", "```json", json.dumps(r["cross"], indent=1), "```",
          "", "## DLQ", "", "```json", json.dumps(r["dlq"], indent=1), "```",
          "", "## Windows (vs aggregation of each engine's own non-late results)", "", "```json",
          json.dumps(r["windows"], indent=1), "```",
          "", "## Flink metrics / checkpoints", "", "```json",
          json.dumps({"metrics": r["flink_metrics"], "checkpoints": r["flink_checkpoints"]}, indent=1), "```",
          "", "## Spark progress (fraudwatch.metrics.spark)", "", "```json", json.dumps(r["spark_progress"], indent=1), "```"]
    for eng in ("flink", "spark"):
        if r["engines"][eng]["mismatch_examples"]:
            L += ["", f"### {eng} mismatch examples", "", "```json", json.dumps(r["engines"][eng]["mismatch_examples"], indent=1), "```"]
    (OUT / "parity_report.md").write_text("\n".join(L) + "\n", encoding="utf-8")


def cmd_failure(args) -> int:
    since = now_ms() - 2000
    nonce = uuid.uuid4().hex[:8]
    run_id = f"failure-{nonce}"
    p = producer()
    et = now_ms()
    for i in range(args.count):
        send_txn(p, make_txn(run_id, i, f"failure-{nonce}", f"acct-{i % 3}", et + i * 100, 500.0 + i, ["flink", "spark"]))
    send_heartbeat(p, et + args.count * 100)
    p.flush(10)
    got = wait_for_results(run_id, {"flink": args.count, "spark": args.count}, since, args.timeout)
    rep = {}
    for eng in ("flink", "spark"):
        vals = [r["value"] for r in got[eng]]
        rep[eng] = {"records": len(vals), "status": dict(Counter(v["scoring"]["status"] for v in vals)),
                    "attempts": dict(Counter(v["scoring"]["attempts"] for v in vals)),
                    "response_null": sum(v["scoring"]["response"] is None for v in vals),
                    "error_examples": sorted({(v["scoring"]["error"] or "")[:120] for v in vals})[:3],
                    "scoring_ms_p50": _pct([v["timing"]["scoring_completed_at"] - v["timing"]["scoring_started_at"] for v in vals], 50)}
    rep["run_id"] = run_id
    (OUT / "failure_report.json").write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=1))
    ok = all(rep[e]["records"] == args.count and rep[e]["status"].get("failed") == args.count
             and rep[e]["attempts"].get(3) == args.count for e in ("flink", "spark"))
    log("FAILURE-PATH " + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


def cmd_restore(args) -> int:
    """Produce a steady stream across an engine restart performed by the caller, then check for gaps."""
    OUT.mkdir(parents=True, exist_ok=True)
    nonce = uuid.uuid4().hex[:8]
    run_id = f"restore-{nonce}"
    state_file = OUT / f"restore_{args.phase}.json"
    if args.phase == "produce":
        since = now_ms() - 2000
        p = producer()
        base = now_ms()
        n = args.count
        t_end = time.time() + args.seconds
        i = 0
        while time.time() < t_end or i < n:
            et = base + i * 50
            send_txn(p, make_txn(run_id, i, f"restore-{nonce}", f"acct-{i % 7}", et, 100.0 + i % 50, ["flink", "spark"]))
            if i % 20 == 0:
                send_heartbeat(p, et)
            p.poll(0)
            i += 1
            time.sleep(args.seconds / max(n, 1))
            if i >= n and time.time() >= t_end:
                break
        p.flush(10)
        (OUT / "restore_run.json").write_text(json.dumps({"run_id": run_id, "count": i, "since": since}), encoding="utf-8")
        log(f"produced {i} txns for {run_id}")
        return 0
    meta = json.loads((OUT / "restore_run.json").read_text(encoding="utf-8"))
    got = wait_for_results(meta["run_id"], {"flink": meta["count"], "spark": meta["count"]}, meta["since"], args.timeout)
    rep = {"run_id": meta["run_id"], "produced": meta["count"]}
    for eng in ("flink", "spark"):
        ids = Counter(r["value"]["event"]["seq"] for r in got[eng])
        byid: dict[int, dict] = {}
        for r in got[eng]:
            byid.setdefault(r["value"]["event"]["seq"], r["value"])
        # per-account prior_count_total must be a gap-free 0..n-1 sequence if state was restored correctly
        per_acct = defaultdict(list)
        for seq in sorted(byid):
            v = byid[seq]
            per_acct[v["event"]["account_id"]].append(v["raw_features"]["prior_count_total"])
        state_ok = all(sorted(set(c)) == list(range(len(set(c)))) for c in per_acct.values())
        rep[eng] = {"unique": len(byid), "missing": meta["count"] - len(byid), "duplicates": sum(c - 1 for c in ids.values()),
                    "engine_job_ids": sorted({v["engine_job_id"] for v in byid.values()}),
                    "late": sum(v["late"] for v in byid.values()),
                    "state_counts_continuous": state_ok}
    (OUT / "restore_report.json").write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=1))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("parity")
    a.add_argument("--rate", type=float, default=250.0)
    a.add_argument("--timeout", type=int, default=300)
    b = sub.add_parser("failure")
    b.add_argument("--count", type=int, default=6)
    b.add_argument("--timeout", type=int, default=120)
    c = sub.add_parser("restore")
    c.add_argument("phase", choices=["produce", "check"])
    c.add_argument("--count", type=int, default=600)
    c.add_argument("--seconds", type=float, default=60)
    c.add_argument("--timeout", type=int, default=300)
    args = ap.parse_args()
    return {"parity": cmd_parity, "failure": cmd_failure, "restore": cmd_restore}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
