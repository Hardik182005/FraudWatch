"""FraudWatch result consumer.

Consumes engine outputs (results, windows, DLQ, Spark progress) and persists them to Postgres.
At-least-once: Kafka offsets are committed only after the Postgres transaction commits. Replayed
or duplicated records are idempotent: upsert on (engine, event_id) increments `duplicates` and
never creates a second alert or a second SSE message.

persisted_at is taken immediately before the batch INSERT; the batch commit time is measured and
reported in the component heartbeat (commit_ms) so the boundary is explicit.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from psycopg.types.json import Jsonb

from fwsvc import now_ms, settings
from fwsvc.db import connect, heartbeat, migrate
from fwsvc.logs import dlq_reason, install as install_logs

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("result-consumer")

BATCH_MAX = settings.env_int("RESULT_CONSUMER_BATCH_MAX", 500)
BATCH_WAIT_S = settings.env_float("RESULT_CONSUMER_BATCH_WAIT_S", 0.05)
STREAM_LOG_RETENTION_MS = settings.env_int("STREAM_LOG_RETENTION_MS", 3_600_000)
GROUP_ID = os.getenv("RESULT_CONSUMER_GROUP_ID", "fw-result-consumer-v1")

RESULT_TOPICS = {v: k for k, v in settings.TOPIC_RESULTS.items()}
WINDOW_TOPICS = {v: k for k, v in settings.TOPIC_WINDOWS.items()}

running = True
sink = None          # fwsvc.logs.LogSink (structured System Intelligence logs)
LOG_PER_BATCH = 20   # cap per-event log rows per batch so a failure storm cannot flood the table
stats = {"results": 0, "duplicates": 0, "alerts": 0, "windows": 0, "dlq": 0, "spark_progress": 0, "invalid": 0,
         "batches": 0, "last_commit_ms": None, "max_commit_ms": 0.0, "last_persisted_at": None}


def _stop(*_):
    global running
    running = False


def summary_from_row(r: dict) -> dict:
    return {
        "event_id": r["event_id"], "engine": r["engine"], "session_id": r["session_id"], "run_id": r["run_id"],
        "seq": r["seq"], "account_id": r["account_id"], "event_time": r["event_time"], "amount": r["amount"],
        "source_ref": r["source_ref"], "source_dt_s": r["source_dt_s"], "modified": r["modified"],
        "action": r["action"], "score": r["score"], "late": r["late"], "pattern": r["pattern"],
        "injected": r["injected"], "rules_fired": list(r["rules_fired"]), "persisted_at": r["persisted_at"],
        "e2e_ms": r["e2e_ms"],
    }


def alert_from_row(r: dict) -> dict:
    return {k: r[k] for k in ("event_id", "engine", "session_id", "run_id", "account_id", "amount",
                              "source_ref", "modified", "action", "score", "created_at", "status", "decision_note",
                              "updated_at")} | {"id": str(r["id"]), "rules_fired": list(r["rules_fired"])}


def parse_window(rec: dict) -> dict:
    """Validate a window aggregate before the transaction touches it.

    A window used to be validated only by the INSERT, so one unusable record — for instance a
    schema 1.0 window still carrying ``amount_sum_inr`` from before the dataset override — raised
    inside the transaction and failed the whole batch. The batch then retried forever and the
    pipeline stopped persisting anything. Anything unusable now goes to the DLQ like any other bad
    record, and the rest of the batch is unaffected.
    """
    if rec.get("record_type") != "window":
        raise ValueError("record_type must be window")
    if "amount_sum" not in rec and "amount_sum_inr" in rec:
        raise ValueError("schema 1.0 window (amount_sum_inr): amounts in the retired INR convention "
                         "are not comparable with the IEEE-CIS source amounts and are not persisted")
    return {"demo_session_id": rec["demo_session_id"], "window_start": int(rec["window_start"]),
            "window_end": int(rec["window_end"]), "txn_count": int(rec["txn_count"]),
            "amount_sum": float(rec["amount_sum"]), "alert_count": int(rec["alert_count"]),
            "review_count": int(rec["review_count"]), "unscored_count": int(rec.get("unscored_count", 0)),
            "emitted_at": rec.get("emitted_at")}


def parse_result(rec: dict, engine: str, persisted_at: int) -> dict:
    """Flatten an engine output record (contracts section 5) into a scored_events row."""
    if rec.get("record_type") != "scored_txn":
        raise ValueError("record_type must be scored_txn")
    ev = rec["event"]
    scoring = rec.get("scoring") or {}
    resp = scoring.get("response") or {}
    ok = scoring.get("status") == "ok" and resp
    rules_results = (resp.get("rules") or {}).get("results") or []
    timing = rec.get("timing") or {}
    kafka = rec.get("kafka") or {}
    action = (resp.get("policy") or {}).get("action") if ok else "unscored"
    score = (resp.get("model") or {}).get("score") if ok else None
    return {
        "engine": engine, "event_id": ev["event_id"], "session_id": ev["demo_session_id"], "run_id": ev["run_id"],
        "seq": int(ev["seq"]), "account_id": ev["account_id"], "event_time": int(ev["event_time"]),
        "amount": float(ev["amount"]), "source_ref": ev.get("source_ref"), "source_dt_s": ev.get("source_dt_s"),
        "modified": bool(ev.get("modified", False)), "pattern": ev.get("pattern"),
        "injected": bool(ev.get("injected", False)), "action": action or "unscored", "score": score,
        "late": bool(rec.get("late", False)), "watermark_ms": rec.get("watermark_ms"),
        "rules_fired": [r["rule_id"] for r in rules_results if r.get("fired")],
        "scoring_status": scoring.get("status") or "unknown",
        "engine_job_id": rec.get("engine_job_id"), "engine_run_label": rec.get("engine_run_label"),
        "event": Jsonb(ev),
        "detail": Jsonb({
            "raw_features": rec.get("raw_features"), "model_inputs": resp.get("model_inputs"),
            "model": resp.get("model"), "rules": resp.get("rules"), "policy": resp.get("policy"),
            "scoring": {"status": scoring.get("status"), "attempts": scoring.get("attempts"),
                        "error": scoring.get("error"), "scored_at": resp.get("scored_at"),
                        "scoring_ms": resp.get("scoring_ms")},
            "kafka": kafka, "spark": rec.get("spark"),
        }),
        "produced_at": int(ev["produced_at"]), "kafka_ts": kafka.get("timestamp"),
        "engine_received_at": timing.get("engine_received_at"), "scoring_started_at": timing.get("scoring_started_at"),
        "scoring_completed_at": timing.get("scoring_completed_at"), "emitted_at": timing.get("emitted_at"),
        "persisted_at": persisted_at, "e2e_ms": persisted_at - int(ev["produced_at"]),
    }


COLUMNS = ["engine", "event_id", "session_id", "run_id", "seq", "account_id", "event_time", "amount",
           "source_ref", "source_dt_s", "modified", "pattern", "injected", "action", "score", "late",
           "watermark_ms", "rules_fired", "scoring_status", "engine_job_id", "engine_run_label", "event", "detail",
           "produced_at", "kafka_ts", "engine_received_at", "scoring_started_at", "scoring_completed_at",
           "emitted_at", "persisted_at", "e2e_ms"]
UPSERT_SQL = (
    f"INSERT INTO scored_events ({', '.join(COLUMNS)}) VALUES ({', '.join('%(' + c + ')s' for c in COLUMNS)}) "
    "ON CONFLICT (engine, event_id) DO UPDATE SET duplicates = scored_events.duplicates + 1, "
    "last_duplicate_at = EXCLUDED.persisted_at "
    "RETURNING (xmax = 0) AS inserted, *"
)


def persist_batch(conn, msgs: list) -> None:
    persisted_at = now_ms()
    results, windows, dlq, progress = [], [], [], []
    for m in msgs:
        topic = m.topic()
        try:
            rec = json.loads(m.value())
            if topic in RESULT_TOPICS:
                results.append(parse_result(rec, RESULT_TOPICS[topic], persisted_at))
            elif topic in WINDOW_TOPICS:
                windows.append((WINDOW_TOPICS[topic], parse_window(rec)))
            elif topic == settings.TOPIC_DLQ:
                dlq.append((rec, m))
            elif topic == settings.TOPIC_SPARK_METRICS:
                progress.append(rec)
        except Exception as exc:
            stats["invalid"] += 1
            dlq.append(({"engine": "result_consumer", "error": f"unparseable {topic} record: {exc}",
                         "raw": (m.value() or b"")[:4096].decode("utf-8", "replace")}, m))

    t0 = time.perf_counter()
    with conn.transaction():
        streamed_sessions = {}
        if results:
            ids = sorted({r["session_id"] for r in results})
            streamed_sessions = {row["id"]: row["stream_updates"] for row in
                                 conn.execute("SELECT id, stream_updates FROM demo_sessions WHERE id = ANY(%s)", (ids,))}
        feed = []
        dup_logs, fail_logs = [], []
        with conn.cursor() as cur:
            for r in results:
                row = cur.execute(UPSERT_SQL, r).fetchone()
                if not row["inserted"]:
                    stats["duplicates"] += 1
                    if len(dup_logs) < LOG_PER_BATCH:
                        dup_logs.append(row)
                    continue
                if row["scoring_status"] != "ok" and len(fail_logs) < LOG_PER_BATCH:
                    fail_logs.append(row)
                stats["results"] += 1
                stream = streamed_sessions.get(r["session_id"], False)
                if stream:
                    feed.append((r["session_id"], "txn", summary_from_row(row)))
                if row["action"] in ("alert", "review"):
                    a = cur.execute(
                        """INSERT INTO alerts (event_id, engine, session_id, run_id, account_id, amount, source_ref, modified,
                                               action, score, rules_fired, created_at, updated_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (engine, event_id) DO NOTHING RETURNING *""",
                        (row["event_id"], row["engine"], row["session_id"], row["run_id"], row["account_id"],
                         row["amount"], row["source_ref"], row["modified"], row["action"], row["score"], row["rules_fired"],
                         persisted_at, persisted_at)).fetchone()
                    if a is not None:
                        stats["alerts"] += 1
                        if stream:
                            feed.append((r["session_id"], "alert", alert_from_row(a)))
            for engine, w in windows:
                row = cur.execute(
                    """INSERT INTO window_aggregates (engine, session_id, window_start, window_end, txn_count, amount_sum,
                                                      alert_count, review_count, unscored_count, emitted_at, persisted_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (engine, session_id, window_start) DO UPDATE SET duplicates = window_aggregates.duplicates + 1
                       RETURNING (xmax = 0) AS inserted, *""",
                    (engine, w["demo_session_id"], int(w["window_start"]), int(w["window_end"]), int(w["txn_count"]),
                     float(w["amount_sum"]), int(w["alert_count"]), int(w["review_count"]),
                     int(w.get("unscored_count", 0)), w.get("emitted_at"), persisted_at)).fetchone()
                if row["inserted"]:
                    stats["windows"] += 1
                    feed.append((row["session_id"], "window", {
                        k: row[k] for k in ("engine", "window_start", "window_end", "txn_count", "amount_sum",
                                            "alert_count", "review_count", "unscored_count", "emitted_at", "persisted_at")
                    } | {"session_id": row["session_id"]}))
            for rec, m in dlq:
                cur.execute("INSERT INTO dlq_events (engine, error, raw, kafka, received_at, persisted_at) VALUES (%s,%s,%s,%s,%s,%s)",
                            (rec.get("engine"), str(rec.get("error"))[:2000], (rec.get("raw") or "")[:4096],
                             Jsonb(rec.get("kafka")), rec.get("received_at"), persisted_at))
                stats["dlq"] += 1
            for p in progress:
                cur.execute("INSERT INTO spark_progress (query_name, run_label, batch_id, progress, at) VALUES (%s,%s,%s,%s,%s)",
                            (p.get("name") or p.get("query_name"), p.get("run_label") or p.get("engine_run_label"),
                             p.get("batchId", p.get("batch_id")), Jsonb(p), persisted_at))
                stats["spark_progress"] += 1
            if feed:
                window_sessions = {s for s, t, _ in feed if t == "window"}
                allowed = streamed_sessions
                if window_sessions - allowed.keys():
                    allowed = allowed | {row["id"]: row["stream_updates"] for row in conn.execute(
                        "SELECT id, stream_updates FROM demo_sessions WHERE id = ANY(%s)", (sorted(window_sessions),))}
                cur.executemany("INSERT INTO stream_log (session_id, type, payload, at) VALUES (%s,%s,%s,%s)",
                                [(s, t, Jsonb(p), persisted_at)
                                 for s, t, p in feed if allowed.get(s, False)])
    commit_ms = (time.perf_counter() - t0) * 1000
    if sink is not None:
        _log_batch(dup_logs, fail_logs, dlq[:LOG_PER_BATCH], persisted_at)
    stats["batches"] += 1
    stats["last_commit_ms"] = round(commit_ms, 2)
    stats["max_commit_ms"] = max(stats["max_commit_ms"], round(commit_ms, 2))
    if results:
        stats["last_persisted_at"] = persisted_at


def _log_batch(dup_logs: list, fail_logs: list, dlq: list, persisted_at: int) -> None:
    for row in fail_logs:
        sc = (row["detail"] or {}).get("scoring") or {}
        sink.warning(f"{row['engine']} result persisted without a model score ({row['scoring_status']}): "
                     f"{sc.get('error') or 'no error text'}",
                     session_id=row["session_id"], run_id=row["run_id"], event_id=row["event_id"],
                     trace_id=row["event_id"], engine=row["engine"], job_id=row["engine_job_id"], stage="scored",
                     status=row["scoring_status"], error_code="scoring_failed", retry_attempt=sc.get("attempts"),
                     timestamp_utc=row["scoring_completed_at"] or row["emitted_at"] or persisted_at)
    for row in dup_logs:
        sink.info(f"duplicate {row['engine']} delivery collapsed by (engine, event_id) upsert "
                  f"(duplicates={row['duplicates']})",
                  session_id=row["session_id"], run_id=row["run_id"], event_id=row["event_id"], trace_id=row["event_id"],
                  engine=row["engine"], job_id=row["engine_job_id"], stage="persisted", status="duplicate",
                  timestamp_utc=persisted_at)
    for rec, m in dlq:
        err = str(rec.get("error") or "")
        sink.warning(f"dead-letter record from {rec.get('engine') or 'unknown'}: {err[:300]}", engine=rec.get("engine"),
                     stage="engine_received", status="dead_letter", error_code=dlq_reason(err),
                     timestamp_utc=rec.get("received_at") or persisted_at,
                     payload={"kafka": rec.get("kafka"), "raw_excerpt": (rec.get("raw") or "")[:300]})


def main() -> None:
    global sink
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    migrate(settings.DATABASE_URL)
    sink = install_logs(settings.DATABASE_URL, "result_consumer")
    sink.info("result consumer started", stage="lifecycle", status="started", payload={"group": GROUP_ID})
    conn = connect(settings.DATABASE_URL, autocommit=True)
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "client.id": "fw-result-consumer",
    })
    topics = list(RESULT_TOPICS) + list(WINDOW_TOPICS) + [settings.TOPIC_DLQ, settings.TOPIC_SPARK_METRICS]
    consumer.subscribe(topics)
    log.info("subscribed to %s", topics)
    last_hb = last_trim = 0.0
    while running:
        msgs = consumer.consume(num_messages=BATCH_MAX, timeout=BATCH_WAIT_S)
        good = []
        for m in msgs:
            err = m.error()
            if err is None:
                good.append(m)
            elif err.code() not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                log.warning("consumer error: %s", err)
        if good:
            for attempt in range(1, 6):
                try:
                    persist_batch(conn, good)
                    break
                except Exception as exc:
                    log.exception("persist failed (attempt %d)", attempt)
                    if conn.closed or conn.broken:
                        conn = connect(settings.DATABASE_URL, autocommit=True)
                    else:
                        conn.rollback()
                    if attempt == 5:
                        raise SystemExit(f"cannot persist batch: {exc}")  # restart; offsets not committed
                    time.sleep(min(2 ** attempt * 0.2, 5))
            try:
                consumer.commit(asynchronous=False)
            except KafkaException as exc:
                log.warning("offset commit failed (records will be re-delivered and deduplicated): %s", exc)
        mono = time.monotonic()
        if mono - last_hb >= 5:
            last_hb = mono
            try:
                with conn.transaction():
                    heartbeat(conn, "result_consumer", "ok", f"{stats['results']} results persisted", dict(stats))
            except Exception as exc:
                log.warning("heartbeat failed: %s", exc)
                conn.rollback()
        if mono - last_trim >= 60:
            last_trim = mono
            try:
                with conn.transaction():
                    conn.execute("DELETE FROM stream_log WHERE at < %s", (now_ms() - STREAM_LOG_RETENTION_MS,))
            except Exception as exc:
                log.warning("stream_log trim failed: %s", exc)
                conn.rollback()
    consumer.close()
    conn.close()


if __name__ == "__main__":
    main()
