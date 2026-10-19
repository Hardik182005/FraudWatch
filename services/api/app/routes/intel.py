"""System Intelligence (contracts sections 7.3-7.6): structured logs + live tail, trace explorer, data quality
and model/stream operations."""
from __future__ import annotations

import asyncio
import csv
import io
import json
import time

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from fwsvc import settings
from fwsvc.logs import FIELDS, SEVERITIES, dlq_reason, redact_text

from .. import core
from ..access import read_run, readable, session_row
from ..core import ApiError, Principal, fetch_all, fetch_one, limiter, now_ms, principal
from ..health import health_snapshot
from ..serialize import decode_cursor, encode_cursor
from .system import model_metadata

router = APIRouter()
DRAIN_TIMEOUT_MS = core.env_int("RUN_DRAIN_TIMEOUT_MS", 120_000)
SPARK_TRIGGER = core.env("SPARK_TRIGGER_INTERVAL", "1 second")
CONSUMER_GROUPS = {
    core.env("RESULT_CONSUMER_GROUP_ID", "fw-result-consumer-v1"):
        [*settings.TOPIC_RESULTS.values(), *settings.TOPIC_WINDOWS.values(), settings.TOPIC_DLQ],
    core.env("FLINK_GROUP_ID", "fw-flink-v1"): [settings.TOPIC_TRANSACTIONS],
}


# -- logs --------------------------------------------------------------------------------------
def _csv_list(v: str | None) -> list[str] | None:
    items = [x.strip() for x in (v or "").split(",") if x.strip()]
    return items[:20] or None


def _log_filters(p: Principal, f: dict) -> tuple[list[str], dict]:
    """WHERE clauses for a log query. Visitors see their own session's rows plus system-wide rows."""
    where, args = [], {}
    if not p.operator:
        if p.session_id:
            where.append("(session_id IS NULL OR session_id = %(own)s)")
            args["own"] = p.session_id
        else:
            where.append("session_id IS NULL")
    if f.get("from") is not None:
        where.append("timestamp_utc >= %(from)s")
        args["from"] = f["from"]
    if f.get("to") is not None:
        where.append("timestamp_utc <= %(to)s")
        args["to"] = f["to"]
    sev = (f.get("severity") or "").lower()
    if sev in SEVERITIES:
        where.append("severity = ANY(%(sev)s)")
        args["sev"] = list(SEVERITIES[SEVERITIES.index(sev):])
    for key in ("service", "engine"):
        vals = _csv_list(f.get(key))
        if vals:
            where.append(f"{key} = ANY(%({key})s)")
            args[key] = vals
    for key in ("session_id", "run_id", "event_id"):
        if f.get(key):
            where.append(f"{key} = %({key}_eq)s")
            args[f"{key}_eq"] = f[key]
    if f.get("q"):
        where.append("(message ILIKE %(q)s OR error_code ILIKE %(q)s)")
        args["q"] = "%" + f["q"].replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_") + "%"
    return where, args


def log_out(r: dict) -> dict:
    return {k: r.get(k) for k in FIELDS} | {"id": str(r["id"])}


def _filters(request: Request) -> dict:
    qp = request.query_params
    out: dict = {k: qp.get(k) for k in ("severity", "service", "engine", "session_id", "run_id", "event_id", "q")}
    for k in ("from", "to"):
        v = qp.get(k)
        if v:
            try:
                out[k] = int(float(v))
            except ValueError as exc:
                raise ApiError(422, "validation_error", f"{k}: must be epoch milliseconds") from exc
    if out.get("q"):
        out["q"] = out["q"][:200]
    return out


@router.get("/system/logs")
async def system_logs(request: Request, limit: int = Query(100, ge=1, le=500), cursor: str | None = None):
    p = principal(request)
    where, args = _log_filters(p, _filters(request))
    after = decode_cursor(cursor, 1)
    if after:
        where.append("id < %(cur)s")
        args["cur"] = int(after[0])
    args["limit"] = limit + 1
    rows = await fetch_all(f"SELECT * FROM system_logs {'WHERE ' + ' AND '.join(where) if where else ''} "
                           "ORDER BY id DESC LIMIT %(limit)s", args)
    more = len(rows) > limit
    rows = rows[:limit]
    return {"items": [log_out(r) for r in rows], "next_cursor": encode_cursor(rows[-1]["id"]) if more else None}


def _csv_safe(v) -> str:
    if v is None:
        return ""
    s = json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)
    return "'" + s if s[:1] in ("=", "+", "-", "@", "\t", "\r") else s


@router.get("/system/logs/export.csv")
async def system_logs_csv(request: Request):
    p = principal(request)
    limiter.check(request, "logs_export", 10, 60)
    where, args = _log_filters(p, _filters(request))
    rows = await fetch_all(f"SELECT * FROM system_logs {'WHERE ' + ' AND '.join(where) if where else ''} "
                           "ORDER BY id DESC LIMIT 5000", args)
    buf = io.StringIO()
    w = csv.writer(buf)
    cols = ("id", *FIELDS)
    w.writerow(cols)
    for r in rows:
        w.writerow([_csv_safe(r.get(c)) for c in cols])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="fraudwatch-logs.csv"'})


TAIL_MAX_S = core.env_int("SSE_MAX_SECONDS", 300)
TAIL_BEHIND = 500
tail_streams = {"n": 0}


@router.get("/system/logs/stream")
async def system_logs_stream(request: Request, after: str | None = None):
    p = principal(request)
    if tail_streams["n"] >= core.env_int("SSE_MAX_LOG_STREAMS", 50):
        raise ApiError(429, "capacity", "too many log tails open; retry shortly")
    limiter.check(request, "logs_tail", 30, 60)
    where, args = _log_filters(p, _filters(request))
    cond = " AND ".join(where + ["id > %(pos)s"])
    start = request.headers.get("last-event-id") or after
    hi = (await fetch_one("SELECT coalesce(max(id), 0) AS hi FROM system_logs"))["hi"]

    async def gen():
        tail_streams["n"] += 1
        try:
            pos = int(start) if start and start.isdigit() and int(start) <= hi else hi
            loop = asyncio.get_running_loop()
            began = last_ping = loop.time()
            yield {"event": "ping", "id": str(pos), "data": json.dumps({"at": now_ms()})}
            while loop.time() - began < TAIL_MAX_S:
                rows = await fetch_all(f"SELECT * FROM system_logs WHERE {cond} ORDER BY id LIMIT {TAIL_BEHIND + 1}",
                                       args | {"pos": pos})
                if len(rows) > TAIL_BEHIND:
                    top = await fetch_one(f"SELECT count(*) AS n, max(id) AS hi FROM system_logs WHERE {cond}",
                                          args | {"pos": pos})
                    pos = top["hi"]
                    yield {"event": "behind", "id": str(pos), "data": json.dumps({"skipped": top["n"]})}
                else:
                    for r in rows:
                        pos = r["id"]
                        yield {"event": "log", "id": str(pos),
                               "data": json.dumps(log_out(r), default=str, separators=(",", ":"))}
                if loop.time() - last_ping >= 15:
                    last_ping = loop.time()
                    yield {"event": "ping", "id": str(pos), "data": json.dumps({"at": now_ms()})}
                await asyncio.sleep(1.0)
        finally:
            tail_streams["n"] -= 1

    return EventSourceResponse(gen(), ping=3600, headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# -- trace explorer ----------------------------------------------------------------------------
STAGES = ("generated", "kafka_acked", "engine_received", "features_ready", "scored", "engine_emitted", "persisted",
          "alert_evaluated")


def _engine_trace(r: dict, alert_at: int | None) -> dict:
    d = r["detail"] or {}
    scoring = d.get("scoring") or {}
    kafka = d.get("kafka") or {}
    ok_scoring = (scoring.get("status") or r["scoring_status"]) == "ok"
    flagged = r["action"] in ("alert", "review")
    spec = [
        ("generated", r["produced_at"], "generator produced the event (Kafka producer, acks=all)"),
        ("kafka_acked", r["kafka_ts"], f"broker LogAppendTime · partition {kafka.get('partition')} offset "
                                       f"{kafka.get('offset')}" if r["kafka_ts"] else None),
        ("engine_received", r["engine_received_at"],
         "micro-batch picked the record up (Spark)" if r["engine"] == "spark" else "keyed operator handled the record (Flink)"),
        ("features_ready", r["scoring_started_at"], "account state updated, features complete, scoring request sent"),
        ("scored", r["scoring_completed_at"],
         f"{scoring.get('status') or r['scoring_status']} after {scoring.get('attempts') or 0} attempt(s)"
         + (f": {scoring['error']}" if scoring.get("error") else "")),
        ("engine_emitted", r["emitted_at"], f"written to the {r['engine']} results topic"),
        ("persisted", r["persisted_at"], f"result consumer upsert (extra deliveries: {r['duplicates']})"),
    ]
    stages, missing, prev = [], [], None
    for name, at, detail in spec:
        status = "ok" if at is not None else "missing"
        if name == "scored" and not ok_scoring:
            status = "failed"
        if at is None:
            missing.append(name)
        stages.append({"stage": name, "at_ms": at, "duration_ms": None if at is None or prev is None else at - prev,
                       "status": status, "detail": detail})
        prev = at if at is not None else prev
    if flagged:
        stages.append({"stage": "alert_evaluated", "at_ms": alert_at,
                       "duration_ms": None if alert_at is None or prev is None else alert_at - prev,
                       "status": "ok" if alert_at is not None else "missing",
                       "detail": f"{r['action']} alert row created" if alert_at else None})
        if alert_at is None:
            missing.append("alert_evaluated")
    else:
        stages.append({"stage": "alert_evaluated", "at_ms": None, "duration_ms": None, "status": "not_applicable",
                       "detail": "allowed, no alert needed"})
    return {"stages": stages, "missing": missing}


@router.get("/system/trace/{event_id}")
async def system_trace(event_id: str, request: Request):
    p = principal(request)
    rows = await fetch_all("SELECT * FROM scored_events WHERE event_id = %s ORDER BY engine", (event_id,))
    if not rows:
        raise ApiError(404, "not_found", "no engine has persisted this event (yet)")
    s = await session_row(rows[0]["session_id"])
    readable(p, s)
    alerts = {a["engine"]: a["created_at"] for a in
              await fetch_all("SELECT engine, created_at FROM alerts WHERE event_id = %s", (event_id,))}
    engines: dict = {"flink": None, "spark": None}
    for r in rows:
        engines[r["engine"]] = _engine_trace(r, alerts.get(r["engine"]))
    receipt = await fetch_one("SELECT received_at_ms FROM client_receipts WHERE event_id = %s AND session_id = %s",
                              (event_id, s["id"]))
    logs = await fetch_all("SELECT * FROM system_logs WHERE event_id = %s ORDER BY id LIMIT 100", (event_id,))
    return {"event_id": event_id, "session_id": s["id"], "run_id": rows[0]["run_id"], "seq": rows[0]["seq"],
            "engines": engines,
            "client_received": {"at_ms": receipt["received_at_ms"], "source": "browser",
                                "note": "browser clock; never mixed into server-side delays"} if receipt else None,
            "logs": [log_out(r) for r in logs]}


class Receipt(BaseModel):
    event_id: str = Field(min_length=1, max_length=120)
    received_at_ms: int = Field(ge=0)


@router.post("/system/client-receipt")
async def client_receipt(body: Receipt, request: Request):
    p = principal(request)
    if not (p.operator or p.session_id):
        raise ApiError(401, "auth_required", "a session token is required")
    limiter.check(request, "client_receipt", 120, 60)
    row = await fetch_one("SELECT session_id FROM scored_events WHERE event_id = %s LIMIT 1", (body.event_id,))
    if row is None:
        raise ApiError(404, "not_found", "event not persisted yet")
    if not core.can_read_session(p, row["session_id"]):
        raise ApiError(403, "forbidden", "token does not grant access to this event")
    async with core.pool.connection() as c:
        await c.execute("""INSERT INTO client_receipts (event_id, session_id, received_at_ms, recorded_at)
                           VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                        (body.event_id, row["session_id"], body.received_at_ms, now_ms()))
    return {"recorded": True}


# -- data quality ------------------------------------------------------------------------------
DEFINITIONS = {
    "malformed": "Dead-lettered records that were not valid JSON (operator only).",
    "schema_mismatch": "Dead-lettered records with a wrong schema version, type or value range (operator only).",
    "missing_fields": "Dead-lettered records missing a required field (operator only).",
    "duplicates": "Extra deliveries collapsed by the (engine, event_id) upsert; never counted as new transactions.",
    "processing_failures": "Results whose model scoring did not complete (scoring_status other than ok).",
    "late": "Events that arrived behind the engine's watermark.",
    "unmatched": "Events persisted by only one engine in runs that targeted both.",
    "stale": "Acknowledged events with no persisted result after the drain timeout.",
}


@router.get("/system/data-quality")
async def data_quality(request: Request, run_id: str | None = None):
    p = principal(request)
    now = now_ms()
    scope = {"run_id": run_id, "session_id": None, "from": None, "to": now}
    if run_id:
        r, s = await read_run(p, run_id)
        ev, rn, args = "se.run_id = %(run)s", "r.id = %(run)s", {"run": run_id}
        scope.update(session_id=s["id"], **{"from": r["created_at"]})
    elif p.session_id and not p.operator:
        ev, rn, args = "se.session_id = %(s)s", "r.session_id = %(s)s", {"s": p.session_id}
        scope["session_id"] = p.session_id
    else:
        scope["from"] = now - 24 * 3_600_000
        args = {"from": scope["from"]}
        ev, rn = "se.persisted_at >= %(from)s", "r.created_at >= %(from)s"
        if not p.operator:  # anonymous: only the published experiment sessions
            pub = "IN (SELECT id FROM demo_sessions WHERE kind = 'experiment')"
            ev, rn = f"{ev} AND se.session_id {pub}", f"{rn} AND r.session_id {pub}"

    agg = await fetch_one(f"""SELECT coalesce(sum(duplicates), 0) AS duplicates,
                                     count(*) FILTER (WHERE scoring_status <> 'ok') AS processing_failures,
                                     count(*) FILTER (WHERE late) AS late
                              FROM scored_events se WHERE {ev}""", args)
    unmatched = await fetch_one(f"""SELECT count(*) AS n FROM (
                                      SELECT se.event_id FROM scored_events se JOIN runs r ON r.id = se.run_id
                                      WHERE {ev} AND cardinality(r.engines) > 1
                                      GROUP BY se.event_id HAVING count(*) = 1) x""", args)
    runs = await fetch_all(f"""SELECT r.id, r.status, r.engines, r.events_sent, r.events_acked, r.ended_at
                               FROM runs r WHERE {rn} ORDER BY r.created_at DESC LIMIT 50""", args)
    per = {}
    if runs:
        for row in await fetch_all("""SELECT run_id, engine, count(*) AS persisted, coalesce(sum(duplicates), 0) AS dup
                                      FROM scored_events WHERE run_id = ANY(%s) GROUP BY run_id, engine""",
                                   ([r["id"] for r in runs],)):
            per[(row["run_id"], row["engine"])] = row
    recon, stale = [], 0
    for r in runs:
        for e in r["engines"]:
            got = per.get((r["id"], e)) or {"persisted": 0, "dup": 0}
            missing = max(r["events_sent"] - got["persisted"], 0)
            if r["status"] in ("starting", "running", "paused"):
                state = "running"
            elif missing == 0:
                state = "reconciled"
            elif r["ended_at"] and now - r["ended_at"] < DRAIN_TIMEOUT_MS:
                state = "draining"
            else:
                state = "incomplete"
                stale += missing
            recon.append({"run_id": r["id"], "engine": e, "generated": r["events_sent"], "acked": r["events_acked"],
                          "persisted": got["persisted"], "missing": missing, "duplicates": int(got["dup"]),
                          "state": state})

    counts = {"malformed": None, "schema_mismatch": None, "missing_fields": None,
              "duplicates": int(agg["duplicates"]), "processing_failures": agg["processing_failures"],
              "late": agg["late"], "unmatched": unmatched["n"], "stale": stale}
    dlq = {"items": [], "restricted": not p.operator}
    if p.operator:
        since = scope["from"] or now - 24 * 3_600_000
        rows = await fetch_all("SELECT * FROM dlq_events WHERE persisted_at >= %s ORDER BY id DESC LIMIT 2000", (since,))
        for k in ("malformed", "schema_mismatch", "missing_fields"):
            counts[k] = 0
        for i, d in enumerate(rows):
            reason = dlq_reason(d["error"])
            if reason in counts:
                counts[reason] += 1
            if i < 50:
                dlq["items"].append({"id": str(d["id"]), "engine": d["engine"], "error": redact_text(d["error"]),
                                     "reason": reason, "received_at": d["received_at"],
                                     "persisted_at": d["persisted_at"], "kafka": d["kafka"],
                                     "raw_excerpt": redact_text((d["raw"] or "")[:300]) or None})
    return {"scope": scope, "counts": counts, "definitions": DEFINITIONS, "reconciliation": recon, "dlq": dlq}


# -- operations --------------------------------------------------------------------------------
_ops_cache: dict = {"at": 0.0, "value": None}
_ops_lock = asyncio.Lock()


def _kafka_lag_sync() -> list[dict]:
    from confluent_kafka import Consumer, TopicPartition

    out = []
    for group, topics in CONSUMER_GROUPS.items():
        c = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS, "group.id": group,
                      "enable.auto.commit": False, "socket.timeout.ms": 3000})
        try:
            md = c.list_topics(timeout=3)
            tps = [TopicPartition(t, pid) for t in topics if t in md.topics for pid in md.topics[t].partitions]
            if not tps:
                continue
            for tp in c.committed(tps, timeout=4):
                _, hi = c.get_watermark_offsets(tp, timeout=3, cached=False)
                committed = tp.offset if tp.offset >= 0 else None
                out.append({"group": group, "topic": tp.topic, "partition": tp.partition, "committed": committed,
                            "end_offset": hi, "lag": None if committed is None else max(hi - committed, 0)})
        finally:
            c.close()
    return out


async def _flink_vertices(job_id: str) -> tuple[list | str, list | str]:
    base = core.FLINK_REST_URL
    try:
        job = (await core.http.get(f"{base}/jobs/{job_id}", timeout=3)).json()
        verts = job.get("vertices") or []
    except Exception:
        return "not_instrumented", "not_instrumented"
    marks, pressure = [], []
    for v in verts[:12]:
        name = (v.get("name") or v["id"])[:80]
        try:
            w = (await core.http.get(f"{base}/jobs/{job_id}/vertices/{v['id']}/watermarks", timeout=3)).json()
            vals = [int(float(x["value"])) for x in w if str(x.get("id", "")).endswith("currentInputWatermark")]
            vals = [x for x in vals if x > 0]
            marks.append({"vertex": name, "current_input_watermark_ms": min(vals) if vals else None})
        except Exception:
            pass
        try:
            b = (await core.http.get(f"{base}/jobs/{job_id}/vertices/{v['id']}/backpressure", timeout=3)).json()
            ratios = [s.get("ratio") for s in b.get("subtasks") or [] if s.get("ratio") is not None]
            pressure.append({"vertex": name, "level": b.get("backpressureLevel") or b.get("backpressure-level")
                             or b.get("status") or "unknown", "ratio": max(ratios) if ratios else None})
        except Exception:
            pass
    return marks or "not_instrumented", pressure or "not_instrumented"


async def _operations() -> dict:
    now = now_ms()
    health = await health_snapshot()
    comp = {c["id"]: c for c in health["components"]}

    model = None
    try:
        md = await model_metadata()
        feats = md.get("features") or {}
        names = (feats.get("numeric") or []) + (feats.get("categorical") or []) if isinstance(feats, dict) else list(feats)
        th = md.get("thresholds") or {}
        sc = comp.get("scoring", {}).get("metrics") or {}
        inf = None
        try:
            hz = (await core.http.get(f"{core.SCORING_SERVICE_URL}/healthz", timeout=3)).json()
            inf = hz.get("inference_ms")
            if inf:
                inf = inf | {"window_ms": None}
        except Exception:
            pass
        model = {"version": md.get("model_version"), "checksum": md.get("checksum"),
                 "feature_schema": md.get("feature_spec"), "features": names,
                 "thresholds": {"alert": th.get("alert"), "review": th.get("review")} if th else None,
                 "inference_errors": sc.get("errors"), "inference_ms": inf}
    except Exception:
        pass

    fm = comp.get("flink", {}).get("metrics") or {}
    cp = fm.get("last_checkpoint") or None
    flink = {"job_id": fm.get("job_id"), "state": fm.get("job_state"),
             "checkpoint_age_ms": now - cp["completed_at"] if cp and cp.get("completed_at") else None,
             "last_checkpoint": cp, "restarts": fm.get("restarts"),
             "watermarks": "not_instrumented", "backpressure": "not_instrumented"}
    if fm.get("job_id") and fm.get("job_state") == "RUNNING":
        flink["watermarks"], flink["backpressure"] = await _flink_vertices(fm["job_id"])

    smx = comp.get("spark", {}).get("metrics") or {}
    last = await fetch_one("SELECT progress FROM spark_progress ORDER BY id DESC LIMIT 1")
    prog = (last or {}).get("progress") or {}
    dur = prog.get("durationMs") or {}
    spark_state = smx.get("state")
    lb = smx.get("last_batch_at")
    if isinstance(lb, (int, float)) and now_ms() - lb < 30_000 and spark_state in (None, "idle", "unknown"):
        spark_state = "running"  # a batch landed within the last 30 s: the query is active
    spark = {"state": spark_state, "app_id": smx.get("app_id"), "run_label": smx.get("run_label"),
             "last_batch_id": smx.get("last_batch_id"), "last_batch_at": smx.get("last_batch_at"),
             "batch_duration_ms": prog.get("batchDuration") or dur.get("triggerExecution"),
             "input_rows_per_second": prog.get("inputRowsPerSecond"),
             "processed_rows_per_second": prog.get("processedRowsPerSecond"), "trigger": SPARK_TRIGGER}

    notes = ["Flink commits Kafka offsets on each checkpoint, so its lag can trail by one checkpoint interval.",
             "Spark Structured Streaming tracks offsets in its own checkpoint, not in a Kafka consumer group."]
    try:
        lag = await asyncio.wait_for(asyncio.get_running_loop().run_in_executor(None, _kafka_lag_sync), 12)
    except Exception as exc:
        lag = "not_instrumented"
        notes.append(f"consumer-group lag unavailable: {type(exc).__name__}")

    incidents = []
    for c in health["components"]:
        if c["status"] in ("down", "degraded"):
            incidents.append({"id": f"component:{c['id']}", "severity": "critical" if c["status"] == "down" else "warning",
                              "title": f"{c['id']} is {c['status']}", "detail": c.get("detail"),
                              "since": c.get("checked_at") or now, "source": "health"})
    errs = await fetch_one("""SELECT count(*) AS n, count(*) FILTER (WHERE severity = 'critical') AS crit,
                                     min(timestamp_utc) AS since
                              FROM system_logs WHERE severity IN ('error', 'critical') AND timestamp_utc >= %s""",
                           (now - 15 * 60_000,))
    if errs and errs["n"]:
        incidents.append({"id": "logs:errors-15m", "severity": "critical" if errs["crit"] else "warning",
                          "title": f"{errs['n']} error log(s) in the last 15 minutes",
                          "detail": "open the Logs tab filtered to error+", "since": errs["since"], "source": "logs"})
    if cp and flink["checkpoint_age_ms"] and flink["checkpoint_age_ms"] > 5 * 60_000 and flink["state"] == "RUNNING":
        incidents.append({"id": "flink:checkpoint-stale", "severity": "warning", "title": "Flink checkpoint is stale",
                          "detail": f"last completed {flink['checkpoint_age_ms'] // 1000} s ago",
                          "since": cp["completed_at"], "source": "flink"})
    if isinstance(lag, list):
        big = [x for x in lag if (x["lag"] or 0) > 5000]
        if big:
            incidents.append({"id": "kafka:lag", "severity": "warning", "title": "Consumer lag above 5,000 records",
                              "detail": ", ".join(f"{x['group']} {x['topic']}[{x['partition']}]={x['lag']}" for x in big[:4]),
                              "since": now, "source": "kafka"})
    return {"as_of": now, "model": model, "flink": flink, "spark": spark, "kafka_lag": lag,
            "kafka_lag_notes": notes, "incidents": incidents}


@router.get("/system/operations")
async def operations():
    if _ops_cache["value"] is None or time.monotonic() - _ops_cache["at"] > 5:
        async with _ops_lock:
            if _ops_cache["value"] is None or time.monotonic() - _ops_cache["at"] > 5:
                _ops_cache.update(value=await _operations(), at=time.monotonic())
    return _ops_cache["value"]
