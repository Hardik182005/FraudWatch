"""Live data: overview, transactions, per-event trace, SSE stream, alerts and the audit trail."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Literal

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from .. import core
from ..access import read_session, readable, session_row
from ..core import ApiError, fetch_all, fetch_one, limiter, now_ms, principal, require_owner
from ..health import health_snapshot
from ..serialize import (alert_out, audit_out, decode_cursor, encode_cursor, trace, txn_detail, txn_summary,
                         window_out)

log = logging.getLogger("api.live")
router = APIRouter()
EngineQ = Literal["flink", "spark"]
LATENCY_WINDOW_S = 300
SERIES_BUCKET_MS = 5000


@router.get("/sessions/{session_id}/overview")
async def overview(session_id: str, request: Request, engine: EngineQ = "flink", run_id: str | None = None):
    await read_session(principal(request), session_id)
    now = now_ms()
    run_filter = "AND run_id = %(run)s" if run_id else ""
    args = {"s": session_id, "e": engine, "run": run_id, "now": now, "lat_from": now - LATENCY_WINDOW_S * 1000,
            "tp_from": now - 60_000, "series_from": now - LATENCY_WINDOW_S * 1000, "bucket": SERIES_BUCKET_MS}
    c = await fetch_one(
        f"""SELECT count(*) AS scored, count(*) FILTER (WHERE action = 'alert') AS alert,
                   count(*) FILTER (WHERE action = 'review') AS review, count(*) FILTER (WHERE action = 'allow') AS allow,
                   count(*) FILTER (WHERE action = 'unscored') AS unscored, count(*) FILTER (WHERE late) AS late,
                   coalesce(sum(amount_inr) FILTER (WHERE action IN ('alert', 'review')), 0) AS flagged_inr,
                   count(*) FILTER (WHERE persisted_at >= %(tp_from)s) AS last_minute,
                   max(persisted_at) AS last_persisted_at
            FROM scored_events WHERE session_id = %(s)s AND engine = %(e)s {run_filter}""", args)
    lat = await fetch_one(
        f"""SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY e2e_ms) AS p50,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e_ms) AS p95,
                   percentile_cont(0.99) WITHIN GROUP (ORDER BY e2e_ms) AS p99, count(e2e_ms) AS n
            FROM scored_events WHERE session_id = %(s)s AND engine = %(e)s AND persisted_at >= %(lat_from)s {run_filter}""",
        args)
    series = await fetch_all(
        f"""SELECT (persisted_at / %(bucket)s) * %(bucket)s AS t, count(*) AS scored,
                   count(*) FILTER (WHERE action IN ('alert', 'review')) AS flagged
            FROM scored_events WHERE session_id = %(s)s AND engine = %(e)s AND persisted_at >= %(series_from)s {run_filter}
            GROUP BY 1 ORDER BY 1""", args)
    windows = await fetch_all(
        """SELECT * FROM window_aggregates WHERE session_id = %(s)s AND engine = %(e)s
           ORDER BY window_start DESC LIMIT 30""", args)
    return {
        "as_of": now, "engine": engine,
        "counts": {k: c[k] for k in ("scored", "alert", "review", "allow", "unscored", "late")},
        "amount_flagged_inr": round(float(c["flagged_inr"]), 2),
        "throughput_eps_1m": round(c["last_minute"] / 60.0, 3),
        "latency_ms": {k: None if lat[k] is None else round(float(lat[k]), 1) for k in ("p50", "p95", "p99")}
        | {"n": lat["n"], "window_s": LATENCY_WINDOW_S},
        "last_persisted_at": c["last_persisted_at"],
        "series": series,
        "windows": [window_out(w) for w in reversed(windows)],
    }


@router.get("/sessions/{session_id}/transactions")
async def transactions(session_id: str, request: Request, engine: EngineQ = "flink", run_id: str | None = None,
                       action: Literal["alert", "review", "allow", "unscored"] | None = None, late: bool | None = None,
                       pattern: str | None = Query(default=None, max_length=40), limit: int = Query(50, ge=1, le=200),
                       cursor: str | None = None):
    await read_session(principal(request), session_id)
    where = ["session_id = %(s)s", "engine = %(e)s"]
    args: dict = {"s": session_id, "e": engine, "limit": limit + 1}
    if run_id:
        where.append("run_id = %(run)s")
        args["run"] = run_id
    if action:
        where.append("action = %(action)s")
        args["action"] = action
    if late is not None:
        where.append("late = %(late)s")
        args["late"] = late
    if pattern:
        where.append("pattern = %(pattern)s" if pattern != "none" else "pattern IS NULL")
        args["pattern"] = pattern
    cur = decode_cursor(cursor, 2)
    if cur:
        where.append("(persisted_at, event_id) < (%(cp)s, %(ce)s)")
        args["cp"], args["ce"] = int(cur[0]), str(cur[1])
    rows = await fetch_all(
        f"""SELECT * FROM scored_events WHERE {' AND '.join(where)}
            ORDER BY persisted_at DESC, event_id DESC LIMIT %(limit)s""", args)
    more = len(rows) > limit
    rows = rows[:limit]
    return {"items": [txn_summary(r) for r in rows],
            "next_cursor": encode_cursor(rows[-1]["persisted_at"], rows[-1]["event_id"]) if more else None}


@router.get("/transactions/{event_id}")
async def transaction(event_id: str, request: Request):
    rows = await fetch_all("SELECT * FROM scored_events WHERE event_id = %s ORDER BY engine", (event_id,))
    if not rows:
        raise ApiError(404, "not_found", "transaction not found (it may not have been processed yet)")
    s = await session_row(rows[0]["session_id"])
    readable(principal(request), s)
    return {"event": rows[0]["event"], "engines": {r["engine"]: txn_detail(r) for r in rows}, "trace": trace(rows)}


# -- SSE ---------------------------------------------------------------------------------------------
STREAM_MAX_S = core.env_int("SSE_MAX_SECONDS", 300)
STREAM_POLL_S = core.env_float("SSE_POLL_SECONDS", 0.5)
active_streams = {"n": 0}


@router.get("/sessions/{session_id}/stream")
async def stream(session_id: str, request: Request, cursor: int | None = None):
    await read_session(principal(request), session_id)
    if active_streams["n"] >= core.env_int("SSE_MAX_STREAMS", 200):
        raise ApiError(429, "capacity", "too many live streams open; retry shortly")
    limiter.check(request, "sse_connect", 60, 60)
    last_id = request.headers.get("last-event-id")
    start = cursor
    if last_id and last_id.isdigit():
        start = int(last_id)
    bounds = await fetch_one("SELECT coalesce(min(cursor), 0) AS lo, coalesce(max(cursor), 0) AS hi FROM stream_log")

    async def gen():
        active_streams["n"] += 1
        try:
            pos = bounds["hi"] if start is None else start
            if start is not None and (start > bounds["hi"] or (bounds["lo"] and start < bounds["lo"] - 1)):
                pos = bounds["hi"]
                yield {"event": "reset", "id": str(pos), "data": json.dumps({"reason": "cursor_out_of_range"})}
            began = asyncio.get_running_loop().time()
            last_health = last_ping = 0.0
            while True:
                t = asyncio.get_running_loop().time()
                if t - began > STREAM_MAX_S:
                    break
                rows = await fetch_all(
                    "SELECT cursor, type, payload FROM stream_log WHERE session_id = %s AND cursor > %s ORDER BY cursor LIMIT 500",
                    (session_id, pos))
                for r in rows:
                    pos = r["cursor"]
                    yield {"event": r["type"], "id": str(pos), "data": json.dumps(r["payload"], separators=(",", ":"))}
                if t - last_health >= 10:
                    last_health = t
                    yield {"event": "health", "id": str(pos), "data": json.dumps(await health_snapshot())}
                if t - last_ping >= 15:
                    last_ping = t
                    yield {"event": "ping", "id": str(pos), "data": json.dumps({"as_of": now_ms()})}
                if len(rows) < 500:
                    await asyncio.sleep(STREAM_POLL_S)
        except asyncio.CancelledError:
            raise
        finally:
            active_streams["n"] -= 1

    return EventSourceResponse(gen(), ping=3600, headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# -- alerts ----------------------------------------------------------------------------------------
ALERT_STATUSES = ("open", "acknowledged", "escalated", "dismissed", "confirmed_fraud_demo")
AUDIT_LOCK_ID = 470_2027
GENESIS = "0" * 64


@router.get("/sessions/{session_id}/alerts")
async def alerts(session_id: str, request: Request, engine: EngineQ = "flink",
                 status: Literal["open", "acknowledged", "escalated", "dismissed", "confirmed_fraud_demo"] | None = None,
                 action: Literal["alert", "review"] | None = None, min_score: float | None = Query(None, ge=0, le=1),
                 run_id: str | None = None, limit: int = Query(50, ge=1, le=200), cursor: str | None = None):
    await read_session(principal(request), session_id)
    where = ["session_id = %(s)s", "engine = %(e)s"]
    args: dict = {"s": session_id, "e": engine, "limit": limit + 1}
    for col, val in (("status", status), ("action", action), ("run_id", run_id)):
        if val:
            where.append(f"{col} = %({col})s")
            args[col] = val
    if min_score is not None:
        where.append("score >= %(min_score)s")
        args["min_score"] = min_score
    counts_where = " AND ".join(w for w in where if not w.startswith("status"))
    cur = decode_cursor(cursor, 1)
    if cur:
        where.append("id < %(cid)s")
        args["cid"] = int(cur[0])
    rows = await fetch_all(f"SELECT * FROM alerts WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT %(limit)s", args)
    counts = await fetch_all(f"SELECT status, count(*) AS n FROM alerts WHERE {counts_where} GROUP BY status", args)
    more = len(rows) > limit
    rows = rows[:limit]
    return {"items": [alert_out(r) for r in rows], "next_cursor": encode_cursor(rows[-1]["id"]) if more else None,
            "counts_by_status": {c["status"]: c["n"] for c in counts}}


class AlertUpdate(BaseModel):
    status: Literal["open", "acknowledged", "escalated", "dismissed", "confirmed_fraud_demo"]
    note: str = Field(default="", max_length=1000)


def audit_hash(prev_hash: str, entry: dict) -> str:
    canonical = json.dumps(entry, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256((prev_hash + canonical).encode("utf-8")).hexdigest()


def _alert_id(alert_id: str) -> int:
    if not alert_id.isdigit():
        raise ApiError(404, "not_found", "alert not found")
    return int(alert_id)


@router.patch("/alerts/{alert_id}")
async def update_alert(alert_id: str, body: AlertUpdate, request: Request):
    p = principal(request)
    aid = _alert_id(alert_id)
    a = await fetch_one("SELECT * FROM alerts WHERE id = %s", (aid,))
    if a is None:
        raise ApiError(404, "not_found", "alert not found")
    s = await session_row(a["session_id"])
    if s["kind"] == "experiment":
        if not p.operator:
            raise ApiError(403, "operator_required", "benchmark alerts are read-only")
    else:
        require_owner(p, s["id"])
    limiter.check(request, "alert_update", 60, 60)
    note = body.note.strip() or None
    now = now_ms()
    async with core.pool.connection() as c:
        async with c.transaction():
            await c.execute("SELECT pg_advisory_xact_lock(%s)", (AUDIT_LOCK_ID,))
            a = await (await c.execute("SELECT * FROM alerts WHERE id = %s FOR UPDATE", (aid,))).fetchone()
            prev = await (await c.execute("SELECT hash FROM audit_log ORDER BY id DESC LIMIT 1")).fetchone()
            prev_hash = prev["hash"] if prev else GENESIS
            entry = {"at": now, "actor": p.actor, "action": "alert.status_change", "entity_type": "alert",
                     "entity_id": str(aid), "session_id": a["session_id"], "from_status": a["status"],
                     "to_status": body.status, "note": note}
            h = audit_hash(prev_hash, entry)
            await c.execute(
                """INSERT INTO audit_log (at, actor, action, entity_type, entity_id, session_id, from_status, to_status,
                                          note, prev_hash, hash)
                   VALUES (%(at)s, %(actor)s, %(action)s, %(entity_type)s, %(entity_id)s, %(session_id)s,
                           %(from_status)s, %(to_status)s, %(note)s, %(prev_hash)s, %(hash)s)""",
                entry | {"prev_hash": prev_hash, "hash": h})
            row = await (await c.execute(
                "UPDATE alerts SET status = %s, decision_note = %s, updated_at = %s WHERE id = %s RETURNING *",
                (body.status, note, now, aid))).fetchone()
            out = alert_out(row)
            if s["stream_updates"]:
                from psycopg.types.json import Jsonb
                await c.execute("INSERT INTO stream_log (session_id, type, payload, at) VALUES (%s, 'alert', %s, %s)",
                                (s["id"], Jsonb(out), now))
    return out


@router.get("/alerts/{alert_id}/audit")
async def alert_audit(alert_id: str, request: Request):
    aid = _alert_id(alert_id)
    a = await fetch_one("SELECT session_id FROM alerts WHERE id = %s", (aid,))
    if a is None:
        raise ApiError(404, "not_found", "alert not found")
    readable(principal(request), await session_row(a["session_id"]))
    rows = await fetch_all("SELECT * FROM audit_log WHERE entity_type = 'alert' AND entity_id = %s ORDER BY id", (str(aid),))
    return [audit_out(r) for r in rows]


@router.get("/audit/verify")
async def verify_audit_chain():
    """Recomputes the whole hash chain. Public: entries carry no personal data (actors are session ids)."""
    prev = GENESIS
    n, last_id = 0, 0
    while True:
        rows = await fetch_all("SELECT * FROM audit_log WHERE id > %s ORDER BY id LIMIT 1000", (last_id,))
        if not rows:
            break
        for r in rows:
            entry = {k: r[k] for k in ("at", "actor", "action", "entity_type", "entity_id", "session_id",
                                       "from_status", "to_status", "note")}
            if r["prev_hash"] != prev or audit_hash(prev, entry) != r["hash"]:
                return {"valid": False, "entries_checked": n, "first_invalid_id": str(r["id"]), "as_of": now_ms()}
            prev = r["hash"]
            n += 1
            last_id = r["id"]
    return {"valid": True, "entries_checked": n, "head_hash": prev, "as_of": now_ms()}
