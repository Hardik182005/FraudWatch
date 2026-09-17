"""Runs, scenario controls, replay, and per-run engine comparison / late-event views."""
from __future__ import annotations

import secrets
from typing import Literal

from fastapi import APIRouter, Request
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from fwcore.schema import SCENARIOS
from fwcore.simulation import PATTERNS

from .. import core
from ..access import own_run, read_run, read_session, session_row
from ..core import ApiError, fetch_all, fetch_one, limiter, now_ms, principal, require_owner
from ..serialize import run_out

router = APIRouter()
ACTIVE = ("starting", "running", "paused")


class CreateRun(BaseModel):
    scenario: str
    rate_eps: float = Field(gt=0)
    max_events: int | None = Field(default=None, ge=1)
    seed: int | None = Field(default=None, ge=0, le=2**62)
    engines: list[Literal["flink", "spark"]] | None = None


async def _claim_and_start(p, s: dict, *, scenario: str, seed: int, rate: float, max_events: int, engines: list[str],
                           replay_of: str | None) -> dict:
    """Insert the run and claim the session's single active-run slot atomically, then start the generator."""
    run_id = "r_" + secrets.token_hex(8)
    now = now_ms()
    created_by = "operator" if p.operator else "visitor"
    async with core.pool.connection() as c:
        async with c.transaction():
            cur = await c.execute("SELECT active_run_id FROM demo_sessions WHERE id = %s FOR UPDATE", (s["id"],))
            locked = await cur.fetchone()
            if locked["active_run_id"]:
                cur = await c.execute("SELECT status FROM runs WHERE id = %s", (locked["active_run_id"],))
                other = await cur.fetchone()
                if other and other["status"] in ACTIVE:
                    raise ApiError(409, "run_active", "this session already has an active run; stop it first")
            await c.execute(
                """INSERT INTO runs (id, session_id, scenario, seed, rate_eps, initial_rate_eps, engines, status,
                                     max_events, created_at, replay_of, control_log, created_by, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 'starting', %s, %s, %s, %s, %s, %s)""",
                (run_id, s["id"], scenario, seed, rate, rate, engines, max_events, now, replay_of, Jsonb([]),
                 created_by, now))
            await c.execute("UPDATE demo_sessions SET active_run_id = %s WHERE id = %s", (run_id, s["id"]))
    resp = await core.internal("POST", f"{core.GENERATOR_URL}/runs/{run_id}/start")
    if resp.status_code >= 300:
        err = core.forward_error(resp)
        await fetch_one(
            """WITH r AS (UPDATE runs SET status = 'failed', error = %s, ended_at = %s, updated_at = %s
                          WHERE id = %s RETURNING session_id)
               UPDATE demo_sessions SET active_run_id = NULL WHERE id = (SELECT session_id FROM r)
                 AND active_run_id = %s RETURNING id""",
            (f"generator refused start: {err.message}", now_ms(), now_ms(), run_id, run_id))
        raise err
    row = await fetch_one("SELECT * FROM runs WHERE id = %s", (run_id,))
    out = run_out(row)
    if out["status"] == "starting":
        out["status"] = "running"
    return out


def _limits(p, s: dict, rate: float, requested_max: int | None) -> tuple[float, int]:
    if p.operator:
        cap_rate, remaining = core.EXPERIMENT_MAX_EVENTS_PER_SECOND, core.OPERATOR_MAX_EVENTS_PER_RUN
    else:
        cap_rate = float(s["rate_cap_eps"])
        remaining = s["event_cap"] - s["events_used"]
        if remaining <= 0:
            raise ApiError(429, "session_cap_reached",
                           f"this demo session used its {s['event_cap']} events; reset the session to start a new one")
    if not 0.2 <= rate <= cap_rate:
        raise ApiError(422, "invalid_rate", f"rate_eps must be between 0.2 and {cap_rate:g}")
    return rate, min(requested_max or remaining, remaining)


def _require_live_session(s: dict) -> None:
    if s["replaced_by"] or s["expires_at"] <= now_ms():
        raise ApiError(409, "session_expired", "this session has expired or was reset; create a new session")


@router.post("/runs", status_code=201)
async def create_run(body: CreateRun, request: Request):
    p = principal(request)
    if not p.session_id:
        raise ApiError(401, "auth_required", "session token required")
    s = await session_row(p.session_id)
    require_owner(p, s["id"])
    _require_live_session(s)
    limiter.check(request, "run_create", 30, 3600)
    if body.scenario not in SCENARIOS:
        raise ApiError(422, "invalid_scenario", f"scenario must be one of {list(SCENARIOS)}")
    rate, max_events = _limits(p, s, body.rate_eps, body.max_events)
    engines = sorted(set(body.engines or ["flink"])) if p.operator else ["flink"]
    seed = body.seed if body.seed is not None else secrets.randbelow(2**31 - 1) + 1
    return await _claim_and_start(p, s, scenario=body.scenario, seed=seed, rate=rate, max_events=max_events,
                                  engines=engines, replay_of=None)


@router.get("/sessions/{session_id}/runs")
async def list_runs(session_id: str, request: Request):
    await read_session(principal(request), session_id)
    rows = await fetch_all("SELECT * FROM runs WHERE session_id = %s ORDER BY created_at DESC LIMIT 100", (session_id,))
    return [run_out(r) for r in rows]


@router.get("/runs/{run_id}")
async def get_run(run_id: str, request: Request):
    r, _ = await read_run(principal(request), run_id)
    return run_out(r)


async def _control(request: Request, run_id: str, action: str, body: dict | None = None) -> dict:
    p = principal(request)
    r, s = await own_run(p, run_id)
    limiter.check(request, "run_control", 120, 60)
    if r["status"] not in ("running", "paused"):
        raise ApiError(409, "not_active", f"run is {r['status']}")
    resp = await core.internal("POST", f"{core.GENERATOR_URL}/runs/{run_id}/{action}", json=body)
    if resp.status_code >= 300:
        raise core.forward_error(resp)
    return resp.json()


@router.post("/runs/{run_id}/pause")
async def pause(run_id: str, request: Request):
    await _control(request, run_id, "pause")
    return run_out(await fetch_one("SELECT * FROM runs WHERE id = %s", (run_id,)))


@router.post("/runs/{run_id}/resume")
async def resume(run_id: str, request: Request):
    await _control(request, run_id, "resume")
    return run_out(await fetch_one("SELECT * FROM runs WHERE id = %s", (run_id,)))


@router.post("/runs/{run_id}/stop")
async def stop(run_id: str, request: Request):
    await _control(request, run_id, "stop")
    return run_out(await fetch_one("SELECT * FROM runs WHERE id = %s", (run_id,)))


class RateBody(BaseModel):
    rate_eps: float


@router.post("/runs/{run_id}/rate")
async def rate(run_id: str, body: RateBody, request: Request):
    p = principal(request)
    r, s = await own_run(p, run_id)
    cap = core.EXPERIMENT_MAX_EVENTS_PER_SECOND if p.operator else float(s["rate_cap_eps"])
    if not 0.2 <= body.rate_eps <= cap:
        raise ApiError(422, "invalid_rate", f"rate_eps must be between 0.2 and {cap:g}")
    await _control(request, run_id, "rate", {"rate_eps": body.rate_eps})
    return run_out(await fetch_one("SELECT * FROM runs WHERE id = %s", (run_id,)))


class InjectBody(BaseModel):
    pattern: str


@router.post("/runs/{run_id}/inject")
async def inject(run_id: str, body: InjectBody, request: Request):
    if body.pattern not in PATTERNS:
        raise ApiError(422, "invalid_pattern", f"pattern must be one of {list(PATTERNS)}")
    limiter.check(request, "inject", 20, 60)
    res = await _control(request, run_id, "inject", {"pattern": body.pattern})
    return {"pattern": res.get("pattern", body.pattern), "account_id": res.get("account_id"),
            "events": res.get("events"), "injection": res}


@router.post("/runs/{run_id}/replay", status_code=201)
async def replay(run_id: str, request: Request):
    p = principal(request)
    src, s = await own_run(p, run_id)
    if src["status"] in ACTIVE:
        raise ApiError(409, "run_active", "stop the run before replaying it (its control log must be final)")
    if not p.session_id:
        raise ApiError(401, "auth_required", "session token required")
    target = s if s["id"] == p.session_id else await session_row(p.session_id)
    require_owner(p, target["id"])
    _require_live_session(target)
    limiter.check(request, "run_create", 30, 3600)
    rate, max_events = _limits(p, target, float(src["initial_rate_eps"]), src["max_events"])
    engines = list(src["engines"]) if p.operator else ["flink"]
    return await _claim_and_start(p, target, scenario=src["scenario"], seed=int(src["seed"]), rate=rate,
                                  max_events=max_events, engines=engines, replay_of=src["id"])


# -- comparison & late ---------------------------------------------------------------------------
async def engine_stats(run_id: str) -> dict:
    rows = await fetch_all(
        """SELECT engine, count(*) AS count, count(*) FILTER (WHERE late) AS late,
                  count(*) FILTER (WHERE action = 'alert') AS alert, count(*) FILTER (WHERE action = 'review') AS review,
                  count(*) FILTER (WHERE action = 'allow') AS allow, count(*) FILTER (WHERE action = 'unscored') AS unscored,
                  coalesce(sum(duplicates), 0) AS duplicates,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY e2e_ms) AS p50,
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e_ms) AS p95,
                  percentile_cont(0.99) WITHIN GROUP (ORDER BY e2e_ms) AS p99,
                  count(e2e_ms) AS n, min(persisted_at) AS first_persisted_at, max(persisted_at) AS last_persisted_at
           FROM scored_events WHERE run_id = %s GROUP BY engine""", (run_id,))
    out = {}
    for r in rows:
        out[r["engine"]] = {
            "count": r["count"], "late": r["late"],
            "actions": {k: r[k] for k in ("alert", "review", "allow", "unscored")},
            "duplicates": int(r["duplicates"]),
            "latency_ms": {k: None if r[k] is None else round(float(r[k]), 1) for k in ("p50", "p95", "p99")} | {"n": r["n"]},
            "first_persisted_at": r["first_persisted_at"], "last_persisted_at": r["last_persisted_at"],
        }
    return out


@router.get("/runs/{run_id}/comparison")
async def comparison(run_id: str, request: Request):
    await read_run(principal(request), run_id)
    m = await fetch_one(
        """WITH f AS (SELECT event_id, action, late, detail->'raw_features' AS raw FROM scored_events
                      WHERE run_id = %(r)s AND engine = 'flink'),
                s AS (SELECT event_id, action, late, detail->'raw_features' AS raw FROM scored_events
                      WHERE run_id = %(r)s AND engine = 'spark')
           SELECT count(*) FILTER (WHERE f.event_id IS NOT NULL AND s.event_id IS NOT NULL) AS both,
                  count(*) FILTER (WHERE s.event_id IS NULL) AS only_flink,
                  count(*) FILTER (WHERE f.event_id IS NULL) AS only_spark,
                  count(*) FILTER (WHERE f.action = s.action) AS action_agree,
                  count(*) FILTER (WHERE f.action <> s.action) AS action_disagree,
                  count(*) FILTER (WHERE f.raw IS DISTINCT FROM s.raw AND f.event_id IS NOT NULL AND s.event_id IS NOT NULL)
                    AS raw_feature_mismatch,
                  count(*) FILTER (WHERE f.late <> s.late) AS late_disagree
           FROM f FULL OUTER JOIN s ON f.event_id = s.event_id""", {"r": run_id})
    dis = await fetch_all(
        """SELECT coalesce(f.event_id, s.event_id) AS event_id, f.action AS flink_action, s.action AS spark_action,
                  f.late AS flink_late, s.late AS spark_late
           FROM (SELECT * FROM scored_events WHERE run_id = %(r)s AND engine = 'flink') f
           FULL OUTER JOIN (SELECT * FROM scored_events WHERE run_id = %(r)s AND engine = 'spark') s
             ON f.event_id = s.event_id
           WHERE f.event_id IS NULL OR s.event_id IS NULL OR f.action <> s.action OR f.late <> s.late
           ORDER BY coalesce(f.seq, s.seq) LIMIT 100""", {"r": run_id})
    return {"run_id": run_id, "engines": await engine_stats(run_id), "matching": dict(m), "disagreements": dis}


@router.get("/runs/{run_id}/late")
async def late(run_id: str, request: Request):
    r, _ = await read_run(principal(request), run_id)
    out = {}
    for engine in r["engines"]:
        cnt = await fetch_one("SELECT count(*) AS n FROM scored_events WHERE run_id = %s AND engine = %s AND late",
                              (run_id, engine))
        ex = await fetch_all(
            """SELECT event_id, event_time, watermark_ms, watermark_ms - event_time AS lag_ms, pattern
               FROM scored_events WHERE run_id = %s AND engine = %s AND late ORDER BY seq LIMIT 25""", (run_id, engine))
        dropped = None
        if engine == "spark":
            row = await fetch_one(
                """SELECT sum((op->>'numRowsDroppedByWatermark')::bigint) AS n
                   FROM spark_progress sp, jsonb_array_elements(sp.progress->'stateOperators') op
                   WHERE sp.at BETWEEN %s AND %s""",
                (r["started_at"] or 0, (r["ended_at"] or now_ms()) + 120_000))
            dropped = int(row["n"]) if row and row["n"] is not None else None
        out[engine] = {"late": cnt["n"], "windows_late_dropped": dropped, "examples": ex,
                       "windows_late_dropped_note": None if dropped is not None else
                       "late records are excluded from windows by policy; the engine-side drop counter is not recorded for this run"}
    return {"engines": out}
