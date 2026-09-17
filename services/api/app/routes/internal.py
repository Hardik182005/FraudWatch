"""Internal lifecycle routes used by the Cloud Run controller (never by browsers).

Protected by the shared INTERNAL_API_TOKEN (x-internal-token). The controller reaches the API over
private VPC egress, so these routes are not reachable without both network access and the token.
"""
from __future__ import annotations

import hmac
import secrets

from fastapi import APIRouter, Request
from psycopg.types.json import Jsonb

from .. import core
from ..core import ApiError, fetch_all, fetch_one, now_ms

router = APIRouter(prefix="/internal", include_in_schema=False)
EXPERIMENT_LEASE_MS = 60 * 60 * 1000   # an experiment older than this no longer blocks idle shutdown
RUN_LEASE_MS = 30 * 60 * 1000          # same for a demo run (bounded runs finish far sooner)


def _require_internal(request: Request) -> None:
    got = request.headers.get("x-internal-token", "")
    if not core.INTERNAL_TOKEN or not hmac.compare_digest(got, core.INTERNAL_TOKEN):
        raise ApiError(403, "internal_only", "internal route")


@router.get("/active-work")
async def active_work(request: Request):
    """Work that must finish (or drain) before the VM may stop. Bounded by leases, never unlimited."""
    _require_internal(request)
    now = now_ms()
    runs = await fetch_all(
        """SELECT id, status, created_by, events_sent, max_events, created_at FROM runs
           WHERE status IN ('starting', 'running', 'paused') AND created_at > %s""", (now - RUN_LEASE_MS,))
    exps = await fetch_all(
        """SELECT id, kind, status, created_at FROM experiments
           WHERE status IN ('queued', 'running') AND created_at > %s""", (now - EXPERIMENT_LEASE_MS,))
    # results still arriving for recently finished runs = in-flight processing that should drain
    draining = await fetch_one(
        """SELECT count(*) AS n FROM runs r
           WHERE r.status IN ('completed', 'stopped') AND r.ended_at > %s
             AND r.events_sent > (SELECT count(*) FROM scored_events e WHERE e.run_id = r.id)""", (now - 120_000,))
    return {"as_of": now, "runs": runs, "experiments": exps, "draining_runs": int((draining or {}).get("n") or 0),
            "busy": bool(runs or exps or (draining or {}).get("n"))}


@router.post("/drain")
async def drain(request: Request):
    """Stop visitor/operator demo generation before shutdown and record the disposition. Experiments are left
    to finish or hit their lease; the controller does not stop the VM while they are active."""
    _require_internal(request)
    rows = await fetch_all("SELECT id FROM runs WHERE status IN ('starting', 'running', 'paused') AND created_by <> 'experiment'")
    stopped, failed = [], []
    for r in rows:
        resp = await core.internal("POST", f"{core.GENERATOR_URL}/runs/{r['id']}/stop")
        (stopped if resp.status_code < 300 else failed).append(r["id"])
        await fetch_one(
            """UPDATE runs SET control_log = coalesce(control_log, '[]'::jsonb) || %s, updated_at = %s
               WHERE id = %s RETURNING id""",
            (Jsonb([{"at": now_ms(), "action": "stop", "by": "lifecycle-idle-shutdown"}]), now_ms(), r["id"]))
    return {"stopped": stopped, "failed": failed}


@router.post("/probe")
async def probe_start(request: Request):
    """Start a tiny labelled end-to-end probe run (5 events, Flink) in a dedicated operator session."""
    _require_internal(request)
    now = now_ms()
    sid = "s_probe_" + secrets.token_hex(6)
    run_id = "r_" + secrets.token_hex(8)
    await fetch_one(
        """INSERT INTO demo_sessions (id, label, kind, created_at, expires_at, event_cap, rate_cap_eps, stream_updates)
           VALUES (%s, 'lifecycle-readiness-probe', 'operator', %s, %s, 5, 5, false) RETURNING id""",
        (sid, now, now + 3_600_000))
    await fetch_one(
        """INSERT INTO runs (id, session_id, scenario, seed, rate_eps, initial_rate_eps, engines, status, max_events,
                             created_at, control_log, created_by, updated_at)
           VALUES (%s, %s, 'normal', 42, 5, 5, %s, 'starting', 5, %s, %s, 'operator', %s) RETURNING id""",
        (run_id, sid, ["flink"], now, Jsonb([]), now))
    await fetch_one("UPDATE demo_sessions SET active_run_id = %s WHERE id = %s RETURNING id", (run_id, sid))
    resp = await core.internal("POST", f"{core.GENERATOR_URL}/runs/{run_id}/start")
    if resp.status_code >= 300:
        await fetch_one("UPDATE runs SET status = 'failed', error = 'probe start refused', ended_at = %s WHERE id = %s RETURNING id",
                        (now_ms(), run_id))
        raise ApiError(503, "probe_start_failed", "generator refused the readiness probe")
    return {"run_id": run_id, "session_id": sid, "started_at": now}


@router.get("/probe/{run_id}")
async def probe_status(run_id: str, request: Request):
    _require_internal(request)
    row = await fetch_one(
        """SELECT r.status, r.events_sent,
                  (SELECT count(*) FROM scored_events e WHERE e.run_id = r.id AND e.engine = 'flink') AS persisted,
                  (SELECT max(e2e_ms) FROM scored_events e WHERE e.run_id = r.id AND e.engine = 'flink') AS max_e2e_ms
           FROM runs r WHERE r.id = %s""", (run_id,))
    if row is None:
        raise ApiError(404, "not_found", "probe run not found")
    return {"run_id": run_id, "status": row["status"], "events_sent": row["events_sent"],
            "persisted": int(row["persisted"] or 0), "max_e2e_ms": row["max_e2e_ms"], "ok": int(row["persisted"] or 0) >= 1}
