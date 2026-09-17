"""Component health, measured live (cached for a few seconds so SSE fan-out doesn't multiply probes)."""
from __future__ import annotations

import asyncio
import time

from confluent_kafka.admin import AdminClient

from fwsvc import settings

from . import core
from .core import fetch_one, now_ms

HEARTBEAT_STALE_MS = 20_000
CACHE_S = 5.0
_cache: dict = {"at": 0.0, "value": None}
_lock = asyncio.Lock()
_admin: AdminClient | None = None
STARTED_AT = now_ms()


def _component(cid: str, status: str, detail: str | None, metrics: dict | None = None) -> dict:
    return {"id": cid, "status": status, "detail": detail, "checked_at": now_ms(), "metrics": metrics or {}}


async def _postgres() -> dict:
    t0 = time.perf_counter()
    try:
        await asyncio.wait_for(fetch_one("SELECT 1 AS ok"), 3)
        return _component("postgres", "ok", "query ok", {"query_ms": round((time.perf_counter() - t0) * 1000, 1)})
    except Exception as exc:
        return _component("postgres", "down", f"query failed: {type(exc).__name__}")


def _kafka_sync() -> dict:
    global _admin
    if _admin is None:
        _admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS, "socket.timeout.ms": 3000})
    t0 = time.perf_counter()
    md = _admin.list_topics(timeout=3)
    topics = {name: len(t.partitions) for name, t in md.topics.items() if name.startswith("fraudwatch.")}
    missing = [t for t in (settings.TOPIC_TRANSACTIONS, *settings.TOPIC_RESULTS.values()) if t not in topics]
    return _component("kafka", "degraded" if missing else "ok",
                      f"missing topics: {', '.join(missing)}" if missing else f"{len(md.brokers)} broker, {len(topics)} topics",
                      {"brokers": len(md.brokers), "topics": topics, "metadata_ms": round((time.perf_counter() - t0) * 1000, 1)})


async def _kafka() -> dict:
    try:
        return await asyncio.wait_for(asyncio.get_running_loop().run_in_executor(None, _kafka_sync), 5)
    except Exception as exc:
        return _component("kafka", "down", f"metadata request failed: {type(exc).__name__}")


async def _scoring() -> dict:
    try:
        r = await core.http.get(f"{core.SCORING_SERVICE_URL}/healthz", timeout=3)
        body = r.json()
        return _component("scoring", "ok" if r.status_code == 200 else "degraded",
                          f"model {body.get('model_version')}", {k: body.get(k) for k in ("model_version", "requests", "items", "errors")})
    except Exception as exc:
        return _component("scoring", "down", f"unreachable: {type(exc).__name__}")


async def _heartbeat(cid: str, row_id: str) -> dict:
    row = await fetch_one("SELECT * FROM component_heartbeats WHERE id = %s", (row_id,))
    if row is None:
        return _component(cid, "unknown", "no heartbeat recorded yet")
    age = now_ms() - row["checked_at"]
    status = row["status"] if age <= HEARTBEAT_STALE_MS else "down"
    detail = row["detail"] if age <= HEARTBEAT_STALE_MS else f"last heartbeat {age // 1000} s ago"
    return {"id": cid, "status": status, "detail": detail, "checked_at": row["checked_at"], "metrics": row["metrics"]}


async def _flink() -> dict:
    base = core.FLINK_REST_URL
    try:
        jobs = (await core.http.get(f"{base}/jobs/overview", timeout=3)).json().get("jobs", [])
    except Exception as exc:
        return _component("flink", "down", f"JobManager REST unreachable: {type(exc).__name__}")
    running = [j for j in jobs if j.get("state") == "RUNNING"]
    job = (running or sorted(jobs, key=lambda j: j.get("start-time", 0), reverse=True) or [None])[0]
    if job is None:
        return _component("flink", "degraded", "cluster up, no job submitted", {"job_id": None, "job_state": None})
    jid = job["jid"]
    metrics = {"job_id": jid, "job_name": job.get("name"), "job_state": job.get("state"),
               "uptime_ms": job.get("duration"), "restarts": None, "last_checkpoint": None}
    try:
        cp = (await core.http.get(f"{base}/jobs/{jid}/checkpoints", timeout=3)).json()
        done = (cp.get("latest") or {}).get("completed")
        if done:
            metrics["last_checkpoint"] = {"id": done.get("id"), "completed_at": done.get("latest_ack_timestamp"),
                                          "duration_ms": done.get("end_to_end_duration"),
                                          "state_size": done.get("state_size", done.get("checkpointed_size"))}
        metrics["checkpoint_counts"] = cp.get("counts")
        m = (await core.http.get(f"{base}/jobs/{jid}/metrics", params={"get": "numRestarts"}, timeout=3)).json()
        if m:
            metrics["restarts"] = int(float(m[0]["value"]))
    except Exception:
        pass
    status = "ok" if job.get("state") == "RUNNING" else "degraded"
    return _component("flink", status, f"job {job.get('state', '?').lower()}", metrics)


async def _spark() -> dict:
    hb = await fetch_one("SELECT * FROM component_heartbeats WHERE id = 'spark'")
    last = await fetch_one("SELECT batch_id, at, run_label FROM spark_progress ORDER BY id DESC LIMIT 1")
    fresh = hb is not None and now_ms() - hb["checked_at"] <= HEARTBEAT_STALE_MS
    state = (hb["metrics"] or {}).get("state", "idle") if fresh else "idle"
    metrics = {"state": state, "app_id": (hb["metrics"] or {}).get("app_id") if fresh else None,
               "run_label": (hb["metrics"] or {}).get("run_label") if fresh else (last or {}).get("run_label"),
               "last_batch_id": (last or {}).get("batch_id"), "last_batch_at": (last or {}).get("at")}
    if fresh and hb["status"] not in ("ok", "idle"):
        return {"id": "spark", "status": hb["status"], "detail": hb["detail"], "checked_at": hb["checked_at"],
                "metrics": metrics}
    if state == "running":
        return _component("spark", "ok", hb["detail"] or "structured streaming query running", metrics)
    return _component("spark", "idle", "Spark runs on demand for comparisons (operator-launched)", metrics)


async def _compute() -> dict:
    pg, kafka, scoring, gen, rc, flink, spark = await asyncio.gather(
        _postgres(), _kafka(), _scoring(), _heartbeat("generator", "generator"),
        _heartbeat("result_consumer", "result_consumer"), _flink(), _spark())
    api = _component("api", "ok", "serving", {"uptime_ms": now_ms() - STARTED_AT})
    components = [api, pg, kafka, gen, scoring, rc, flink, spark]
    if any(c["status"] == "down" for c in (pg, kafka)):
        overall = "down"
    elif any(c["status"] not in ("ok", "idle") for c in (gen, scoring, rc, flink)) or spark["status"] in ("down", "degraded"):
        overall = "degraded"
    else:
        overall = "ok"
    return {"as_of": now_ms(), "overall": overall, "components": components}


async def health_snapshot(max_age_s: float = CACHE_S) -> dict:
    if _cache["value"] is not None and time.monotonic() - _cache["at"] < max_age_s:
        return _cache["value"]
    async with _lock:
        if _cache["value"] is None or time.monotonic() - _cache["at"] >= max_age_s:
            _cache["value"] = await _compute()
            _cache["at"] = time.monotonic()
    return _cache["value"]
