"""FraudWatch generator (internal service, port 8002).

Owns the Kafka producer. The API creates the run row, then calls POST /runs/{id}/start; this
service reads the run from Postgres (single source of truth) and produces events. Heartbeats go
to every partition of the transactions topic once per second so event-time watermarks advance.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from confluent_kafka import Producer
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from fwcore import SCHEMA_VERSION
from fwcore.simulation import PATTERNS
from fwsvc import now_ms, settings
from fwsvc.db import migrate

from .runner import Run, RunSpec
from .store import Store

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("generator")

PROFILE_PATH = Path(os.getenv("GENERATOR_PROFILE_PATH", "/models/fw-ieee-stream-1.0.0/generator_profile.json"))
VISITOR_MAX_EPS = settings.env_float("DEMO_MAX_EVENTS_PER_SECOND", 50)
EXPERIMENT_MAX_EPS = settings.env_float("EXPERIMENT_MAX_EVENTS_PER_SECOND", 1000)
MAX_TOTAL_EPS = settings.env_float("GENERATOR_MAX_TOTAL_EPS", 1200)
MAX_ACTIVE_RUNS = settings.env_int("GENERATOR_MAX_ACTIVE_RUNS", 25)
INTERNAL_TOKEN = os.getenv("INTERNAL_API_TOKEN", "")
GENERATOR_ID = os.getenv("HOSTNAME", "generator")

state: dict = {"runs": {}, "store": None, "producer": None, "partitions": 0, "heartbeats": 0, "hb_errors": 0,
               "started_at": now_ms()}


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


async def heartbeat_loop() -> None:
    producer: Producer = state["producer"]
    topic = settings.TOPIC_TRANSACTIONS
    last_meta = 0.0
    while True:
        try:
            if time.monotonic() - last_meta > 30 or not state["partitions"]:
                md = await asyncio.get_running_loop().run_in_executor(None, lambda: producer.list_topics(topic, timeout=5))
                t = md.topics.get(topic)
                if t is not None and t.error is None:
                    state["partitions"] = len(t.partitions)
                last_meta = time.monotonic()
            now = now_ms()
            beat = json.dumps({"schema_version": SCHEMA_VERSION, "kind": "heartbeat", "event_time": now,
                               "produced_at": now, "generator_id": GENERATOR_ID}, separators=(",", ":"))
            for p in range(state["partitions"]):
                producer.produce(topic, value=beat, partition=p)
            producer.poll(0)
            state["heartbeats"] += 1
        except Exception as exc:
            state["hb_errors"] += 1
            log.warning("heartbeat failed: %s", exc)
        await asyncio.sleep(1.0)


async def status_loop() -> None:
    store: Store = state["store"]
    while True:
        await asyncio.sleep(5)
        active = [r for r in state["runs"].values() if r.status in ("running", "paused")]
        try:
            for r in active:
                await store.progress(r, publish=True)
            ok = state["partitions"] > 0
            await store.heartbeat("ok" if ok else "degraded",
                                  f"{len(active)} active runs" if ok else "no partition metadata from Kafka",
                                  {"active_runs": len(active), "heartbeats_sent": state["heartbeats"],
                                   "heartbeat_errors": state["hb_errors"], "partitions": state["partitions"],
                                   "offered_eps": round(sum(r.stream.rate_eps for r in active if r.status == "running"), 2)})
        except Exception as exc:
            log.warning("status update failed: %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    migrate(settings.DATABASE_URL)
    state["profile"] = json.loads(PROFILE_PATH.read_text())
    state["store"] = Store(settings.DATABASE_URL)
    failed = state["store"].fail_orphans()
    if failed:
        log.warning("marked %d orphaned runs as failed", failed)
    state["producer"] = Producer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "fw-generator",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 2,
    })
    tasks = [asyncio.create_task(heartbeat_loop()), asyncio.create_task(status_loop())]
    yield
    for r in list(state["runs"].values()):
        if r.task and not r.task.done():
            r.task.cancel()
    for t in tasks:
        t.cancel()
    await asyncio.sleep(0.2)
    state["producer"].flush(10)


app = FastAPI(title="FraudWatch generator", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def internal_auth(request: Request, call_next):
    if INTERNAL_TOKEN and request.url.path != "/healthz" and request.headers.get("x-internal-token") != INTERNAL_TOKEN:
        return _error(401, "unauthorized", "internal token required")
    return await call_next(request)


@app.get("/healthz")
def healthz():
    active = [r for r in state["runs"].values() if r.status in ("running", "paused")]
    return {"status": "ok" if state["partitions"] else "degraded", "partitions": state["partitions"],
            "active_runs": len(active), "heartbeats_sent": state["heartbeats"], "heartbeat_errors": state["hb_errors"]}


def _active_runs() -> list[Run]:
    return [r for r in state["runs"].values() if r.status in ("starting", "running", "paused")]


@app.post("/runs/{run_id}/start")
async def start(run_id: str):
    if run_id in state["runs"] and state["runs"][run_id].status in ("starting", "running", "paused"):
        return _error(409, "already_running", "run already active")
    store: Store = state["store"]
    row = await store._run(lambda c: c.execute("SELECT * FROM runs WHERE id = %s", (run_id,)).fetchone())
    if row is None:
        return _error(404, "not_found", "run not found")
    if row["status"] != "starting":
        return _error(409, "bad_state", f"run is {row['status']}")
    active = _active_runs()
    if len(active) >= MAX_ACTIVE_RUNS:
        return _error(429, "capacity", "too many active runs on the shared demo; try again shortly")
    cap = VISITOR_MAX_EPS if row["created_by"] == "visitor" else EXPERIMENT_MAX_EPS
    rate = min(float(row["rate_eps"]), cap)
    offered = sum(r.stream.rate_eps for r in active)
    if offered + rate > MAX_TOTAL_EPS:
        return _error(429, "capacity", f"shared generator capacity ({MAX_TOTAL_EPS:g} events/s) is in use")
    replay_log = []
    if row["replay_of"]:
        src = await store._run(lambda c: c.execute("SELECT control_log FROM runs WHERE id = %s", (row["replay_of"],)).fetchone())
        replay_log = (src or {}).get("control_log") or []
    spec = RunSpec(run_id=row["id"], session_id=row["session_id"], scenario=row["scenario"], seed=int(row["seed"]),
                   rate_eps=rate, max_events=int(row["max_events"]), engines=list(row["engines"]), control_log=replay_log)
    run = Run(spec, state["profile"], state["producer"], settings.TOPIC_TRANSACTIONS, store, cap)
    state["runs"][run_id] = run
    run.task = asyncio.create_task(run.run())
    return {"run_id": run_id, "status": "running", "rate_eps": rate}


def _get(run_id: str) -> Run | None:
    return state["runs"].get(run_id)


@app.post("/runs/{run_id}/{action}")
async def control(run_id: str, action: str, request: Request):
    run = _get(run_id)
    if run is None or run.status not in ("running", "paused"):
        return _error(409, "not_active", "run is not active on this generator")
    body = {}
    if action in ("rate", "inject"):
        try:
            body = await request.json()
        except Exception:
            return _error(422, "invalid_json", "JSON body required")
    result: dict = {}
    if action == "pause":
        run.pause()
    elif action == "resume":
        run.resume()
    elif action == "stop":
        run.status = "stopped"
    elif action == "rate":
        rate = body.get("rate_eps")
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not 0.2 <= rate <= run.max_rate:
            return _error(422, "invalid_rate", f"rate_eps must be between 0.2 and {run.max_rate:g}")
        others = sum(r.stream.rate_eps for r in _active_runs() if r is not run)
        if others + rate > MAX_TOTAL_EPS:
            return _error(429, "capacity", f"shared generator capacity ({MAX_TOTAL_EPS:g} events/s) is in use")
        run.set_rate(float(rate))
    elif action == "inject":
        pattern = body.get("pattern")
        if pattern not in PATTERNS:
            return _error(422, "invalid_pattern", f"pattern must be one of {list(PATTERNS)}")
        result = run.inject(pattern)
    else:
        return _error(404, "not_found", "unknown action")
    await state["store"].progress(run, publish=True)
    return result or {"run_id": run_id, "status": run.status, "rate_eps": run.stream.rate_eps,
                      "events_sent": run.events_sent}


@app.get("/runs")
def runs():
    return [{"run_id": r.spec.run_id, "session_id": r.spec.session_id, "status": r.status, "events_sent": r.events_sent,
             "rate_eps": r.stream.rate_eps, "delivery_errors": r.delivery_errors} for r in state["runs"].values()]
