"""FraudWatch ops-agent (internal service, port 8003).

Two jobs, both of which need the Docker socket and therefore cannot live in the API:

* **Experiment queue.** One worker claims `experiments` rows with status `queued` (``FOR UPDATE SKIP
  LOCKED``), runs them, and writes the measured results back. Experiments run strictly one at a time:
  two concurrent benchmarks on one host would measure each other rather than the engines.
* **Spark on demand.** `/spark/start` and `/spark/stop` start and stop the Compose-managed `spark`
  container, so a comparison can bring the second engine up only when it is needed.

Every route except `/healthz` requires `x-internal-token`, matching the other internal services. The
service is reachable only on the Compose network; nothing here is exposed publicly.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from fwsvc import now_ms, settings
from fwsvc.logs import install as install_logs

from .dockerapi import ContainerNotFound, Docker, DockerUnavailable
from .experiments import KINDS, Experiments
from .runs import Cancelled, RunFailed
from .store import Store

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("ops-agent")

INTERNAL_TOKEN = os.getenv("INTERNAL_API_TOKEN", "")
DOCKER_SOCKET = os.getenv("DOCKER_SOCKET_PATH", "/var/run/docker.sock")
GENERATOR_URL = os.getenv("GENERATOR_URL", "http://generator:8002")
FLINK_REST_URL = os.getenv("FLINK_REST_URL", "http://flink-jobmanager:8081")
SPARK_TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "1 second")
POLL_S = settings.env_float("OPS_AGENT_POLL_SECONDS", 3.0)
# An experiment left 'running' for longer than this is assumed to belong to an agent that died.
STALE_EXPERIMENT_MS = settings.env_int("OPS_AGENT_STALE_MINUTES", 90) * 60_000
REPLAY_PACK_DIR = Path(os.getenv("REPLAY_PACK_DIR", "/models/replay/ieee-test-replay-1.0"))
SERVICES = {
    "flink": os.getenv("FLINK_TASKMANAGER_SERVICE", "flink-taskmanager"),
    "flink_jobmanager": os.getenv("FLINK_JOBMANAGER_SERVICE", "flink-jobmanager"),
    "flink_taskmanager": os.getenv("FLINK_TASKMANAGER_SERVICE", "flink-taskmanager"),
    "spark": os.getenv("SPARK_SERVICE", "spark"),
}

state: dict = {"store": None, "docker": None, "experiments": None, "runner": None, "worker": None, "logs": None,
               "started_at": now_ms(), "current": None, "completed": 0, "failed": 0}


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


async def worker_loop() -> None:
    """Claim and run one queued experiment at a time, forever."""
    store: Store = state["store"]
    experiments: Experiments = state["experiments"]
    stale = await store.requeue_stale(STALE_EXPERIMENT_MS)
    if stale:
        log.warning("failed %d experiment(s) left running by a previous agent: %s", len(stale), ", ".join(stale))
    while True:
        exp = None
        try:
            exp = await store.claim()
        except Exception:
            log.exception("could not claim an experiment; retrying")
        if exp is None:
            await asyncio.sleep(POLL_S)
            continue
        state["current"] = exp["id"]
        log.info("running experiment %s (%s)", exp["id"], exp["kind"])
        try:
            await run_experiment(exp)
            state["completed"] += 1
        except Exception:
            state["failed"] += 1
            log.exception("experiment %s failed unexpectedly", exp["id"])
        finally:
            state["current"] = None


async def run_experiment(exp: dict) -> None:
    store: Store = state["store"]
    experiments: Experiments = state["experiments"]
    kind = exp["kind"]
    if kind not in KINDS:
        await store.finish(exp["id"], "failed", error=f"unknown experiment kind '{kind}'")
        return
    session_id = await store.experiment_session(exp["id"])
    await store.progress(exp["id"], phase="starting", message="preparing", step=0, total_steps=1)
    try:
        environment = await experiments.environment(["flink", "spark"])
        results = await getattr(experiments, kind)(exp, session_id)
    except Cancelled:
        await store.finish(exp["id"], "cancelled", error="cancelled by the operator")
        log.info("experiment %s cancelled", exp["id"])
        return
    except (RunFailed, ValueError) as exc:
        await store.finish(exp["id"], "failed", error=str(exc)[:1000])
        log.warning("experiment %s failed: %s", exp["id"], exc)
        return
    except Exception as exc:  # unexpected: record the type, never a fabricated result
        await store.finish(exp["id"], "failed", error=f"{type(exc).__name__}: {exc}"[:1000])
        raise
    await store.progress(exp["id"], phase="done", message="complete")
    await store.finish(exp["id"], "completed", results=results, environment=environment)
    log.info("experiment %s completed", exp["id"])


async def load_replay_labels() -> None:
    """Load the replay pack's evaluation-only labels once, if they are not already loaded.

    Migration 004 reserves ``replay_labels`` for exactly this. The labels are ground truth for the
    offline audit; they are never read by scoring, never published to Kafka and never returned on
    the live feed.
    """
    store: Store = state["store"]
    path = REPLAY_PACK_DIR / "labels.json"
    manifest = REPLAY_PACK_DIR / "manifest.json"
    if not path.exists():
        log.info("no replay labels at %s; skipping the label load", path)
        return
    try:
        pack_id = json.loads(manifest.read_text(encoding="utf-8"))["pack_id"] if manifest.exists()             else REPLAY_PACK_DIR.name
        existing = await store.label_count(pack_id)
        if existing:
            log.info("replay labels already loaded for %s (%d rows)", pack_id, existing)
            return
        labels = json.loads(path.read_text(encoding="utf-8"))
        n = await store.load_labels(pack_id, labels)
        log.info("loaded %d evaluation labels for %s", n, pack_id)
    except Exception:
        log.exception("could not load replay labels; the offline audit will have no ground truth")



@asynccontextmanager
async def lifespan(app: FastAPI):
    from .runs import Runner

    state["logs"] = install_logs(settings.DATABASE_URL, "ops_agent")
    store = Store(settings.DATABASE_URL)
    docker = Docker(DOCKER_SOCKET)
    runner = Runner(store, GENERATOR_URL, INTERNAL_TOKEN)
    state.update({
        "store": store, "docker": docker, "runner": runner,
        "experiments": Experiments(store, runner, docker, flink_rest=FLINK_REST_URL,
                                   spark_trigger=SPARK_TRIGGER_INTERVAL, engine_containers=SERVICES),
    })
    if not await docker.ping():
        log.warning("the Docker socket at %s is not reachable: Spark start/stop and fault injection "
                    "will fail until it is mounted", DOCKER_SOCKET)
    await load_replay_labels()
    state["worker"] = asyncio.create_task(worker_loop())
    try:
        yield
    finally:
        state["worker"].cancel()
        await asyncio.gather(state["worker"], return_exceptions=True)
        await runner.aclose()
        await state["experiments"].aclose()
        await docker.aclose()


app = FastAPI(title="FraudWatch ops-agent", version="1.0.0", lifespan=lifespan, docs_url=None, redoc_url=None)


@app.middleware("http")
async def internal_only(request: Request, call_next):
    if request.url.path != "/healthz" and INTERNAL_TOKEN:
        if request.headers.get("x-internal-token") != INTERNAL_TOKEN:
            return _error(401, "unauthorized", "internal token required")
    return await call_next(request)


@app.get("/healthz")
async def healthz():
    docker: Docker = state["docker"]
    return {"status": "ok", "uptime_ms": now_ms() - state["started_at"],
            "docker": await docker.ping(), "current_experiment": state["current"],
            "completed": state["completed"], "failed": state["failed"]}


class SparkStart(BaseModel):
    session_id: str | None = None
    run_id: str | None = None
    mode: str = "live"


@app.post("/spark/start")
async def spark_start(body: SparkStart):
    """Start the Compose-managed Spark container.

    The container's Kafka offsets and filters come from its Compose environment, which this call
    cannot change: starting an existing container reuses the environment it was created with. The
    response therefore reports what actually happened rather than echoing the request back.
    """
    docker: Docker = state["docker"]
    service = SERVICES["spark"]
    try:
        current = await docker.state(service)
        if current == "running":
            return {"service": service, "state": "running", "action": "already_running"}
        await docker.start(service)
    except ContainerNotFound:
        return _error(409, "spark_not_created",
                      f"no container for the '{service}' service. Create it once with "
                      f"`docker compose --profile spark up -d {service}`; after that this endpoint "
                      f"can start and stop it.")
    except DockerUnavailable as exc:
        return _error(503, "docker_unavailable", str(exc))
    # Spark needs roughly a minute to reach its first micro-batch; the caller reads readiness from
    # /health, which is driven by the Spark heartbeat, not from this response.
    return {"service": service, "state": await docker.state(service), "action": "started",
            "note": "Spark reports 'running' in /health once its first micro-batch completes."}


@app.post("/spark/stop")
async def spark_stop():
    docker: Docker = state["docker"]
    service = SERVICES["spark"]
    try:
        current = await docker.state(service)
        if current != "running":
            return {"service": service, "state": current, "action": "already_stopped"}
        await docker.stop(service)
    except ContainerNotFound:
        return _error(409, "spark_not_created", f"no container for the '{service}' service")
    except DockerUnavailable as exc:
        return _error(503, "docker_unavailable", str(exc))
    return {"service": service, "state": await docker.state(service), "action": "stopped"}


@app.get("/status")
async def status():
    docker: Docker = state["docker"]
    engines = {}
    for name in ("flink_jobmanager", "flink_taskmanager", "spark"):
        try:
            engines[name] = await docker.state(SERVICES[name])
        except (DockerUnavailable, ContainerNotFound):
            engines[name] = None
    return {"current_experiment": state["current"], "completed": state["completed"],
            "failed": state["failed"], "containers": engines}
