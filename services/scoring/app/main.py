"""FraudWatch scoring service (internal). Contract: docs/contracts.md section 4."""
from __future__ import annotations

import logging
import os
import time
from collections import deque
from pathlib import Path

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from fwcore.schema import EventValidationError, validate_event

from .model import Model, RawFeatureError, normalise_raw

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s scoring %(message)s")
log = logging.getLogger("scoring")

ARTIFACT = Path(os.getenv("MODEL_ARTIFACT_PATH", "/models/fw-ieee-stream-1.1.0"))
MAX_ITEMS = 500

state: dict = {"model": None, "error": None, "requests": 0, "items": 0, "errors": 0,
               "lat_ms": deque(maxlen=2000), "sink": None, "last_summary": time.monotonic(), "summary_base": (0, 0, 0)}


def _sink():
    """Structured logs go to Postgres only when DATABASE_URL is configured (optional for this service)."""
    if state["sink"] is None and os.getenv("DATABASE_URL"):
        try:
            from fwsvc.logs import install
            state["sink"] = install(os.environ["DATABASE_URL"], "scoring")
        except Exception as exc:  # logging must never stop scoring
            log.warning("structured log sink unavailable: %s", type(exc).__name__)
            state["sink"] = False
    return state["sink"] or None


def _log(severity: str, message: str, **fields) -> None:
    s = _sink()
    if s is not None:
        s.emit(severity, message, **fields)


def _pct(values: list, q: float):
    if not values:
        return None
    v = sorted(values)
    return round(v[min(len(v) - 1, int(round(q * (len(v) - 1))))], 3)


def _maybe_summary() -> None:
    """One info row per minute with measured inference timing (no row when nothing was scored)."""
    now = time.monotonic()
    if now - state["last_summary"] < 60:
        return
    req0, items0, err0 = state["summary_base"]
    state["last_summary"] = now
    state["summary_base"] = (state["requests"], state["items"], state["errors"])
    if state["requests"] == req0:
        return
    lat = [x for x, _ in state["lat_ms"]]
    _log("info", f"scored {state['items'] - items0} items in {state['requests'] - req0} requests "
                 f"({state['errors'] - err0} rejected); inference p50 {_pct(lat, 0.5)} ms",
         stage="scored", status="summary", payload={"pid": os.getpid(), "inference_ms_p50": _pct(lat, 0.5),
                                                    "inference_ms_p95": _pct(lat, 0.95), "samples": len(lat)})


def load_model() -> None:
    try:
        state["model"] = Model(ARTIFACT)
        log.info("loaded model %s from %s", state["model"].version, ARTIFACT)
        _log("info", f"model {state['model'].version} loaded", stage="lifecycle", status="loaded",
             payload={"pid": os.getpid(), "artifact": ARTIFACT.name})
    except Exception as exc:  # surfaced through /healthz rather than crashing silently
        state["error"] = f"{type(exc).__name__}: {exc}"
        log.exception("model load failed")
        _log("critical", "model load failed", stage="lifecycle", status="failed", error_code="model_load_failed",
             payload={"error": state["error"]})


@asynccontextmanager
async def lifespan(_: FastAPI):
    load_model()
    yield


app = FastAPI(title="FraudWatch scoring", version="1.0.0", lifespan=lifespan)


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


@app.get("/healthz")
def healthz():
    m: Model | None = state["model"]
    if m is None:
        return _error(503, "model_not_loaded", state["error"] or "model loading")
    lat = [x for x, _ in state["lat_ms"]]
    return {"status": "ok", "model_version": m.version, "loaded_at": m.loaded_at,
            "requests": state["requests"], "items": state["items"], "errors": state["errors"], "pid": os.getpid(),
            "inference_ms": {"p50": _pct(lat, 0.5), "p95": _pct(lat, 0.95), "n": len(lat),
                             "boundary": "Model.score() wall time per request (batch), this worker process, "
                                         "last <=2000 requests"} if lat else None}


@app.get("/v1/model")
def model_metadata():
    m: Model | None = state["model"]
    if m is None:
        return _error(503, "model_not_loaded", state["error"] or "model loading")
    return m.metadata


@app.post("/v1/score")
async def score(request: Request):
    m: Model | None = state["model"]
    if m is None:
        return _error(503, "model_not_loaded", state["error"] or "model loading")
    state["requests"] += 1
    try:
        body = await request.json()
    except Exception:
        state["errors"] += 1
        return _error(400, "invalid_json", "request body must be JSON")
    items = body.get("items") if isinstance(body, dict) else None
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_ITEMS:
        state["errors"] += 1
        return _error(400, "invalid_items", f"items must be a list of 1..{MAX_ITEMS}")
    events, raws, lates = [], [], []
    for i, item in enumerate(items):
        try:
            if not isinstance(item, dict):
                raise ValueError("item must be an object")
            event = validate_event(item.get("event"))
            if event.get("kind") != "txn":
                raise ValueError("only txn events can be scored")
            raw = normalise_raw(item.get("raw"))
            if abs(raw["amount"] - float(event["amount"])) > 1e-6:
                raise ValueError("raw.amount must equal event.amount")
            late = item.get("late", False)
            if not isinstance(late, bool):
                raise ValueError("late must be boolean")
        except (EventValidationError, RawFeatureError, ValueError) as exc:
            state["errors"] += 1
            _log("warning", f"rejected scoring request: items[{i}]: {exc}", stage="scored", status="rejected",
                 error_code="invalid_item", engine=body.get("engine") if isinstance(body.get("engine"), str) else None,
                 event_id=(item.get("event") or {}).get("event_id") if isinstance(item, dict) and isinstance(item.get("event"), dict) else None)
            return _error(400, "invalid_item", f"items[{i}]: {exc}")
        events.append(event)
        raws.append(raw)
        lates.append(late)
    t0 = time.perf_counter()
    results = await run_in_threadpool(m.score, events, raws, lates)
    state["lat_ms"].append(((time.perf_counter() - t0) * 1000, len(results)))
    state["items"] += len(results)
    _maybe_summary()
    if len(results) > 50:
        log.info("scored batch engine=%s n=%d in %.1f ms", body.get("engine"), len(results), (time.perf_counter() - t0) * 1000)
    return {"model_version": m.version, "items": results}
