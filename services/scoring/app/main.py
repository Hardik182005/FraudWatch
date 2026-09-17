"""FraudWatch scoring service (internal). Contract: docs/contracts.md section 4."""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from fwcore.schema import EventValidationError, validate_event

from .model import Model, RawFeatureError, normalise_raw

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s scoring %(message)s")
log = logging.getLogger("scoring")

ARTIFACT = Path(os.getenv("MODEL_ARTIFACT_PATH", "/models/fw-ieee-stream-1.0.0"))
MAX_ITEMS = 500

state: dict = {"model": None, "error": None, "requests": 0, "items": 0, "errors": 0}


def load_model() -> None:
    try:
        state["model"] = Model(ARTIFACT)
        log.info("loaded model %s from %s", state["model"].version, ARTIFACT)
    except Exception as exc:  # surfaced through /healthz rather than crashing silently
        state["error"] = f"{type(exc).__name__}: {exc}"
        log.exception("model load failed")


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
    return {"status": "ok", "model_version": m.version, "loaded_at": m.loaded_at,
            "requests": state["requests"], "items": state["items"], "errors": state["errors"], "pid": os.getpid()}


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
            if abs(raw["amount_inr"] - float(event["amount_inr"])) > 1e-6:
                raise ValueError("raw.amount_inr must equal event.amount_inr")
            late = item.get("late", False)
            if not isinstance(late, bool):
                raise ValueError("late must be boolean")
        except (EventValidationError, RawFeatureError, ValueError) as exc:
            state["errors"] += 1
            return _error(400, "invalid_item", f"items[{i}]: {exc}")
        events.append(event)
        raws.append(raw)
        lates.append(late)
    t0 = time.perf_counter()
    results = await run_in_threadpool(m.score, events, raws, lates)
    state["items"] += len(results)
    if len(results) > 50:
        log.info("scored batch engine=%s n=%d in %.1f ms", body.get("engine"), len(results), (time.perf_counter() - t0) * 1000)
    return {"model_version": m.version, "items": results}
