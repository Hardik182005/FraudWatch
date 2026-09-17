"""FraudWatch public API (port 8000, base path /api/v1). Contract: docs/contracts.md section 6."""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from fwsvc import settings
from fwsvc.db import migrate

from . import core
from .routes import live, runs, sessions, system

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(_: FastAPI):
    migrate(settings.DATABASE_URL)
    await core.open_pool()
    await core.open_http()
    log.info("api ready; allowed origins: %s", ", ".join(core.ALLOWED_ORIGINS))
    yield
    await core.http.aclose()
    await core.close_pool()


app = FastAPI(title="FraudWatch API", version="1.0.0", lifespan=lifespan,
              docs_url="/api/v1/docs", openapi_url="/api/v1/openapi.json", redoc_url=None)
app.add_middleware(
    CORSMiddleware, allow_origins=core.ALLOWED_ORIGINS, allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Operator-Token", "Last-Event-ID"],
    expose_headers=["Retry-After"], max_age=600)


def _err(status: int, code: str, message: str, headers: dict | None = None) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}}, headers=headers)


@app.exception_handler(core.ApiError)
async def api_error(_: Request, exc: core.ApiError):
    return _err(exc.status, exc.code, exc.message, exc.headers)


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError):
    first = exc.errors()[0] if exc.errors() else {}
    loc = ".".join(str(x) for x in first.get("loc", []) if x != "body")
    return _err(422, "validation_error", f"{loc}: {first.get('msg', 'invalid request')}" if loc else "invalid request")


@app.exception_handler(StarletteHTTPException)
async def http_error(_: Request, exc: StarletteHTTPException):
    return _err(exc.status_code, "not_found" if exc.status_code == 404 else "http_error", str(exc.detail))


@app.exception_handler(Exception)
async def unhandled(_: Request, exc: Exception):
    log.exception("unhandled error")
    return _err(500, "internal_error", "unexpected server error")


@app.middleware("http")
async def security_headers(request: Request, call_next):
    resp = await call_next(request)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    if request.url.path.startswith("/api/v1/sessions") or request.url.path.startswith("/api/v1/operator"):
        resp.headers.setdefault("Cache-Control", "no-store")
    return resp


@app.get("/healthz", include_in_schema=False)
async def healthz():
    return {"status": "ok"}


for r in (sessions.router, runs.router, live.router, system.router):
    app.include_router(r, prefix="/api/v1")
