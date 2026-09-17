"""Config, demo sessions, share links and operator login."""
from __future__ import annotations

import secrets

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from fwcore.schema import SCENARIOS
from fwcore.simulation import PATTERNS
from fwsvc import tokens

from .. import core
from ..access import session_row
from ..ai import provider_order
from ..core import ApiError, fetch_one, limiter, now_ms, principal, require_owner, require_read
from ..serialize import session_out

router = APIRouter()

SCENARIO_TEXT = {
    "normal": "Baseline card traffic sampled from the IEEE-CIS profile",
    "burst": "Normal traffic plus rapid repeated purchases on one account every ~15 s",
    "profile_deviation": "Normal traffic plus purchases far above an account's usual amount",
    "card_testing": "Normal traffic plus small test charges followed by a large one",
    "late_events": "10% of events arrive after the 5 s watermark; 10% out of order",
    "mixed": "All patterns rotating, with some late and out-of-order events",
}
PATTERN_TEXT = {
    "burst": "Burst of purchases on one account",
    "profile_deviation": "Amount far above the account's history",
    "card_testing": "Small test charges, then a large purchase",
    "late_burst": "A burst whose events arrive after the watermark",
}


@router.get("/config")
async def config():
    return {
        "live_url": core.LIVE_APP_URL, "lab_url": core.LAB_APP_URL,
        "demo_max_events_per_session": core.DEMO_MAX_EVENTS_PER_SESSION,
        "demo_max_events_per_second": core.DEMO_MAX_EVENTS_PER_SECOND,
        "ai_enabled": bool(provider_order()) or core.env_bool("AI_FALLBACK_ENABLED", True),
        "ai_provider_order": provider_order(),
        "model_version": core.MODEL_VERSION,
        "scenarios": [{"id": s, "label": s.replace("_", " "), "description": SCENARIO_TEXT.get(s)} for s in SCENARIOS],
        "patterns": [{"id": p, "label": p.replace("_", " "), "description": PATTERN_TEXT.get(p)} for p in PATTERNS],
    }


class CreateSession(BaseModel):
    label: str | None = Field(default=None, max_length=60)


async def _create_session(request: Request, label: str | None) -> dict:
    limiter.check(request, "session_create", core.SESSIONS_PER_CLIENT_PER_HOUR, 3600)
    sid = "s_" + secrets.token_urlsafe(12)
    now = now_ms()
    row = await fetch_one(
        """INSERT INTO demo_sessions (id, label, kind, created_at, expires_at, event_cap, rate_cap_eps, client_hash)
           VALUES (%s, %s, 'visitor', %s, %s, %s, %s, %s) RETURNING *""",
        (sid, (label or "").strip() or None, now, now + core.SESSION_TTL_S * 1000, core.DEMO_MAX_EVENTS_PER_SESSION,
         core.DEMO_MAX_EVENTS_PER_SECOND, core.client_hash(request)))
    token, _ = tokens.sign(core.SESSION_SECRET, "session", sid, core.SESSION_TTL_S)
    return {"session": session_out(row), "token": token}


@router.post("/sessions", status_code=201)
async def create_session(body: CreateSession, request: Request):
    return await _create_session(request, body.label)


@router.get("/sessions/current")
async def current_session(request: Request):
    p = principal(request)
    if not p.session_id:
        raise ApiError(401, "auth_required", "session token required")
    return session_out(await session_row(p.session_id))


@router.get("/sessions/{session_id}")
async def get_session(session_id: str, request: Request):
    p = principal(request)
    s = await session_row(session_id)
    if s["kind"] != "experiment":
        require_read(p, session_id)
    return session_out(s)


@router.post("/sessions/current/reset")
async def reset_session(request: Request):
    p = principal(request)
    if p.kind not in ("session", "operator") or not p.session_id:
        raise ApiError(401, "auth_required", "session token required")
    require_owner(p, p.session_id)
    old = await session_row(p.session_id)
    if old["active_run_id"]:
        try:  # best effort: the generator also fails orphans on restart
            await core.internal("POST", f"{core.GENERATOR_URL}/runs/{old['active_run_id']}/stop")
        except ApiError:
            pass
    out = await _create_session(request, old["label"])
    await fetch_one("UPDATE demo_sessions SET replaced_by = %s WHERE id = %s RETURNING id", (out["session"]["id"], old["id"]))
    return out


@router.post("/sessions/{session_id}/share")
async def share(session_id: str, request: Request):
    p = principal(request)
    await session_row(session_id)
    require_owner(p, session_id)
    token, exp = tokens.sign(core.SESSION_SECRET, "view", session_id, core.VIEW_TOKEN_TTL_S)
    return {"view_token": token, "expires_at": exp, "lab_url": core.LAB_APP_URL, "live_url": core.LIVE_APP_URL}


class OperatorLoginBody(BaseModel):
    passphrase: str = Field(min_length=1, max_length=256)


@router.post("/operator/login")
async def operator_login(body: OperatorLoginBody, request: Request):
    limiter.check(request, "operator_login", 5, 900)
    if not core.verify_passphrase(body.passphrase):
        raise ApiError(401, "invalid_passphrase", "passphrase not accepted")
    token, exp = tokens.sign(core.OPERATOR_SECRET, "operator", "operator", core.OPERATOR_TOKEN_TTL_S)
    return {"token": token, "expires_at": exp}
