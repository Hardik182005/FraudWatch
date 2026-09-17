"""Compact HMAC-SHA256 signed tokens (session, view, operator).

Format: base64url(json payload) + "." + base64url(hmac). Payload: {typ, sub, iat, exp, [scope]}.
Separate secrets for visitor tokens and operator tokens, so a leaked session secret cannot mint
operator access.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time


class TokenError(ValueError):
    pass


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def sign(secret: str, typ: str, sub: str, ttl_s: int, **extra) -> tuple[str, int]:
    if len(secret) < 32:
        raise RuntimeError("signing secret must be at least 32 characters")
    now = int(time.time())
    payload = {"typ": typ, "sub": sub, "iat": now, "exp": now + ttl_s, **extra}
    body = _b64(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode())
    mac = hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest()
    return f"{body}.{_b64(mac)}", payload["exp"] * 1000


def verify(secret: str, token: str, typ: str) -> dict:
    try:
        body, mac = token.split(".", 1)
        expected = hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _unb64(mac)):
            raise TokenError("bad signature")
        payload = json.loads(_unb64(body))
    except TokenError:
        raise
    except Exception as exc:
        raise TokenError("malformed token") from exc
    if payload.get("typ") != typ:
        raise TokenError("wrong token type")
    if int(payload.get("exp", 0)) < time.time():
        raise TokenError("token expired")
    return payload
