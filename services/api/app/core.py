"""API infrastructure: settings, DB pool, errors, auth, rate limits, internal HTTP clients."""
from __future__ import annotations

import hashlib
import hmac
import time
from collections import defaultdict, deque
from dataclasses import dataclass

import httpx
from fastapi import Request
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from fwsvc import settings, tokens
from fwsvc.settings import env, env_bool, env_float, env_int

# -- settings --------------------------------------------------------------------------------
SESSION_SECRET = env("SESSION_SIGNING_SECRET", required=True)
OPERATOR_SECRET = env("OPERATOR_AUTH_SECRET", required=True)
OPERATOR_PASSPHRASE_HASH = env("OPERATOR_PASSPHRASE_HASH", "")
INTERNAL_TOKEN = env("INTERNAL_API_TOKEN", "")
CLIENT_HASH_SALT = env("CLIENT_HASH_SALT", "")
ALLOWED_ORIGINS = [o.strip() for o in env("ALLOWED_ORIGINS", "http://localhost:5173,http://localhost:5174").split(",") if o.strip()]
LIVE_APP_URL = env("LIVE_APP_URL", "http://localhost:5173")
LAB_APP_URL = env("LAB_APP_URL", "http://localhost:5174")
TRUST_PROXY = env_bool("TRUST_PROXY", False)

DEMO_MAX_EVENTS_PER_SESSION = env_int("DEMO_MAX_EVENTS_PER_SESSION", 5000)
DEMO_MAX_EVENTS_PER_SECOND = env_float("DEMO_MAX_EVENTS_PER_SECOND", 50)
EXPERIMENT_MAX_EVENTS_PER_SECOND = env_float("EXPERIMENT_MAX_EVENTS_PER_SECOND", 1000)
OPERATOR_MAX_EVENTS_PER_RUN = env_int("OPERATOR_MAX_EVENTS_PER_RUN", 200_000)
SESSION_TTL_S = env_int("SESSION_TTL_S", 24 * 3600)
VIEW_TOKEN_TTL_S = env_int("VIEW_TOKEN_TTL_S", 7 * 24 * 3600)
OPERATOR_TOKEN_TTL_S = env_int("OPERATOR_TOKEN_TTL_S", 8 * 3600)
SESSIONS_PER_CLIENT_PER_HOUR = env_int("SESSIONS_PER_CLIENT_PER_HOUR", 10)
MAX_CONCURRENT_BENCHMARKS = env_int("MAX_CONCURRENT_BENCHMARKS", 1)

GENERATOR_URL = env("GENERATOR_URL", "http://generator:8002")
SCORING_SERVICE_URL = env("SCORING_SERVICE_URL", "http://scoring:8001")
FLINK_REST_URL = env("FLINK_REST_URL", "http://flink-jobmanager:8081")
OPS_AGENT_URL = env("OPS_AGENT_URL", "http://ops-agent:8003")
MODEL_VERSION = env("MODEL_VERSION", "fw-ieee-stream-1.0.0")
RESULTS_DIR = env("MODEL_RESULTS_DIR", "/srv/results")
EVIDENCE_DIR = env("EVIDENCE_DIR", "/srv/evidence")

ENGINES = ("flink", "spark")


# -- errors ----------------------------------------------------------------------------------
class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, headers: dict | None = None):
        super().__init__(message)
        self.status, self.code, self.message, self.headers = status, code, message, headers


def now_ms() -> int:
    return time.time_ns() // 1_000_000


# -- database --------------------------------------------------------------------------------
pool: AsyncConnectionPool | None = None


async def open_pool() -> None:
    global pool
    pool = AsyncConnectionPool(settings.DATABASE_URL, min_size=2, max_size=env_int("API_DB_POOL_MAX", 12),
                               kwargs={"autocommit": True, "row_factory": dict_row}, open=False)
    await pool.open(wait=True, timeout=60)


async def close_pool() -> None:
    if pool is not None:
        await pool.close()


async def fetch_one(sql: str, params: tuple | dict = ()) -> dict | None:
    async with pool.connection() as c:
        return await (await c.execute(sql, params)).fetchone()


async def fetch_all(sql: str, params: tuple | dict = ()) -> list[dict]:
    async with pool.connection() as c:
        return await (await c.execute(sql, params)).fetchall()


# -- clients & rate limits -------------------------------------------------------------------
def client_ip(request: Request) -> str:
    if TRUST_PROXY:
        fwd = request.headers.get("x-forwarded-for")
        if fwd:
            return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def client_hash(request: Request) -> str:
    return hmac.new(CLIENT_HASH_SALT.encode(), client_ip(request).encode(), hashlib.sha256).hexdigest()[:24]


class RateLimiter:
    """Sliding-window limiter, in memory (single API instance on the VM)."""

    def __init__(self):
        self.hits: dict[tuple[str, str], deque] = defaultdict(deque)

    def check(self, request: Request, bucket: str, limit: int, window_s: int) -> None:
        key = (bucket, client_hash(request))
        q = self.hits[key]
        now = time.monotonic()
        while q and q[0] <= now - window_s:
            q.popleft()
        if len(q) >= limit:
            retry = int(window_s - (now - q[0])) + 1
            raise ApiError(429, "rate_limited", f"too many requests; retry in {retry} s", {"Retry-After": str(retry)})
        q.append(now)
        if len(self.hits) > 50_000:  # bound memory under abuse
            self.hits.clear()


limiter = RateLimiter()
http: httpx.AsyncClient | None = None


async def open_http() -> None:
    global http
    http = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=3.0))


async def internal(method: str, url: str, **kw) -> httpx.Response:
    headers = kw.pop("headers", {}) | ({"x-internal-token": INTERNAL_TOKEN} if INTERNAL_TOKEN else {})
    try:
        return await http.request(method, url, headers=headers, **kw)
    except httpx.HTTPError as exc:
        raise ApiError(503, "dependency_unavailable", f"{url.split('/')[2]} unavailable: {type(exc).__name__}") from exc


def forward_error(resp: httpx.Response) -> ApiError:
    try:
        err = resp.json()["error"]
        return ApiError(resp.status_code, err["code"], err["message"])
    except Exception:
        return ApiError(503 if resp.status_code >= 500 else resp.status_code, "dependency_error", resp.text[:200])


# -- auth ------------------------------------------------------------------------------------
@dataclass
class Principal:
    kind: str                    # anonymous | session | view | operator
    session_id: str | None = None
    operator: bool = False

    @property
    def actor(self) -> str:
        return "operator" if self.operator else f"visitor:{self.session_id}"


def _bearer(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return request.query_params.get("token")


def principal(request: Request) -> Principal:
    p = Principal("anonymous")
    op = request.headers.get("x-operator-token")
    if op:
        try:
            tokens.verify(OPERATOR_SECRET, op, "operator")
            p.operator = True
            p.kind = "operator"
        except tokens.TokenError as exc:
            raise ApiError(401, "invalid_operator_token", str(exc))
    tok = _bearer(request)
    if tok:
        for typ in ("session", "view"):
            try:
                payload = tokens.verify(SESSION_SECRET, tok, typ)
                p.session_id = payload["sub"]
                if p.kind == "anonymous":
                    p.kind = typ
                else:
                    p.kind = "operator"
                break
            except tokens.TokenError as exc:
                last = exc
        else:
            raise ApiError(401, "invalid_token", str(last))
    return p


def require_operator(p: Principal) -> None:
    if not p.operator:
        raise ApiError(403, "operator_required", "this control is restricted to the project operator")


def can_read_session(p: Principal, session_id: str) -> bool:
    return p.operator or p.session_id == session_id


def require_read(p: Principal, session_id: str) -> None:
    if not can_read_session(p, session_id):
        if p.kind == "anonymous":
            raise ApiError(401, "auth_required", "a session or view token for this session is required")
        raise ApiError(403, "forbidden", "token does not grant access to this session")


def require_owner(p: Principal, session_id: str) -> None:
    """Write access: the owning session token (not a view token) or the operator."""
    if p.operator:
        return
    if p.kind == "session" and p.session_id == session_id:
        return
    if p.kind == "view" and p.session_id == session_id:
        raise ApiError(403, "read_only", "view links are read-only")
    require_read(p, session_id)
    raise ApiError(403, "forbidden", "not the owner of this session")


# -- operator passphrase ---------------------------------------------------------------------
def verify_passphrase(passphrase: str) -> bool:
    """OPERATOR_PASSPHRASE_HASH = scrypt:<n>:<r>:<p>:<salt hex>:<hash hex> (see scripts/hash_passphrase.py).
    ':' rather than '$' so Compose/.env interpolation cannot mangle the value."""
    if not OPERATOR_PASSPHRASE_HASH:
        raise ApiError(503, "operator_login_disabled", "operator login is not configured on this deployment")
    try:
        _, n, r, pp, salt, digest = OPERATOR_PASSPHRASE_HASH.replace("$", ":").split(":")
        got = hashlib.scrypt(passphrase.encode(), salt=bytes.fromhex(salt), n=int(n), r=int(r), p=int(pp),
                             dklen=len(bytes.fromhex(digest)), maxmem=256 * 1024 * 1024)
        return hmac.compare_digest(got, bytes.fromhex(digest))
    except ValueError as exc:
        raise ApiError(503, "operator_login_misconfigured", "operator passphrase hash is malformed") from exc
