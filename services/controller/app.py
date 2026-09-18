"""FraudWatch lifecycle controller (Cloud Run, scales to zero).

Responsibilities
- Wake the single Apache VM on a validated website visit and drive it to *verified* readiness
  (VM RUNNING -> Postgres+Kafka healthy -> Flink job RUNNING + scoring model loaded -> 5-event end-to-end probe persisted).
- Stop the same VM after PIPELINE_IDLE_TIMEOUT_MINUTES without meaningful activity and without active job leases,
  draining demo generation first. Kafka, Postgres and Flink checkpoint volumes live on the persistent disk.
- Proxy the public API (/api/v1/*) to the VM's *internal* IP over Direct VPC egress; the VM exposes no public ports.

Durability: all lifecycle state, locks, activity and history live in Firestore (outside the VM). Every state transition
is a Firestore transaction guarded by a step token, and each step runs from an authenticated Cloud Tasks callback, so
duplicate deliveries, controller restarts and stale timers cannot stop a system that became active again.
"""
from __future__ import annotations

import asyncio
import base64
import datetime as dt
import hashlib
import hmac
import json
import logging
import os
import time
import uuid

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from google.auth.transport import requests as ga_requests
from google.cloud import firestore
from google.oauth2 import id_token

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("controller")


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


PROJECT = env("GCP_PROJECT_ID", "fraudwatch-508912")
REGION = env("GCP_REGION", "asia-south1")
VM_NAME = env("PIPELINE_VM_NAME", "fraudwatch-pipeline")
VM_ZONE = env("PIPELINE_VM_ZONE", "asia-south1-a")
API_PORT = int(env("PIPELINE_API_PORT", "8000"))
AUTO_WAKE = env("AUTO_WAKE_ENABLED", "true").lower() == "true"
IDLE_TIMEOUT_MIN = float(env("PIPELINE_IDLE_TIMEOUT_MINUTES", "45"))
STARTUP_TIMEOUT_S = int(env("PIPELINE_STARTUP_TIMEOUT_SECONDS", "600"))
DRAIN_TIMEOUT_S = int(env("PIPELINE_DRAIN_TIMEOUT_SECONDS", "180"))
MIN_UPTIME_MIN = float(env("PIPELINE_MIN_UPTIME_MINUTES", "10"))
MAX_WAKES_PER_DAY = int(env("PIPELINE_MAX_WAKES_PER_DAY", "24"))
MAX_UPTIME_H_PER_DAY = float(env("PIPELINE_MAX_UPTIME_HOURS_PER_DAY", "12"))
KEEP_AWAKE_MAX_MIN = int(env("PIPELINE_KEEP_AWAKE_MAX_MINUTES", "180"))
FAILED_RETRY_COOLDOWN_S = int(env("PIPELINE_FAILED_RETRY_COOLDOWN_SECONDS", "60"))
COLLECTION = env("CONTROL_STATE_COLLECTION", "fraudwatch-runtime")
QUEUE = env("CONTROL_TASK_QUEUE", "fraudwatch-lifecycle")
SELF_URL = env("CONTROLLER_URL").rstrip("/")
TASKS_SA = env("TASKS_SERVICE_ACCOUNT")
INTERNAL_TOKEN = env("INTERNAL_API_TOKEN")
OPERATOR_SECRET = env("OPERATOR_AUTH_SECRET")
ALLOWED_ORIGINS = [o.strip() for o in env("ALLOWED_ORIGINS").split(",") if o.strip()]
ENVIRONMENT = env("APP_ENV", "production")

STATE_DOC = "pipeline"
HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers",
               "transfer-encoding", "upgrade", "host", "content-length", "x-forwarded-for", "x-forwarded-host"}
STARTING = ("starting_vm", "starting_services", "restoring")
ACTIVITY_EXEMPT_PATHS = ("system/client-receipt",)

app = FastAPI(title="FraudWatch lifecycle controller", docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_credentials=False,
                   allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
                   allow_headers=["Authorization", "Content-Type", "X-Operator-Token", "Last-Event-ID", "X-FraudWatch-Visit"],
                   expose_headers=["Retry-After"], max_age=600)

db = firestore.AsyncClient(project=PROJECT)
http = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0))
_proxy = httpx.AsyncClient(timeout=httpx.Timeout(None, connect=5.0))
_token_cache: dict = {"token": None, "exp": 0.0}
_state_cache: dict = {"at": 0.0, "value": None}
_activity_written: dict[str, float] = {}
_vm_cache: dict = {"at": 0.0, "status": None}


def now_ms() -> int:
    return int(time.time() * 1000)


def state_ref():
    return db.collection(COLLECTION).document(STATE_DOC)


def default_state() -> dict:
    return {"state": "sleeping", "stage": None, "operation_id": None, "step_token": None, "idle_token": None,
            "updated_at": now_ms(), "last_activity_at": None, "ready_at": None, "wake_requested_at": None,
            "wake_started_at": None, "vm_internal_ip": None, "error": None, "failed_at": None,
            "keep_awake_until": None, "pending_wake": False, "probe_run_id": None, "probe_started_at": None,
            "probe_attempts": 0, "drain_deadline": None, "stop_requested_at": None, "wakes": [], "uptime": [],
            "last_wake_duration_ms": None, "last_stop_duration_ms": None}


async def read_state(max_age_s: float = 0.0) -> dict:
    if max_age_s and _state_cache["value"] and time.monotonic() - _state_cache["at"] < max_age_s:
        return _state_cache["value"]
    snap = await state_ref().get()
    value = default_state() | (snap.to_dict() or {}) if snap.exists else default_state()
    _state_cache.update(at=time.monotonic(), value=value)
    return value


async def history(event: str, **fields) -> None:
    """Lifecycle history for System Intelligence; stored outside the VM, no secrets."""
    doc = {"at": now_ms(), "event": event, "environment": ENVIRONMENT, **fields}
    try:
        await db.collection(COLLECTION).document(STATE_DOC).collection("history").add(doc)
    except Exception as exc:  # history must never break the state machine
        log.warning("history write failed: %s", type(exc).__name__)
    log.info("lifecycle %s", json.dumps(doc, default=str))


def transactional_update(mutate):
    """Run mutate(state) -> dict | None inside a Firestore transaction; returns (new_state, changes)."""
    async def run():
        transaction = db.transaction()

        @firestore.async_transactional
        async def txn(t):
            snap = await state_ref().get(transaction=t)
            cur = default_state() | (snap.to_dict() or {}) if snap.exists else default_state()
            changes = mutate(cur)
            if changes:
                changes["updated_at"] = now_ms()
                t.set(state_ref(), cur | changes)
                return cur | changes, changes
            return cur, None

        result = await txn(transaction)
        _state_cache.update(at=time.monotonic(), value=result[0])
        return result
    return run()


# -- GCP REST helpers (metadata-server credentials; no key files) ---------------------------------------------
async def gcp_token() -> str:
    if _token_cache["token"] and time.time() < _token_cache["exp"] - 60:
        return _token_cache["token"]
    r = await http.get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
                       headers={"Metadata-Flavor": "Google"})
    r.raise_for_status()
    body = r.json()
    _token_cache.update(token=body["access_token"], exp=time.time() + body["expires_in"])
    return body["access_token"]


def _instance_url(suffix: str = "") -> str:
    return f"https://compute.googleapis.com/compute/v1/projects/{PROJECT}/zones/{VM_ZONE}/instances/{VM_NAME}{suffix}"


async def vm_get() -> dict:
    r = await http.get(_instance_url(), headers={"Authorization": f"Bearer {await gcp_token()}"})
    r.raise_for_status()
    inst = r.json()
    nic = (inst.get("networkInterfaces") or [{}])[0]
    return {"status": inst.get("status"), "internal_ip": nic.get("networkIP")}


async def vm_action(action: str) -> str | None:
    r = await http.post(_instance_url(f"/{action}"), headers={"Authorization": f"Bearer {await gcp_token()}"})
    r.raise_for_status()
    return r.json().get("name")


async def enqueue(path: str, payload: dict, delay_s: float = 0) -> None:
    body = {"httpRequest": {"httpMethod": "POST", "url": f"{SELF_URL}{path}",
                            "headers": {"Content-Type": "application/json"},
                            "body": base64.b64encode(json.dumps(payload).encode()).decode(),
                            "oidcToken": {"serviceAccountEmail": TASKS_SA, "audience": SELF_URL}}}
    if delay_s > 0:
        at = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=delay_s)
        body["scheduleTime"] = at.isoformat().replace("+00:00", "Z")
    url = f"https://cloudtasks.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/queues/{QUEUE}/tasks"
    r = await http.post(url, headers={"Authorization": f"Bearer {await gcp_token()}"}, json={"task": body})
    r.raise_for_status()


def verify_task(request: Request) -> None:
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "task auth required")
    try:
        claims = id_token.verify_oauth2_token(auth[7:], ga_requests.Request(), audience=SELF_URL)
    except Exception as exc:
        raise HTTPException(401, "invalid task token") from exc
    if claims.get("email") != TASKS_SA or not claims.get("email_verified"):
        raise HTTPException(403, "unexpected task identity")


async def vm_api(method: str, path: str, ip: str, **kw) -> httpx.Response:
    headers = kw.pop("headers", {}) | {"x-internal-token": INTERNAL_TOKEN}
    return await http.request(method, f"http://{ip}:{API_PORT}/api/v1{path}", headers=headers, **kw)


# -- operator auth (same HMAC token format as the API; secret from Secret Manager) --------------------------------
def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def require_operator(request: Request) -> None:
    token = request.headers.get("x-operator-token") or request.headers.get("authorization", "").removeprefix("Bearer ")
    try:
        body, mac = token.split(".", 1)
        expected = hmac.new(OPERATOR_SECRET.encode(), body.encode(), hashlib.sha256).digest()
        payload = json.loads(_unb64(body))
        ok = len(OPERATOR_SECRET) >= 32 and hmac.compare_digest(expected, _unb64(mac)) \
            and payload.get("typ") == "operator" and int(payload.get("exp", 0)) > time.time()
    except Exception:
        ok = False
    if not ok:
        raise HTTPException(403, "operator token required")


# -- public lifecycle API ----------------------------------------------------------------------------------------
def public_view(s: dict) -> dict:
    last = s.get("last_activity_at")
    return {"state": s["state"], "stage": s.get("stage"), "operation_id": s.get("operation_id"),
            "updated_at": s.get("updated_at"), "error": s.get("error"), "auto_wake_enabled": AUTO_WAKE,
            "idle_timeout_minutes": IDLE_TIMEOUT_MIN, "last_activity_at": last,
            "idle_deadline_at": (last + int(IDLE_TIMEOUT_MIN * 60_000)) if (last and s["state"] == "ready") else None,
            "ready_at": s.get("ready_at"), "wake_started_at": s.get("wake_started_at"),
            "last_wake_duration_ms": s.get("last_wake_duration_ms"), "last_stop_duration_ms": s.get("last_stop_duration_ms"),
            "keep_awake_until": s.get("keep_awake_until"), "pending_wake": s.get("pending_wake", False),
            "startup_timeout_seconds": STARTUP_TIMEOUT_S, "as_of": now_ms()}


def visitor_check(request: Request) -> None:
    origin = request.headers.get("origin", "")
    if ALLOWED_ORIGINS and origin not in ALLOWED_ORIGINS:
        raise HTTPException(403, "unknown origin")
    if request.headers.get("x-fraudwatch-visit") != "1":
        raise HTTPException(400, "visit header required")


def client_ip(request: Request) -> str:
    fwd = [p.strip() for p in request.headers.get("x-forwarded-for", "").split(",") if p.strip()]
    return fwd[-1] if fwd else (request.client.host if request.client else "unknown")


async def rate_limit(request: Request, bucket: str, limit: int, window_s: int) -> None:
    key = hashlib.sha256(f"{bucket}:{client_ip(request)}".encode()).hexdigest()[:32]
    ref = db.collection(COLLECTION).document(STATE_DOC).collection("ratelimits").document(key)
    window = int(time.time() // window_s)
    transaction = db.transaction()

    @firestore.async_transactional
    async def txn(t):
        snap = await ref.get(transaction=t)
        d = snap.to_dict() or {}
        count = d.get("count", 0) if d.get("window") == window else 0
        if count >= limit:
            return False
        t.set(ref, {"window": window, "count": count + 1, "expires_at": dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=window_s * 2)})
        return True

    if not await txn(transaction):
        raise HTTPException(429, "too many wake requests; try again shortly")


@app.get("/health")
async def healthz():
    return {"status": "ok"}


@app.get("/lifecycle/status")
async def status():
    s = await read_state(max_age_s=2)
    view = public_view(s)
    if time.monotonic() - _vm_cache["at"] > 15:
        try:
            _vm_cache.update(at=time.monotonic(), status=(await vm_get())["status"])
        except Exception:
            _vm_cache.update(at=time.monotonic(), status=None)
    view["vm_status_observed"] = _vm_cache["status"]   # Compute Engine view, independent of controller state
    return view


@app.get("/lifecycle/history")
async def lifecycle_history(limit: int = 30):
    q = state_ref().collection("history").order_by("at", direction=firestore.Query.DESCENDING).limit(min(max(limit, 1), 100))
    return {"items": [d.to_dict() async for d in q.stream()]}


def _budget_block(s: dict) -> str | None:
    day_ago = now_ms() - 86_400_000
    wakes = [w for w in s.get("wakes", []) if w > day_ago]
    if len(wakes) >= MAX_WAKES_PER_DAY:
        return f"daily wake limit ({MAX_WAKES_PER_DAY}) reached to protect trial credits"
    up_ms = sum(max(0, min(u["end"], now_ms()) - max(u["start"], day_ago)) for u in s.get("uptime", []) if u["end"] > day_ago)
    if up_ms >= MAX_UPTIME_H_PER_DAY * 3_600_000:
        return f"daily runtime guard ({MAX_UPTIME_H_PER_DAY:g} h) reached to protect trial credits"
    return None


@app.post("/lifecycle/ensure-ready")
async def ensure_ready(request: Request):
    visitor_check(request)
    await rate_limit(request, "wake", 10, 600)
    if not AUTO_WAKE:
        return public_view(await read_state())
    started = {"op": None, "reconciled": False}
    # "ready" is the controller's belief, not a fact: a VM stopped outside the controller (Console, gcloud,
    # host maintenance) leaves the state at "ready" and every visit would be told the backend is up. When the
    # state claims ready, ask Compute Engine, and treat a stopped VM as sleeping so this visit wakes it.
    vm_down = False
    if (await read_state(max_age_s=2))["state"] == "ready":
        try:
            vm_status = (await vm_get())["status"]
            _vm_cache.update(at=time.monotonic(), status=vm_status)
            vm_down = vm_status in ("TERMINATED", "STOPPED", "STOPPING", "SUSPENDED", "SUSPENDING")
        except Exception:
            vm_down = False   # cannot tell: keep the controller's view rather than guess

    def mutate(s: dict):
        now = now_ms()
        base: dict = {}
        if s["state"] == "ready":
            if not vm_down:
                return {"last_activity_at": now}
            started["reconciled"] = True
            uptime = s.get("uptime", [])
            if uptime:   # the real stop time is unknown; closing at now over-counts, which is the safe side for the guard
                uptime[-1] = uptime[-1] | {"end": now}
            base = {"state": "sleeping", "stage": None, "vm_internal_ip": None, "idle_token": None, "uptime": uptime}
            s = s | base
        if s["state"] in STARTING:
            return {"last_activity_at": now}
        if s["state"] == "stopping":
            if s.get("stage") in ("draining", "waiting_drain"):   # VM not yet asked to stop: cancel the shutdown
                return {"state": "ready", "stage": None, "last_activity_at": now, "idle_token": uuid.uuid4().hex,
                        "error": None}
            return {"pending_wake": True, "last_activity_at": now}
        if s["state"] == "failed" and s.get("failed_at") and now - s["failed_at"] < FAILED_RETRY_COOLDOWN_S * 1000:
            return None
        block = _budget_block(s)
        if block:
            return (base | {"error": block}) if (base or s.get("error") != block) else None
        op = "wake-" + uuid.uuid4().hex[:12]
        started["op"] = op
        return base | {"state": "starting_vm", "stage": "requesting VM start", "operation_id": op, "step_token": uuid.uuid4().hex,
                "wake_requested_at": now, "wake_started_at": now, "last_activity_at": now, "error": None,
                "probe_run_id": None, "probe_attempts": 0, "pending_wake": False,
                "wakes": [w for w in s.get("wakes", []) if w > now - 86_400_000] + [now]}

    s, changes = await transactional_update(mutate)
    if started["reconciled"] and changes:
        await history("vm_stopped_externally", vm_status=_vm_cache["status"])
    if started["op"] and changes:
        await history("wake_requested", operation_id=started["op"])
        await enqueue("/tasks/advance", {"token": s["step_token"]})
    if s["state"] == "ready" and changes and "idle_token" in changes:
        await history("shutdown_cancelled_by_visit")
        await enqueue("/tasks/idle-check", {"token": s["idle_token"]}, IDLE_TIMEOUT_MIN * 60)
    return public_view(s)


async def record_activity(kind: str) -> None:
    last = _activity_written.get("any", 0.0)
    if time.monotonic() - last < 30:   # at most one write per instance per 30 s
        return
    _activity_written["any"] = time.monotonic()
    await state_ref().set({"last_activity_at": now_ms(), "last_activity_kind": kind[:40]}, merge=True)


@app.post("/lifecycle/activity")
async def activity(request: Request):
    visitor_check(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    await record_activity(str(body.get("kind") or "interaction"))
    return {"ok": True}


@app.post("/lifecycle/keep-awake")
async def keep_awake(request: Request):
    require_operator(request)
    body = await request.json()
    minutes = int(body.get("minutes", 0))
    if not 0 <= minutes <= KEEP_AWAKE_MAX_MIN:
        raise HTTPException(422, f"minutes must be between 0 and {KEEP_AWAKE_MAX_MIN}")
    until = now_ms() + minutes * 60_000 if minutes else None
    s, _ = await transactional_update(lambda cur: {"keep_awake_until": until, "last_activity_at": now_ms()})
    await history("keep_awake_set", minutes=minutes, until=until)
    return public_view(s)


# -- task handlers ------------------------------------------------------------------------------------------------
@app.post("/tasks/advance")
async def advance(request: Request):
    verify_task(request)
    token = (await request.json()).get("token")
    s = await read_state()
    if token != s.get("step_token"):
        return {"ignored": "stale step"}
    try:
        if s["state"] in STARTING:
            await _advance_start(s)
        elif s["state"] == "stopping":
            await _advance_stop(s)
    except Exception as exc:
        log.warning("advance step failed in %s/%s: %s", s["state"], s.get("stage"), type(exc).__name__)
        await enqueue("/tasks/advance", {"token": token}, 15)   # bounded by the startup/stop deadlines
    return {"ok": True}


async def _set_if_token(token: str, changes: dict) -> dict | None:
    def mutate(cur):
        return changes if cur.get("step_token") == token else None
    s, applied = await transactional_update(mutate)
    return s if applied else None


async def _fail_start(s: dict, stage: str, reason: str) -> None:
    token = s["step_token"]
    new = await _set_if_token(token, {"state": "failed", "stage": stage, "error": reason, "failed_at": now_ms(),
                                      "step_token": uuid.uuid4().hex})
    if new:
        await history("wake_failed", operation_id=s.get("operation_id"), stage=stage, reason=reason)
        try:   # do not leave a broken VM consuming credits
            await vm_action("stop")
            await history("vm_stop_after_failure", operation_id=s.get("operation_id"))
        except Exception as exc:
            log.warning("stop after failure failed: %s", type(exc).__name__)


async def _advance_start(s: dict) -> None:
    token, state = s["step_token"], s["state"]
    if now_ms() - (s.get("wake_started_at") or now_ms()) > STARTUP_TIMEOUT_S * 1000:
        await _fail_start(s, state, f"startup exceeded {STARTUP_TIMEOUT_S} s at stage '{s.get('stage')}'")
        return
    if state == "starting_vm":
        vm = await vm_get()
        if vm["status"] in ("TERMINATED", "STOPPED"):
            op = await vm_action("start")
            await _set_if_token(token, {"stage": "VM start requested", "gce_operation": op})
            await history("vm_start_requested", operation_id=s.get("operation_id"), gce_operation=op)
        elif vm["status"] == "RUNNING":
            if await _set_if_token(token, {"state": "starting_services", "stage": "waiting for Postgres and Kafka",
                                           "vm_internal_ip": vm["internal_ip"], "vm_running_at": now_ms()}):
                await history("vm_running", operation_id=s.get("operation_id"))
        else:
            await _set_if_token(token, {"stage": f"VM is {vm['status'].lower()}"})
        await enqueue("/tasks/advance", {"token": token}, 10)
        return
    ip = s.get("vm_internal_ip")
    try:
        health = (await vm_api("GET", "/health", ip, timeout=6)).json()
    except Exception:
        health = None
    comps = {c["id"]: c for c in (health or {}).get("components", [])}
    if state == "starting_services":
        if comps.get("postgres", {}).get("status") == "ok" and comps.get("kafka", {}).get("status") == "ok":
            await _set_if_token(token, {"state": "restoring", "stage": "restoring Flink job and scoring model"})
            await history("services_up", operation_id=s.get("operation_id"))
        else:
            await _set_if_token(token, {"stage": "waiting for Postgres and Kafka" if health else "waiting for the API container"})
        await enqueue("/tasks/advance", {"token": token}, 8)
        return
    # restoring: Flink job RUNNING + model loaded, then a real end-to-end probe must persist a scored result
    flink, scoring = comps.get("flink", {}), comps.get("scoring", {})
    if flink.get("metrics", {}).get("job_state") != "RUNNING" or scoring.get("status") != "ok":
        await _set_if_token(token, {"stage": "waiting for Flink job RUNNING and scoring model"})
        await enqueue("/tasks/advance", {"token": token}, 8)
        return
    probe = s.get("probe_run_id")
    if probe:
        st = (await vm_api("GET", f"/internal/probe/{probe}", ip, timeout=6)).json()
        if st.get("ok"):
            now = now_ms()
            idle_token = uuid.uuid4().hex
            new = await _set_if_token(token, {
                "state": "ready", "stage": None, "ready_at": now, "error": None, "idle_token": idle_token,
                "last_wake_duration_ms": now - (s.get("wake_started_at") or now), "step_token": uuid.uuid4().hex,
                "uptime": [u for u in s.get("uptime", []) if u["end"] > now - 7 * 86_400_000] + [{"start": s.get("vm_running_at") or now, "end": now}],
                "flink_job_id": flink.get("metrics", {}).get("job_id"), "model_version": scoring.get("metrics", {}).get("model_version")})
            if new:
                await history("ready", operation_id=s.get("operation_id"), probe_run_id=probe, probe_events=st.get("persisted"),
                              wake_duration_ms=new["last_wake_duration_ms"], flink_job_id=new.get("flink_job_id"))
                await enqueue("/tasks/idle-check", {"token": idle_token}, IDLE_TIMEOUT_MIN * 60)
            return
        if now_ms() - (s.get("probe_started_at") or 0) < 90_000:
            await enqueue("/tasks/advance", {"token": token}, 5)
            return
        if s.get("probe_attempts", 0) >= 3:
            await _fail_start(s, "restoring", "end-to-end probe produced no persisted result after 3 attempts")
            return
    r = await vm_api("POST", "/internal/probe", ip, timeout=10)
    r.raise_for_status()
    await _set_if_token(token, {"stage": "running end-to-end probe", "probe_run_id": r.json()["run_id"],
                                "probe_started_at": now_ms(), "probe_attempts": s.get("probe_attempts", 0) + 1})
    await enqueue("/tasks/advance", {"token": token}, 5)


@app.post("/tasks/idle-check")
async def idle_check(request: Request):
    verify_task(request)
    token = (await request.json()).get("token")
    s = await read_state()
    if s["state"] != "ready" or token != s.get("idle_token"):
        return {"ignored": "stale idle check"}
    now = now_ms()
    timeout_ms = int(IDLE_TIMEOUT_MIN * 60_000)
    candidates = [(s.get("last_activity_at") or 0) + timeout_ms, (s.get("ready_at") or 0) + int(MIN_UPTIME_MIN * 60_000),
                  s.get("keep_awake_until") or 0]
    not_before = max(candidates)
    if now < not_before:
        await enqueue("/tasks/idle-check", {"token": token}, (not_before - now) / 1000 + 5)
        return {"rescheduled_at": not_before}
    try:
        work = (await vm_api("GET", "/internal/active-work", s["vm_internal_ip"], timeout=8)).json()
    except Exception:
        work = {"busy": False, "unreachable": True}
    if work.get("busy"):
        await history("idle_deferred_active_work", runs=len(work.get("runs", [])), experiments=len(work.get("experiments", [])),
                      draining_runs=work.get("draining_runs"))
        await enqueue("/tasks/idle-check", {"token": token}, 300)
        return {"deferred": True}
    step = uuid.uuid4().hex

    def mutate(cur):
        if cur["state"] != "ready" or cur.get("idle_token") != token:
            return None
        if (cur.get("last_activity_at") or 0) + timeout_ms > now_ms():
            return None
        return {"state": "stopping", "stage": "draining", "step_token": step, "stop_requested_at": now_ms(),
                "operation_id": "stop-" + uuid.uuid4().hex[:12]}

    new, changed = await transactional_update(mutate)
    if changed:
        await history("idle_shutdown_started", operation_id=new["operation_id"], idle_minutes=IDLE_TIMEOUT_MIN,
                      last_activity_at=new.get("last_activity_at"))
        await enqueue("/tasks/advance", {"token": step})
    else:
        await enqueue("/tasks/idle-check", {"token": token}, 60)
    return {"ok": True}


async def _advance_stop(s: dict) -> None:
    token, stage, ip = s["step_token"], s.get("stage"), s.get("vm_internal_ip")
    if stage == "draining":
        try:
            res = (await vm_api("POST", "/internal/drain", ip, timeout=20)).json()
        except Exception:
            res = {"error": "drain call failed"}
        await _set_if_token(token, {"stage": "waiting_drain", "drain_deadline": now_ms() + DRAIN_TIMEOUT_S * 1000})
        await history("drain_requested", operation_id=s.get("operation_id"), result=res)
        await enqueue("/tasks/advance", {"token": token}, 10)
        return
    if stage == "waiting_drain":
        try:
            work = (await vm_api("GET", "/internal/active-work", ip, timeout=8)).json()
        except Exception:
            work = {"busy": False, "draining_runs": 0, "unreachable": True}
        if work.get("experiments"):   # an experiment started meanwhile: abort shutdown
            idle_token = uuid.uuid4().hex
            if await _set_if_token(token, {"state": "ready", "stage": None, "idle_token": idle_token, "step_token": uuid.uuid4().hex}):
                await history("shutdown_aborted_active_experiment", operation_id=s.get("operation_id"))
                await enqueue("/tasks/idle-check", {"token": idle_token}, 300)
            return
        if work.get("draining_runs") and now_ms() < (s.get("drain_deadline") or 0):
            await enqueue("/tasks/advance", {"token": token}, 10)
            return
        op = await vm_action("stop")   # the VM shutdown script stops Compose gracefully (checkpoints are retained)
        await _set_if_token(token, {"stage": "stopping_vm", "gce_operation": op})
        await history("vm_stop_requested", operation_id=s.get("operation_id"), gce_operation=op,
                      undrained_runs=work.get("draining_runs", 0))
        await enqueue("/tasks/advance", {"token": token}, 15)
        return
    if stage == "stopping_vm":
        vm = await vm_get()
        if vm["status"] != "TERMINATED":
            if now_ms() - (s.get("stop_requested_at") or 0) > (DRAIN_TIMEOUT_S + 600) * 1000:
                await _set_if_token(token, {"state": "failed", "stage": "stopping_vm", "failed_at": now_ms(),
                                            "error": f"VM still {vm['status']} long after stop request"})
                await history("stop_failed", operation_id=s.get("operation_id"), vm_status=vm["status"])
                return
            await enqueue("/tasks/advance", {"token": token}, 15)
            return
        now = now_ms()
        pending = s.get("pending_wake")
        uptime = s.get("uptime", [])
        if uptime:
            uptime[-1] = uptime[-1] | {"end": now}
        changes = {"state": "sleeping", "stage": None, "vm_internal_ip": None, "pending_wake": False, "uptime": uptime,
                   "last_stop_duration_ms": now - (s.get("stop_requested_at") or now), "step_token": uuid.uuid4().hex}
        if pending:
            op = "wake-" + uuid.uuid4().hex[:12]
            changes |= {"state": "starting_vm", "stage": "requesting VM start", "operation_id": op, "wake_started_at": now,
                        "probe_run_id": None, "probe_attempts": 0}
        new = await _set_if_token(token, changes)
        if new:
            await history("sleeping", operation_id=s.get("operation_id"), stop_duration_ms=changes["last_stop_duration_ms"])
            if pending:
                await history("wake_requested", operation_id=new["operation_id"], reason="visit during shutdown")
                await enqueue("/tasks/advance", {"token": new["step_token"]})


# -- API proxy ---------------------------------------------------------------------------------------------------
@app.api_route("/api/v1/{path:path}", methods=["GET", "POST", "PATCH", "OPTIONS"])
async def proxy(path: str, request: Request):
    s = await read_state(max_age_s=5)
    if s["state"] not in ("ready", "stopping", "restoring") or not s.get("vm_internal_ip") \
            or (s["state"] == "stopping" and s.get("stage") == "stopping_vm"):
        return JSONResponse(status_code=503, headers={"Retry-After": "15"}, content={"error": {
            "code": "pipeline_" + s["state"], "message": "the cloud processing backend is not ready",
            "lifecycle": public_view(s)}})
    if request.method in ("POST", "PATCH") and not path.startswith(ACTIVITY_EXEMPT_PATHS):
        asyncio.create_task(record_activity(f"api:{path.split('/')[0]}"))
    headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_HEADERS}
    headers["x-forwarded-for"] = client_ip(request)
    url = f"http://{s['vm_internal_ip']}:{API_PORT}/api/v1/{path}"
    try:
        upstream = await _proxy.send(_proxy.build_request(request.method, url, params=request.query_params,
                                                          headers=headers, content=await request.body()), stream=True)
    except httpx.HTTPError:
        return JSONResponse(status_code=502, content={"error": {"code": "backend_unreachable",
                                                                "message": "the cloud backend did not respond"}})
    out_headers = {k: v for k, v in upstream.headers.items()
                   if k.lower() not in HOP_HEADERS | {"content-encoding"} and not k.lower().startswith("access-control-")}

    async def body():
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
        finally:
            await upstream.aclose()

    return StreamingResponse(body(), status_code=upstream.status_code, headers=out_headers)
