"""Model (CA1), health & architecture, experiments (CA2), Spark ops, evidence and the assistant."""
from __future__ import annotations

import csv
import io
import json
import secrets
import time
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from fwcore import SCHEMA_VERSION
from fwsvc import settings

from .. import ai, core
from ..access import readable, session_row
from ..core import ApiError, fetch_all, fetch_one, limiter, now_ms, principal, require_operator
from ..health import health_snapshot
from ..serialize import experiment_out

router = APIRouter()
_model_cache: dict = {"at": 0.0, "value": None}


def _results_file(name: str) -> dict | None:
    p = Path(core.RESULTS_DIR) / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


# -- model ---------------------------------------------------------------------------------------
async def model_metadata() -> dict:
    if _model_cache["value"] is None or time.monotonic() - _model_cache["at"] > 60:
        r = await core.internal("GET", f"{core.SCORING_SERVICE_URL}/v1/model")
        if r.status_code != 200:
            raise core.forward_error(r)
        _model_cache.update(value=r.json(), at=time.monotonic())
    return _model_cache["value"]


@router.get("/model")
async def model():
    return await model_metadata()


@router.get("/model/results")
async def model_results():
    return {"stream": _results_file("stream_model_results.json"), "reference": _results_file("reference_model_results.json")}


@router.get("/model/evaluation")
async def model_evaluation():
    """CA1 visuals from the saved training artifact (evaluation.json); nothing is recomputed at request time."""
    ev = _results_file("evaluation.json")
    if ev is None:
        raise ApiError(404, "not_found", "evaluation artifact not bundled with this API image")
    return ev


class WhatIfEvent(BaseModel):
    amount: float = Field(gt=0, le=1_000_000)
    product_cd: str = Field(max_length=20)
    card_network: str = Field(max_length=40)
    card_type: str = Field(max_length=40)
    email_domain: str = Field(max_length=80)
    device_type: str = Field(max_length=40)
    has_identity: bool


class WhatIf(BaseModel):
    event: WhatIfEvent
    raw: dict[str, float | int | None]


@router.post("/model/what-if")
async def what_if(body: WhatIf, request: Request):
    limiter.check(request, "what_if", 30, 60)
    now = now_ms()
    event = {"schema_version": SCHEMA_VERSION, "kind": "txn", "event_id": "what-if", "demo_session_id": "what-if",
             "run_id": "what-if", "seq": 0, "account_id": "what-if", "event_time": now, "produced_at": now,
             "target_engines": ["flink"], **body.event.model_dump()}
    raw = dict(body.raw) | {"amount": body.event.amount}
    r = await core.internal("POST", f"{core.SCORING_SERVICE_URL}/v1/score",
                            json={"engine": "what-if", "items": [{"event": event, "raw": raw, "late": False}]})
    if r.status_code == 400:
        err = r.json().get("error", {})
        raise ApiError(422, err.get("code", "invalid_input"), err.get("message", "invalid what-if input"))
    if r.status_code != 200:
        raise core.forward_error(r)
    item = r.json()["items"][0]
    return item | {"disclaimer": "What-if shows how this model's score responds to the inputs you changed "
                                 "(sensitivity). It is not causal and says nothing about real customers."}


# -- health & architecture -------------------------------------------------------------------------
@router.get("/health")
async def health():
    return await health_snapshot()


@router.get("/architecture")
async def architecture():
    h = {c["id"]: c["status"] for c in (await health_snapshot())["components"]}
    t = settings
    nodes = [
        {"id": "generator", "label": "Transaction generator", "role": "Replays IEEE-CIS-profiled synthetic card traffic into Kafka",
         "config": {"producer": "idempotent, acks=all", "heartbeats": "1 per second to every partition",
                    "visitor_rate_cap_eps": core.DEMO_MAX_EVENTS_PER_SECOND, "session_event_cap": core.DEMO_MAX_EVENTS_PER_SESSION}},
        {"id": "kafka", "label": "Apache Kafka 4.1 (KRaft)", "role": "Durable, partitioned event log shared by both engines",
         "config": {"transactions_topic": t.TOPIC_TRANSACTIONS, "partitions": 4, "timestamp": "LogAppendTime",
                    "results_topics": list(t.TOPIC_RESULTS.values()), "partition_key": "demo_session_id:account_id"}},
        {"id": "flink", "label": "Apache Flink 1.20", "role": "True streaming: per-record keyed state, event-time watermarks",
         "config": {"watermark": "bounded out-of-orderness 5 s", "checkpoint_interval": "10 s", "parallelism": 2,
                    "window": "60 s tumbling (event time)", "restart": "fixed-delay 10 x 5 s", "rest": core.FLINK_REST_URL}},
        {"id": "spark", "label": "Apache Spark 3.5 Structured Streaming", "role": "Micro-batch streaming with applyInPandasWithState",
         "config": {"trigger": core.env("SPARK_TRIGGER_INTERVAL", "1 second"), "master": "local[2]",
                    "watermark": "5 s (fixed per micro-batch)", "output": "foreachBatch -> Kafka", "launch": "on demand"}},
        {"id": "scoring", "label": "Scoring service", "role": "LightGBM model + rules + policy, TreeSHAP explanations",
         "config": {"model_version": core.MODEL_VERSION, "retries": "3 attempts, 200 ms backoff, 3 s timeout"}},
        {"id": "result_consumer", "label": "Result consumer", "role": "Idempotent upsert of engine output into Postgres",
         "config": {"idempotency_key": "(engine, event_id)", "delivery": "at-least-once, duplicates counted"}},
        {"id": "postgres", "label": "PostgreSQL 17", "role": "Scored events, alerts, hash-chained audit log, experiments", "config": {}},
        {"id": "api", "label": "FastAPI", "role": "Public API, SSE fan-out, caps and operator protection",
         "config": {"base_path": "/api/v1", "sse": "cursor-resumable (Last-Event-ID)"}},
        {"id": "live", "label": "FraudWatch Live", "role": "Operations console website", "config": {"url": core.LIVE_APP_URL}},
        {"id": "lab", "label": "FraudWatch Lab", "role": "CA1/CA2 evidence and experiments website", "config": {"url": core.LAB_APP_URL}},
    ]
    for n in nodes:
        n["status"] = h.get(n["id"], "unknown")
    edges = [
        {"from": "generator", "to": "kafka", "label": "transactions"},
        {"from": "kafka", "to": "flink", "label": "consume (per record)"},
        {"from": "kafka", "to": "spark", "label": "consume (micro-batch)"},
        {"from": "flink", "to": "scoring", "label": "raw features"},
        {"from": "spark", "to": "scoring", "label": "raw features"},
        {"from": "flink", "to": "kafka", "label": "results.flink"},
        {"from": "spark", "to": "kafka", "label": "results.spark"},
        {"from": "kafka", "to": "result_consumer", "label": "results + windows"},
        {"from": "result_consumer", "to": "postgres", "label": "idempotent upsert"},
        {"from": "api", "to": "postgres", "label": "queries + stream log"},
        {"from": "api", "to": "generator", "label": "run controls"},
        {"from": "live", "to": "api", "label": "REST + SSE"},
        {"from": "lab", "to": "api", "label": "REST + SSE"},
    ]
    return {"nodes": nodes, "edges": edges}


# -- experiments ----------------------------------------------------------------------------------
async def _experiment(exp_id: str) -> dict:
    row = await fetch_one("SELECT * FROM experiments WHERE id = %s", (exp_id,))
    if row is None:
        raise ApiError(404, "not_found", "experiment not found")
    return row


@router.get("/experiments")
async def list_experiments():
    rows = await fetch_all("SELECT * FROM experiments ORDER BY created_at DESC LIMIT 100")
    return [experiment_out(r) for r in rows]


@router.get("/experiments/{exp_id}")
async def get_experiment(exp_id: str):
    return experiment_out(await _experiment(exp_id))


RAW_COLUMNS = ["run_id", "engine", "event_id", "seq", "pattern", "late", "action", "scoring_status", "duplicates",
               "event_time", "produced_at", "kafka_ts", "engine_received_at", "scoring_started_at",
               "scoring_completed_at", "emitted_at", "persisted_at", "e2e_ms", "engine_run_label"]


@router.get("/experiments/{exp_id}/raw.csv")
async def experiment_raw(exp_id: str, request: Request):
    exp = await _experiment(exp_id)
    limiter.check(request, "raw_csv", 20, 600)
    run_ids = list(exp["run_ids"])

    async def rows():
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(RAW_COLUMNS)
        yield buf.getvalue()
        last = ("", "", "")
        while run_ids:
            batch = await fetch_all(
                f"""SELECT {', '.join(RAW_COLUMNS)} FROM scored_events WHERE run_id = ANY(%s)
                    AND (run_id, engine, event_id) > (%s, %s, %s) ORDER BY run_id, engine, event_id LIMIT 5000""",
                (run_ids, *last))
            if not batch:
                break
            buf.seek(0)
            buf.truncate()
            for r in batch:
                w.writerow([r[c] for c in RAW_COLUMNS])
            yield buf.getvalue()
            last = (batch[-1]["run_id"], batch[-1]["engine"], batch[-1]["event_id"])

    return StreamingResponse(rows(), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="{exp_id}-raw.csv"'})


@router.get("/experiments/{exp_id}/evidence.json")
async def experiment_evidence(exp_id: str):
    exp = await _experiment(exp_id)
    runs = await fetch_all("SELECT * FROM runs WHERE id = ANY(%s) ORDER BY created_at", (list(exp["run_ids"]),))
    body = {"experiment": experiment_out(exp),
            "runs": [{k: r[k] for k in ("id", "scenario", "seed", "initial_rate_eps", "engines", "status", "max_events",
                                        "events_sent", "started_at", "ended_at", "control_log", "error")} for r in runs],
            "raw_csv": f"/api/v1/experiments/{exp_id}/raw.csv", "exported_at": now_ms(),
            "provenance": "measured" if exp["status"] == "completed" else "incomplete"}
    return JSONResponse(body, headers={"Content-Disposition": f'attachment; filename="{exp_id}-evidence.json"'})


class CreateExperiment(BaseModel):
    kind: Literal["comparison", "late_events", "recovery"]
    params: dict = Field(default_factory=dict)


@router.post("/experiments", status_code=202)
async def create_experiment(body: CreateExperiment, request: Request):
    require_operator(principal(request))
    pending = await fetch_one("SELECT count(*) AS n FROM experiments WHERE status IN ('queued', 'running')")
    if pending["n"] >= core.env_int("EXPERIMENT_QUEUE_MAX", 5):
        raise ApiError(429, "queue_full", "experiment queue is full; wait for queued experiments to finish")
    if len(json.dumps(body.params)) > 4000:
        raise ApiError(422, "invalid_params", "params too large")
    exp_id = f"exp_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}_{secrets.token_hex(3)}"
    row = await fetch_one(
        """INSERT INTO experiments (id, kind, status, params, created_at, created_by)
           VALUES (%s, %s, 'queued', %s, %s, 'operator') RETURNING *""",
        (exp_id, body.kind, Jsonb(body.params), now_ms()))
    return experiment_out(row)


@router.post("/experiments/{exp_id}/cancel")
async def cancel_experiment(exp_id: str, request: Request):
    require_operator(principal(request))
    row = await fetch_one(
        """UPDATE experiments SET
             status = CASE WHEN status = 'queued' THEN 'cancelled' ELSE status END,
             finished_at = CASE WHEN status = 'queued' THEN %s ELSE finished_at END,
             progress = progress || '{"cancel_requested": true}'::jsonb
           WHERE id = %s AND status IN ('queued', 'running') RETURNING *""", (now_ms(), exp_id))
    if row is None:
        await _experiment(exp_id)
        raise ApiError(409, "not_cancellable", "experiment already finished")
    return experiment_out(row)


# -- Spark ops ------------------------------------------------------------------------------------
class SparkStart(BaseModel):
    session_id: str | None = None
    run_id: str | None = None
    mode: Literal["live", "replay"] = "live"


async def _spark_component() -> dict:
    return next(c for c in (await health_snapshot(0))["components"] if c["id"] == "spark")


@router.post("/ops/spark/start")
async def spark_start(body: SparkStart, request: Request):
    require_operator(principal(request))
    r = await core.internal("POST", f"{core.OPS_AGENT_URL}/spark/start", json=body.model_dump(), timeout=60)
    if r.status_code >= 300:
        raise core.forward_error(r)
    return await _spark_component()


@router.post("/ops/spark/stop")
async def spark_stop(request: Request):
    require_operator(principal(request))
    r = await core.internal("POST", f"{core.OPS_AGENT_URL}/spark/stop", timeout=60)
    if r.status_code >= 300:
        raise core.forward_error(r)
    return await _spark_component()


# -- evidence -------------------------------------------------------------------------------------
@router.get("/evidence")
async def evidence():
    items = []
    stream = _results_file("stream_model_results.json")
    if stream:
        items.append({"id": "model-" + stream["model_version"], "title": f"Model evaluation ({stream['model_version']})",
                      "kind": "model", "created_at": int((await model_metadata()).get("trained_at_unix", 0) * 1000),
                      "config": {"selected": stream["selected"], "thresholds": stream["thresholds"]},
                      "url": "/api/v1/model/results", "provenance": "measured"})
    for e in await fetch_all("SELECT * FROM experiments WHERE status = 'completed' ORDER BY finished_at DESC LIMIT 50"):
        items.append({"id": e["id"], "title": f"{e['kind'].replace('_', ' ').title()} experiment", "kind": "experiment",
                      "created_at": e["finished_at"], "config": e["params"],
                      "url": f"/api/v1/experiments/{e['id']}/evidence.json", "provenance": "measured"})
    index = Path(core.EVIDENCE_DIR) / "index.json"
    if index.exists():
        for it in json.loads(index.read_text(encoding="utf-8")):
            items.append({k: it.get(k) for k in ("id", "title", "kind", "created_at", "config", "url", "provenance")})
    return items


# -- assistant ------------------------------------------------------------------------------------
class AssistBody(BaseModel):
    topic: Literal["alert", "architecture", "experiment", "viva"]
    ref_id: str | None = Field(default=None, max_length=80)
    question: str = Field(min_length=3, max_length=1000)


def _fmt(v, nd=3):
    return f"{v:.{nd}f}" if isinstance(v, float) else str(v)


async def _alert_context(p, ref_id: str | None) -> tuple[str, str, list]:
    if not ref_id or not ref_id.isdigit():
        raise ApiError(422, "ref_required", "ref_id (alert id) is required for topic 'alert'")
    a = await fetch_one("SELECT * FROM alerts WHERE id = %s", (int(ref_id),))
    if a is None:
        raise ApiError(404, "not_found", "alert not found")
    readable(p, await session_row(a["session_id"]))
    ev = await fetch_one("SELECT * FROM scored_events WHERE engine = %s AND event_id = %s", (a["engine"], a["event_id"]))
    d = (ev or {}).get("detail") or {}
    m = d.get("model") or {}
    top = ((m.get("explanation") or {}).get("top") or [])[:6]
    rules = [r for r in ((d.get("rules") or {}).get("results") or []) if r.get("fired")]
    policy = d.get("policy") or {}
    th = m.get("thresholds") or {}
    lines = [
        f"Alert {a['id']} ({a['engine']}): action={a['action']}, status={a['status']}, amount={a['amount']:.2f} (source units, no conversion), "
        f"source_ref={a['source_ref']}, pseudo-account={a['account_id']}",
        f"Model {m.get('version')}: score={_fmt(m.get('score'))}; alert threshold={_fmt(th.get('alert'))}, "
        f"review threshold={_fmt(th.get('review'))}. Score is a ranking score, not a calibrated probability.",
        "Top TreeSHAP contributions (log-odds; positive raises the score): "
        + "; ".join(f"{c['feature']}={c.get('value')} ({c['contribution']:+.3f})" for c in top),
        "Rules fired: " + ("; ".join(f"{r['rule_id']} {r.get('title')}: {r.get('evidence')}" for r in rules) or "none"),
        f"Policy {policy.get('policy_version')}: action={policy.get('action')}, reasons={policy.get('reasons')}",
        f"Late event: {(ev or {}).get('late')}; injected pattern (synthetic ground truth): {(ev or {}).get('pattern')}",
        f"Raw stream features: {json.dumps(d.get('raw_features'))}",
    ]
    fallback = "\n".join([
        f"This transaction was routed to **{a['action']}** by policy {policy.get('policy_version')}.",
        f"The model score was {_fmt(m.get('score'))} (alert threshold {_fmt(th.get('alert'))}, review threshold {_fmt(th.get('review'))}); "
        "it is a ranking score, not a probability of fraud.",
        "Largest model contributions: " + (", ".join(f"{c['feature']} ({c['contribution']:+.3f})" for c in top[:4]) or "not recorded") + ".",
        "Rules fired: " + (", ".join(r["rule_id"] for r in rules) or "none") + ".",
        f"Policy reasons: {', '.join(policy.get('reasons') or []) or 'not recorded'}.",
        "Treat this as decision support for a reviewer, not proof of fraud.",
    ])
    cites = [{"type": "alert", "id": str(a["id"]), "label": f"Alert {a['id']}"},
             {"type": "transaction", "id": a["event_id"], "label": "Transaction evidence"}]
    return "\n".join(lines), fallback, cites


def _project_facts() -> str:
    s = _results_file("stream_model_results.json") or {}
    r = _results_file("reference_model_results.json") or {}
    sel = s.get("selected")
    t = (s.get("test") or {}).get(sel) or {}
    at = t.get("at_threshold") or {}
    pol = s.get("test_policy") or {}
    return "\n".join([
        "Dataset: IEEE-CIS Fraud Detection (Kaggle, Vesta). Chronological 70/15/15 split. Stream-computable features only (fs-1.0).",
        f"Deployed model {s.get('model_version')} = {sel}; selection: {s.get('selection_rule')}.",
        f"Test PR-AUC={_fmt(t.get('pr_auc'))} (prevalence {_fmt(t.get('prevalence'), 4)}), ROC-AUC={_fmt(t.get('roc_auc'))}; "
        f"at alert threshold precision={_fmt(at.get('precision'))}, recall={_fmt(at.get('recall'))}, "
        f"all-legitimate baseline accuracy={_fmt(t.get('all_legitimate_baseline_accuracy'))}.",
        f"Policy on test: alert share={_fmt((pol.get('alert') or {}).get('share'))}, review share={_fmt((pol.get('review') or {}).get('share'))}, "
        f"flagged recall={_fmt(pol.get('flagged_recall'))}.",
        f"Full-feature offline reference ({r.get('n_features')} features, not deployable in the stream): test PR-AUC={_fmt((r.get('test') or {}).get('pr_auc'))}.",
        "CA2 semantics: 5 s bounded-out-of-orderness watermark; late = event_time < watermark, scored read-only and excluded from 60 s tumbling windows; "
        "state key session:account; at-least-once sink with idempotent upsert on (engine, event_id); Flink checkpoints every 10 s; "
        "Spark Structured Streaming micro-batch trigger 1 s with applyInPandasWithState.",
    ])


async def _experiment_context(ref_id: str | None) -> tuple[str, str, list]:
    if ref_id:
        e = await _experiment(ref_id)
        rows = [e]
    else:
        rows = await fetch_all("SELECT * FROM experiments ORDER BY created_at DESC LIMIT 5")
    if not rows:
        return "No experiments have been run yet; there are no measured benchmark results.", \
            "No experiments have been run yet, so there are no measured results to explain.", []
    parts, cites = [], []
    for e in rows:
        parts.append(f"Experiment {e['id']} kind={e['kind']} status={e['status']} params={json.dumps(e['params'])} "
                     f"results={json.dumps(e['results'])[:5000]} environment={json.dumps(e['environment'])[:800]}")
        cites.append({"type": "experiment", "id": e["id"], "label": f"{e['kind']} {e['id']}"})
    fallback = "\n".join(f"- {e['id']} ({e['kind']}): {e['status']}. Raw evidence: /api/v1/experiments/{e['id']}/evidence.json"
                         for e in rows)
    return "\n".join(parts), fallback, cites


@router.post("/assist")
async def assist(body: AssistBody, request: Request):
    limiter.check(request, "assist_min", 6, 60)
    limiter.check(request, "assist_hour", 40, 3600)
    p = principal(request)
    if body.topic == "alert":
        context, fallback, cites = await _alert_context(p, body.ref_id)
    elif body.topic == "experiment":
        context, fallback, cites = await _experiment_context(body.ref_id)
    else:
        h = await health_snapshot()
        context = _project_facts() + "\nLive component status: " + ", ".join(f"{c['id']}={c['status']}" for c in h["components"])
        fallback = ("The AI providers are unavailable, so here are the recorded project facts:\n" + _project_facts())
        cites = [{"type": "model", "id": core.MODEL_VERSION, "label": "Model results"},
                 {"type": "architecture", "id": "architecture", "label": "Architecture"}]
    out = await ai.answer(body.question, context, fallback)
    return out | {"citations": cites, "disclaimer": ai.DISCLAIMER}
