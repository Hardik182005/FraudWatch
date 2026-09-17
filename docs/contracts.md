# FraudWatch contracts (v1)

The single source of truth that the streaming jobs, Python services and both websites build against.
Python reference code: `packages/fwcore/fwcore/` (schema, features, rules, policy, simulation).

All times are **integer epoch milliseconds (UTC)**. Amounts are INR (`amount_inr`, 2 decimals).

---

## 1. Kafka

| Topic | Partitions | Key | Notes |
|---|---|---|---|
| `fraudwatch.transactions` | 4 | `demo_session_id:account_id` (heartbeats: none, explicit partition) | `message.timestamp.type=LogAppendTime` → record timestamp = broker ingestion time |
| `fraudwatch.results.flink` | 4 | `event_id` | scored records from Flink |
| `fraudwatch.results.spark` | 4 | `event_id` | scored records from Spark |
| `fraudwatch.windows.flink` | 1 | `engine:session:window_start` | finalized 1-min windows |
| `fraudwatch.windows.spark` | 1 | same | |
| `fraudwatch.dlq` | 1 | none | invalid input records (both engines) |
| `fraudwatch.metrics.spark` | 1 | none | Spark `StreamingQueryProgress` JSON (one per micro-batch) |

Bootstrap (inside Compose): `kafka:9092`. Consumer groups: Flink `fw-flink-v1`; result consumer `fw-result-consumer-v1`. Spark tracks offsets in its checkpoint (`groupIdPrefix=fw-spark`).
Topic creation is done by the `kafka-init` Compose service (owned by services workstream).

## 2. Input event (`fraudwatch.transactions`)

Transaction (`kind="txn"`): see `fwcore/schema.py` (`JSON_SCHEMA`, `validate_event`). Required fields:
`schema_version="1.0"`, `kind`, `event_id` (uuid5 of `run_id:seq`), `demo_session_id`, `run_id`, `seq`, `account_id`,
`event_time`, `produced_at`, `amount_inr`, `product_cd`, `card_network`, `card_type`, `email_domain`, `device_type`,
`has_identity` (bool), `target_engines` (array of `"flink"|"spark"`).
Optional display-only fields: `merchant_id`, `merchant_name`, `merchant_category`, `city`, `scenario`, `pattern`, `injected`, `arrival_delay_ms`.

Heartbeat (`kind="heartbeat"`): `{"schema_version":"1.0","kind":"heartbeat","event_time":<ms>,"produced_at":<ms>,"generator_id":"..."}`
Sent once per second to **every partition** so event-time watermarks advance when traffic is idle. Heartbeats update watermarks only; they never reach keyed state, scoring, outputs or windows.

An engine processes a txn only if its name is in `target_engines`. Invalid records (JSON or validation failure — same rules as `validate_event`) go to `fraudwatch.dlq` and are counted.

## 3. Processing semantics (identical in Flink and Spark)

* **Event time** = `event_time`. **Watermark** = bounded out-of-orderness **5 s**. (Flink: `forBoundedOutOfOrderness(5s)` + idleness 30 s. Spark: `withWatermark("event_ts", "5 seconds")`.)
* **State key** = `demo_session_id + ":" + account_id`. Arrival order per key = Kafka offset order (same key → same partition).
* **Features**: exactly `fwcore.features.compute_raw` then `update` (read `features.py`; port faithfully — iterate entries in insertion order, incremental mean `mean += (x-mean)/n`, prune rule, `MAX_ENTRIES=500`, `secs_since_last` from `max_ts`). Engines send **raw** features; the scoring service does all transforms.
* **Late policy**: `late = event_time < current_watermark` (Flink: `ctx.timerService().currentWatermark()` at the keyed operator; Spark: `GroupState.getCurrentWatermarkMs()` for the micro-batch). A late event is **scored with a read-only view of state** (compute_raw, no update) and `late=true`; it is excluded from windows. Flink's watermark advances per record; Spark's watermark is fixed per micro-batch — boundary cases may legitimately differ and must be reported, not hidden.
* **Scoring**: `POST {SCORING_SERVICE_URL}/v1/score` (section 4). Retries: 3 attempts total, 200 ms fixed backoff, 3 s timeout per attempt. After final failure emit the record with `scoring.status="failed"` (never drop it, never invent a score).
* **Windows**: 60 s tumbling event-time windows keyed by `demo_session_id` over **non-late** scored records; allowed lateness 0; emitted once when finalized (Flink: event-time trigger; Spark: `append` output mode on a second query reading `fraudwatch.results.spark`). Records late at the window operator are counted (Flink side output counter; Spark `numRowsDroppedByWatermark`).
* **Guarantees**: source offsets + state are checkpointed; sink is at-least-once; the result consumer upserts on `(engine, event_id)` and counts duplicates. Scoring is an external side effect and may be repeated on replay (it is deterministic for the same inputs).
* Flink: checkpoint every 10 s (EXACTLY_ONCE mode for state), retained on cancellation, filesystem storage `file:///opt/flink/checkpoints` on a shared volume, fixed-delay restart (10 attempts, 5 s), parallelism 2.
* Spark: `trigger(processingTime=SPARK_TRIGGER_INTERVAL)` (default `1 second`), explicit micro-batch mode, checkpoint dir `SPARK_CHECKPOINT_URI` (per launch label), `local[2]`.

## 4. Scoring service (`services/scoring`, port 8001, internal only)

`POST /v1/score`
```json
{ "engine": "flink",
  "items": [ { "event": { "...full txn event..." },
               "raw": { "amount_inr": 1234.5, "prior_count_60s": 0, "prior_count_10m": 0, "prior_count_1h": 0,
                        "prior_amount_sum_1h": 0.0, "small_count_10m": 0, "secs_since_last": null,
                        "prior_count_total": 0, "prior_mean_amount": null, "amount_ratio": null },
               "late": false, "watermark_ms": 1726560000000 } ] }
```
≤ 500 items. Response `200`:
```json
{ "model_version": "fw-ieee-stream-1.0.0",
  "items": [ { "event_id": "...",
      "model": { "version": "fw-ieee-stream-1.0.0", "score": 0.0312,
                 "thresholds": { "alert": 0.121, "review": 0.069 },
                 "explanation": { "method": "treeshap_log_odds", "base_value": -3.1,
                                  "top": [ { "feature": "prior_count_60s", "value": 5.0, "contribution": 0.83 } ] } },
      "rules": { "version": "rules-1.0", "results": [ { "rule_id": "R1_BURST", "title": "Rapid burst", "fired": true, "evidence": "..." } ] },
      "policy": { "policy_version": "policy-1.0", "action": "alert", "reasons": ["..."] },
      "model_inputs": { "log_amount": 7.1, "...": "..." },
      "scored_at": 1726560000123, "scoring_ms": 1.8 } ] }
```
`action ∈ {alert, review, allow, unscored}`. `400` invalid payload (body explains), `503` model not loaded. `GET /healthz`, `GET /v1/model` (metadata).

## 5. Engine output record (`fraudwatch.results.<engine>`)

```json
{ "schema_version": "1.0", "record_type": "scored_txn", "engine": "flink",
  "engine_job_id": "<flink job id | spark app id>", "engine_run_label": "<optional launch label>",
  "event": { "...original txn event verbatim..." },
  "kafka": { "partition": 2, "offset": 1234, "timestamp": 1726560000005 },
  "late": false, "watermark_ms": 1726559995000,
  "raw_features": { "...compute_raw output..." },
  "scoring": { "status": "ok", "attempts": 1, "error": null, "response": { "...one scoring item..." } },
  "timing": { "engine_received_at": 0, "scoring_started_at": 0, "scoring_completed_at": 0, "emitted_at": 0 },
  "spark": { "batch_id": 17 } }
```
`watermark_ms` is `null` when the engine has no watermark yet. `spark` block only for Spark. `engine_received_at` = when the engine operator first handled the record (Spark: micro-batch start time).

Window record (`fraudwatch.windows.<engine>`):
```json
{ "schema_version": "1.0", "record_type": "window", "engine": "flink", "demo_session_id": "...",
  "window_start": 0, "window_end": 60000, "txn_count": 42, "amount_sum_inr": 51234.5,
  "alert_count": 2, "review_count": 3, "unscored_count": 0, "emitted_at": 0 }
```
DLQ record: `{ "engine": "flink", "error": "missing field: seq", "raw": "<first 4096 chars>", "kafka": {...}, "received_at": 0 }`

## 6. Public HTTP API (`services/api`, base path `/api/v1`)

Auth headers: `Authorization: Bearer <session token | view token>`; operator: `X-Operator-Token: <token>`.
EventSource cannot set headers → SSE uses `?token=`. Errors: `{"error": {"code": "string", "message": "string"}}` with proper status (401, 403, 404, 409 conflict, 422 validation, 429 rate/caps, 503 dependency down).

### Config & sessions
* `GET /config` → `{ live_url, lab_url, demo_max_events_per_session, demo_max_events_per_second, ai_enabled, ai_provider_order, model_version, scenarios: [...], patterns: [...] }`
* `POST /sessions` `{label?}` → `201 { session: Session, token }`
* `GET /sessions/current` (session token) → `Session`
* `GET /sessions/{id}` (owner session, view token for that session, or operator) → `Session`
* `POST /sessions/current/reset` → stops active runs and issues a **new** session `{ session, token }` (old data kept, isolated)
* `POST /sessions/{id}/share` (owner) → `{ view_token, expires_at, lab_url, live_url }` — read-only cross-site link: `?session=<id>&vt=<view_token>&run=<run_id>&engine=flink&action=alert`
* `POST /operator/login` `{passphrase}` → `{ token, expires_at }` (rate limited)

`Session = { id, label, created_at, expires_at, status: "active"|"expired", event_cap, events_used, rate_cap_eps, active_run_id }`

### Runs & scenario controls
* `POST /runs` `{ scenario, rate_eps, max_events?, seed?, engines? }` → `201 Run` (visitors: engines fixed to `["flink"]`, caps enforced; operator may add `"spark"`)
* `GET /sessions/{id}/runs` → `Run[]`;  `GET /runs/{id}` → `Run`
* `POST /runs/{id}/pause` | `/resume` | `/stop` → `Run`
* `POST /runs/{id}/rate` `{rate_eps}` → `Run`
* `POST /runs/{id}/inject` `{pattern: "burst"|"profile_deviation"|"card_testing"|"late_burst"}` → `{ pattern, account_id, events, injection }`
* `POST /runs/{id}/replay` → `201 Run` (new run id, same seed/config/control log, `replay_of` set)

`Run = { id, session_id, scenario, seed, rate_eps, engines, status: "starting"|"running"|"paused"|"completed"|"stopped"|"failed", max_events, events_sent, started_at, ended_at, replay_of, control_log: [{at_base_index, action, value}], created_by: "visitor"|"operator"|"experiment", error }`

### Live data
* `GET /sessions/{id}/overview?engine=flink&run_id=` →
  `{ as_of, engine, counts: {scored, alert, review, allow, unscored, late}, amount_flagged_inr, throughput_eps_1m, latency_ms: {p50, p95, p99, n, window_s}, last_persisted_at, series: [{t, scored, flagged}], windows: WindowAgg[] }`
  (`amount_flagged_inr` = amount routed to alert/review — **not** confirmed losses or money saved.)
* `GET /sessions/{id}/transactions?engine=&run_id=&action=&late=&pattern=&limit=50&cursor=` → `{ items: TxnSummary[], next_cursor }`
* `GET /transactions/{event_id}` → `{ event, engines: { flink?: TxnDetail, spark?: TxnDetail }, trace: TraceStep[] }`
* `GET /sessions/{id}/stream?token=&cursor=` — SSE. Every message has `id: <cursor>`; supports `Last-Event-ID`. Event types: `txn` (TxnSummary), `alert` (Alert), `window` (WindowAgg), `run` (Run), `health` (Health, every 10 s), `ping` (`{as_of}`, every 15 s), `reset` (`{reason}` → client refetches snapshot). Server ends the stream after ~5 min; client reconnects with backoff (1 s → 30 s max) and the last cursor.

`TxnSummary = { event_id, engine, session_id, run_id, seq, account_id, event_time, amount_inr, merchant_name, merchant_category, city, action, score, late, pattern, injected, rules_fired: string[], persisted_at, e2e_ms }`
`TxnDetail = { engine, engine_job_id, kafka, late, watermark_ms, raw_features, model_inputs, model, rules, policy, scoring: {status, attempts, error}, timing: {produced_at, kafka_ts, engine_received_at, scoring_started_at, scoring_completed_at, emitted_at, persisted_at}, duplicates }`
`TraceStep = { stage: "generated"|"kafka_ingest"|"engine_received"|"scoring"|"engine_emitted"|"persisted", engine: string|null, at_ms, delta_ms, detail }`
`WindowAgg = { engine, session_id, window_start, window_end, txn_count, amount_sum_inr, alert_count, review_count, unscored_count, emitted_at, persisted_at }`

### Alerts & audit
* `GET /sessions/{id}/alerts?status=&action=&engine=flink&min_score=&limit=50&cursor=` → `{ items: Alert[], next_cursor, counts_by_status }`
* `PATCH /alerts/{id}` `{ status, note }` (owner session or operator) → `Alert`; appends an audit entry. Decisions never retrain the model.
* `GET /alerts/{id}/audit` → `AuditEntry[]`

`Alert = { id, event_id, engine, session_id, run_id, account_id, amount_inr, merchant_name, action: "alert"|"review", score, rules_fired, created_at, status: "open"|"acknowledged"|"escalated"|"dismissed"|"confirmed_fraud_demo", decision_note, updated_at }`
`AuditEntry = { id, at, actor: "visitor:<session>"|"operator", action, entity_type, entity_id, from_status, to_status, note, prev_hash, hash }` (hash-chained)

### Model (CA1)
* `GET /model` → deployed model metadata (version, type, features, thresholds, dataset manifest, environment)
* `GET /model/results` → `{ stream: <ml/results/stream_model_results.json>, reference: <reference_model_results.json | null> }`
* `POST /model/what-if` `{ event: {amount_inr, product_cd, card_network, card_type, email_domain, device_type, has_identity}, raw: {...compute_raw fields...} }` → scoring item + `disclaimer` (sensitivity, not causality). Rate limited.

### Health & architecture
* `GET /health` → `{ as_of, overall: "ok"|"degraded"|"down", components: Component[] }`
  `Component = { id: "api"|"postgres"|"kafka"|"generator"|"scoring"|"result_consumer"|"flink"|"spark", status: "ok"|"degraded"|"down"|"idle"|"unknown", detail, checked_at, metrics: object }`
  Flink metrics: `{ job_id, job_state, uptime_ms, restarts, last_checkpoint: {id, completed_at, duration_ms, state_size} }`; Spark: `{ state: "idle"|"running", app_id, run_label, last_batch_id, last_batch_at }`
* `GET /architecture` → `{ nodes: [{ id, label, role, config: {k: v}, status }], edges: [{ from, to, label }] }` (ids: `generator, kafka, flink, spark, scoring, result_consumer, postgres, api, live, lab`)

### Engine comparison, late events, experiments (CA2 / Lab)
* `GET /runs/{id}/comparison` → `{ run_id, engines: { flink: EngineStats, spark: EngineStats }, matching: { both, only_flink, only_spark, action_agree, action_disagree, raw_feature_mismatch, late_disagree }, disagreements: [{event_id, flink_action, spark_action, flink_late, spark_late}] }`
  `EngineStats = { count, late, actions: {alert, review, allow, unscored}, duplicates, latency_ms: {p50, p95, p99, n}, first_persisted_at, last_persisted_at }`
* `GET /runs/{id}/late` → `{ engines: { flink: {late, windows_late_dropped, examples: [{event_id, event_time, watermark_ms, lag_ms, pattern}]}, spark: {...} } }`
* `GET /experiments` → `Experiment[]` (public read); `GET /experiments/{id}` → `Experiment` with `results`
* `GET /experiments/{id}/raw.csv` (per-event samples), `GET /experiments/{id}/evidence.json`
* `POST /experiments` (operator) `{ kind: "comparison"|"late_events"|"recovery", params }` → `202 Experiment`; `POST /experiments/{id}/cancel` (operator)
* `POST /ops/spark/start` `{ session_id?, run_id?, mode: "live"|"replay" }`, `POST /ops/spark/stop` (operator) → Component

`Experiment = { id, kind, status: "queued"|"running"|"completed"|"failed"|"cancelled", params, created_at, started_at, finished_at, created_by, run_ids, results: object|null, error, environment: object }`
Comparison `results.engines.<engine>` = `{ offered_eps, processed_eps, sent, persisted, missing, duplicates, failed_scoring, late, latency_ms: {p50, p95, p99, mean, n, interval_ms}, backlog_max, cpu_pct, mem_mb, repeats: [...] }` — only real measurements; before any run the list is empty.

### Evidence & AI
* `GET /evidence` → `[{ id, title, kind: "model"|"experiment"|"test"|"deployment"|"report", created_at, config, url, provenance: "measured"|"prerecorded"|"illustrative" }]`
* `POST /assist` `{ topic: "alert"|"architecture"|"experiment"|"viva", ref_id?, question }` → `{ provider: "groq"|"gemini"|"openai"|"deterministic", model, answer, citations: [{type, id, label}], disclaimer }` (rate limited; deterministic fallback when no key / all providers fail).
