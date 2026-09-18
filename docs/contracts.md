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

## 7. System Intelligence & scenarios (API v1 additions)

All paths are under `/api/v1`. All times are epoch **milliseconds (UTC)**. `null` always means "not recorded / not instrumented" and is never replaced by `0`. Access: a *visitor* (session or view token) sees only rows of their own demo session plus public experiment sessions; the *operator* (`X-Operator-Token`) sees everything. Unauthenticated callers see experiment-session data only.

### 7.1 Scenario presets
* `GET /scenarios` → `ScenarioPreset[]` (public)
  `ScenarioPreset = { id: "everyday_traffic"|"transaction_burst"|"small_tests_then_large"|"spending_deviation"|"delayed_arrival"|"replay_duplicates", name, description, scenario: <fwcore scenario>, seed, max_events, rate_eps, engines: ["flink"], delivery: { duplicate_every: number|null }, expected: string[], evidence: string[], features: { model: string[], rules: string[], rule_only_fields: string[] }, bounds: { max_events, max_rate_eps }, caveats: string[] }`
  `expected` describes the input conditions the generator creates (never a promised model output); `features.model` lists only features the deployed LightGBM model consumes; `rule_only_fields` are used by explicit rules only and are never passed to the model.
* `POST /runs` now also accepts `{ preset?: string, scenario?: string, rate_eps?: number, max_events?: number, seed?: number, engines? }` — `preset` supplies defaults (`scenario`, `seed`, `rate_eps`, `max_events`, delivery options); explicit fields override; everything is clamped by the session caps (`DEMO_MAX_EVENTS_PER_SESSION`, `DEMO_MAX_EVENTS_PER_SECOND`). Without `preset`, `scenario` + `rate_eps` are required as before.

### 7.2 Run lifecycle, counters, exports
`Run` gains: `preset: string|null`, `options: { duplicate_every?: number }`, `lifecycle`, `counters` (populated on `GET /runs/{id}` and in exports; list endpoints return `counters: null`).
* `lifecycle: "queued"|"starting"|"running"|"paused"|"draining"|"completed"|"failed"|"cancelled"` — `queued` = experiment run created but not started yet; `draining` = generator finished but persisted < acknowledged for a target engine and the run ended less than `RUN_DRAIN_TIMEOUT_MS` (default 120000) ago; `cancelled` = stopped by a user; the raw `status` field is unchanged for compatibility.
* `RunCounters = { generated, attempts, acked, delivery_errors, duplicates_sent, engines: { <engine>: { processed: null, persisted, flagged, failed, late, duplicate_deliveries } }, not_instrumented: string[] }`
  `generated` = unique events produced, `attempts` = generated + duplicates_sent (Kafka produce calls), `acked` = broker delivery confirmations over all attempts (null for runs created before this counter existed), `persisted` = unique `(engine,event_id)` rows, `flagged` = action alert|review, `failed` = scoring_status ≠ ok, `duplicate_deliveries` = extra deliveries collapsed by the `(engine,event_id)` upsert. `processed` is `null` (engine-internal count not exported) and listed in `not_instrumented`.
* `GET /runs/{id}/export.json` → `{ run: Run, counters: RunCounters, events: ExportEvent[], alerts: Alert[], truncated: boolean, limit, exported_at }`
* `GET /runs/{id}/export.csv` → CSV with the `ExportEvent` columns (header row). Both rate-limited, same access as `GET /runs/{id}`, max 20000 events.
  `ExportEvent = { engine, event_id, seq, event_time, account_id, amount_inr, merchant_name, pattern, late, action, score, rules_fired, scoring_status, duplicates, produced_at, kafka_ts, engine_received_at, scoring_completed_at, persisted_at, e2e_ms }`

### 7.3 Structured logs
`LogEntry = { id: string, timestamp_utc: number, observed_at: number, severity: "debug"|"info"|"warning"|"error"|"critical", service: "api"|"generator"|"scoring"|"result_consumer"|"ops_agent", environment, session_id, run_id, event_id, trace_id, engine, job_id, stage, status, duration_ms, message, error_code, retry_attempt, payload: object|null }`
`timestamp_utc` = when the logged thing happened as known by the service; `observed_at` = when the log row was written. Engine lifecycle (Flink job restarts/checkpoints, Spark start/stop/batches) is logged by `ops_agent` with `engine` set. Secrets (keys, tokens, passwords, connection strings) are redacted before storage; retention is bounded (`LOG_RETENTION_MS`, default 7 days, plus `LOG_MAX_ROWS`).
* `GET /system/logs?from&to&severity&service&engine&session_id&run_id&event_id&q&limit&cursor` → `{ items: LogEntry[] (newest first), next_cursor: string|null }` — `severity` is a minimum level; `service`/`engine` accept comma lists; `q` = case-insensitive substring on message/error_code; `limit` ≤ 500 (default 100).
* `GET /system/logs/stream?<same filters>` → SSE, oldest-first. `event: log` (`id` = LogEntry.id, `data` = LogEntry); `event: behind` `{ skipped: number }` when more than 500 rows arrived since the last delivery (the viewer is behind: reload history via `/system/logs`); `event: ping` `{ at }` every 15 s. Resume with the `Last-Event-ID` header or `?after=<id>`; without either the stream starts at "now".
* `GET /system/logs/export.csv?<same filters>` → CSV (≤ 5000 rows, `payload` column JSON-encoded, sanitized). Rate-limited.

### 7.4 Trace explorer
* `GET /system/trace/{event_id}` → `{ event_id, session_id, run_id, seq, engines: { flink: EngineTrace|null, spark: EngineTrace|null }, client_received: { at_ms, source: "browser", note }|null, logs: LogEntry[] }`
  `EngineTrace = { stages: [{ stage, at_ms: number|null, duration_ms: number|null, status: "ok"|"failed"|"missing"|"not_applicable", detail: string|null }], missing: string[] }`
  Stage order: `generated` (`produced_at`, immediately before Kafka produce) → `kafka_acked` (broker LogAppendTime) → `engine_received` → `features_ready` (= scoring request sent; features are complete at that point) → `scored` (scoring response; `detail` has attempts/error) → `engine_emitted` → `persisted` → `alert_evaluated` (alert row creation for alert/review; `not_applicable` for allow). `duration_ms` = difference from the previous stage that has a timestamp. An engine that has no result for the event is `null`. `client_received` comes only from `POST /system/client-receipt` and is browser clock time — it is never mixed into server-side delays. 404 when neither engine persisted the event.
* `POST /system/client-receipt` `{ event_id, received_at_ms }` (visitor token or operator; rate-limited) → `{ recorded: true }`

### 7.5 Data quality
* `GET /system/data-quality?run_id` → `{ scope: { run_id, session_id, from, to }, counts: { malformed, schema_mismatch, missing_fields, duplicates, processing_failures, late, unmatched, stale }, definitions: { <count key>: string }, reconciliation: [{ run_id, engine, generated, acked, persisted, missing, duplicates, state: "running"|"draining"|"reconciled"|"incomplete" }], dlq: { items: [{ id, engine, error, reason: "malformed"|"schema_mismatch"|"missing_fields"|"processing_failure", received_at, persisted_at, kafka: object|null, raw_excerpt }], restricted: boolean } }`
  Without `run_id` the scope is the caller's session (visitor) or the last 24 h (operator). DLQ records carry no session attribution, so `dlq.items` and the DLQ-derived counts (`malformed`, `schema_mismatch`, `missing_fields`) are operator-only; visitors get `null` for those counts and `restricted: true`. `duplicates` counts extra deliveries (retries never inflate unique transactions); `unmatched` counts events persisted by only one engine in runs targeting both; `stale` counts acknowledged events with no persisted result after the drain timeout. `reconciliation` lists at most 50 runs.

### 7.6 Model & stream operations
* `GET /system/operations` → `{ as_of, model: { version, checksum: string|null, feature_schema, features: string[], thresholds: {alert, review}, inference_errors: number|null, inference_ms: { p50, p95, n, window_ms, boundary }|null }, flink: { job_id, state, checkpoint_age_ms, last_checkpoint, restarts, watermarks: [{vertex, current_input_watermark_ms}]|"not_instrumented", backpressure: [{vertex, level, ratio}]|"not_instrumented" }, spark: { state, app_id, run_label, last_batch_id, last_batch_at, batch_duration_ms, input_rows_per_second, processed_rows_per_second, trigger }, kafka_lag: [{ group, topic, partition, committed, end_offset, lag }], kafka_lag_notes: string[], incidents: [{ id, severity: "warning"|"critical", title, detail, since, source }] }`
  Public read (no secrets or per-session data). Unavailable values are `null` or the literal string `"not_instrumented"`.

### 7.7 Experiment results (ops-agent)
* comparison `results = { profile, rate_eps, events_per_run, repeats, seed, trigger_interval, warmup: { run_id, events, excluded: true }, parity: { run_id, both, only_flink, only_spark, action_agree, action_disagree, raw_feature_mismatch, late_disagree }, engines: { flink: EngineResult, spark: EngineResult }, runs: [{ run_id, engine, repeat, order, sent, persisted, missing, duplicates, late, failed_scoring, latency_ms, offered_eps, processed_eps, duration_ms, drain: "complete"|"stable"|"timeout" }], resources: { samples: [{at, container, cpu_pct, mem_mb}] }, delay_boundary, notes: string[], winner: null }`
  `EngineResult` = the section 6 comparison shape (`offered_eps, processed_eps, sent, persisted, missing, duplicates, failed_scoring, late, latency_ms: {p50, p95, p99, mean, n, interval_ms}, backlog_max, cpu_pct, mem_mb, repeats`) pooled over the timed repeats (warm-up and parity runs excluded).
* late_events `results = { run_id, rate_eps, events, watermark_bound_ms, planned_late, engines: { <engine>: { persisted, late, late_planned, late_unplanned, planned_not_late, windows_late_dropped, examples } } }`
* recovery `results = { run_id, rate_eps, events, before: { job_id, state, restarts, last_checkpoint }, kill: { at, container, signal }, restart: { restarted_by: "docker_restart_policy"|"ops_agent", at }, after: { state, restarts, running_at, first_new_checkpoint, restored_from, restore_ms, recovery_ms }, integrity: { sent, persisted, missing, duplicate_deliveries, drain } }`
* `Experiment.progress = { phase, message, step, total_steps, run_id?, cancel_requested? }`; `Experiment.environment = { versions, resources, trigger_interval, host, notes }`.
