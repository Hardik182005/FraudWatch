# Benchmark methodology

## Status first: what has and has not been measured

**A fair Flink-vs-Spark comparison has now been run on the deployed GCP stack.** The results, with the
run ids, live in [`measured-results.md`](measured-results.md). In short: over one 500-record run that
both engines consumed, the two engines agreed on 500/500 decisions with a maximum score difference of
`0.000000000`, and Flink's end-to-end latency was p50 92 ms / p95 155 ms against Spark's p50 3862 ms /
p95 5695 ms at a 1000 ms trigger.

Two caveats that keep the rest of this document necessary:

1. `services/ops-agent/app/` is still an **empty directory**. The experiment runner that
   `POST /api/v1/experiments` is specified against does not exist, so the run above was orchestrated by
   hand (insert a `runs` row, then `POST /runs/{id}/start` on the generator). It is evidence, but it is
   not yet a button in the UI, and `comparison`, `late_events` and `recovery` experiments cannot be
   launched from the API.
2. The comparison measures **latency at a fixed low arrival rate** (10 events/second), not throughput.
   Sustained events/second at saturation, p99 latency, and behaviour under backpressure remain:

> **`_not measured yet_`**

Anything in this repository that looks like a performance number and is not in
[`measured-results.md`](measured-results.md) is a schema example in [`contracts.md`](contracts.md) or a
UI placeholder — not a measurement.

The rest of this document describes *how* a comparison must be run for its output to mean anything, so
that numbers can be trusted — and so that nobody publishes numbers produced any other way.

## 1. What "fair" has to mean here

A stream-engine comparison is only interesting if the two engines differ in *exactly one* respect: the
engine. This repository is built so that everything else can be held identical.

### Identical input events

Both engines read the **same Kafka topic**, `fraudwatch.transactions`, with
`message.timestamp.type=LogAppendTime`. Not two generator runs, not two topics, not a replayed copy:
the same records, the same partitions, the same offsets, the same broker-assigned timestamps.

Consumption is independent, so one engine cannot starve the other: Flink uses its own consumer group
(`FLINK_GROUP_ID`, default `fw-flink-v1`) while Spark's Structured Streaming Kafka source tracks offsets
in its own checkpoint and does not join a shared group. Neither engine's progress removes work from the
other.

### Identical event IDs

Matching is by `event_id`, which is assigned by the generator before the produce call. That is what
`GET /api/v1/runs/{id}/comparison` joins on — a `FULL OUTER JOIN` on `event_id` between the `flink` and
`spark` rows of the same `run_id` — which is how `both`, `only_flink` and `only_spark` can be counted at
all. Without a shared, producer-assigned id there is no way to prove the two engines saw the same
event.

### Identical model and artifact

Both engines call the **same running scoring service** at `http://scoring:8001`, which loads the single
committed artifact `fw-ieee-stream-1.1.0`. Same weights, same thresholds, same ruleset. Neither engine
scores locally. This deliberately moves the model out of the comparison: the measurement is about
stream processing, not about inference.

The retry budget is also identical by construction: 3 attempts, 200 ms backoff, 3 s per-attempt timeout
in both engines. Flink's async operator adds a 15 s outer timeout above that budget; Spark batches into
chunks of ≤ 500 with `SPARK_SCORING_CONCURRENCY` (default 2) parallel chunks. Those are honest
architectural differences and part of what is being compared — they are not confounders to be hidden,
but they must be stated alongside any result.

### Identical feature computation

The feature code is the same logic on both sides: `fwcore` (Python) in Spark and the scoring service, a
Java port in Flink. The Flink image build runs `FeatureParityTest` against `tests/streaming/fixtures`
and **fails the build** if they diverge, and `prepare_manifest.json` records an offline parity check at
`max_relative_diff` 3.668121910221923e-13 over 405 uids / 34,801 events.

At comparison time this is re-verified per event rather than assumed:
`GET /runs/{id}/comparison` returns `raw_feature_mismatch`, comparing the stored
`detail->'raw_features'` of the two engines for every event both persisted. A run with a non-zero
`raw_feature_mismatch` is **not** a valid performance comparison — the engines did not compute the same
thing, so fix that before reading any timing.

### Aligned event-time assumptions

Both engines use 5 s of allowed out-of-orderness (Flink `forBoundedOutOfOrderness(5 s)`, Spark
`withWatermark("event_ts", "5 seconds")`) and the same 60 s tumbling event-time window. Lateness is
decided against each engine's own watermark and reported per event, and
`GET /runs/{id}/comparison` returns `late_disagree` so a run where the two engines classified different
events as late is visible rather than silently averaged in.

Watermark mechanics still differ: Flink has `withIdleness(30 s)` and emits watermarks every 200 ms
(`pipeline.auto-watermark-interval`), while Spark advances its watermark per micro-batch and relies on
heartbeats being routed *through* the stateful operator to keep it moving. Any latency comparison at
low event rates is partly measuring that difference, not throughput.

## 2. What p95 means in this project

The only latency figure this system records is `e2e_ms`, computed in
`services/result-consumer/app/main.py`:

```python
"persisted_at": persisted_at, "e2e_ms": persisted_at - int(ev["produced_at"]),
```

- `produced_at` is stamped by the generator immediately before the Kafka produce call.
- `persisted_at` is taken by the result consumer immediately before its batch `INSERT`, and is the
  **same value for every record in that batch**.

So `e2e_ms` is **producer-to-database wall-clock latency for the whole path**, and it includes:

Kafka produce and broker append → engine consume → keyed state and feature computation → HTTP round
trip to the scoring service (including any retries) → result produce to the engine's results topic →
result-consumer consume → batching delay → the instant before the insert.

It is explicitly **not**:

- engine-internal processing time,
- time to first byte of any kind,
- scoring latency (the API exposes that separately as
  `GET /system/operations` → `model.inference_ms`),
- window emission latency,
- anything a browser sees (`POST /system/client-receipt` records browser clock time and
  `contracts.md` states it "is never mixed into server-side delays").

p50/p95/p99 are computed in PostgreSQL with `percentile_cont(...) WITHIN GROUP (ORDER BY e2e_ms)` over
the persisted rows of a run, grouped by engine, and returned with their sample size `n`
(`services/api/app/routes/runs.py`). Consequences worth stating with any figure:

- **Only persisted events count.** An event that was dropped, DLQ'd or never scored contributes nothing
  to the percentile — so latency must always be read next to `sent`, `persisted` and `missing`.
- **Result-consumer batching is inside the number.** A larger batch inflates `e2e_ms` for the earliest
  record in it. Two engines with different emission patterns will be batched differently.
- **One clock, no skew problem, but a shared endpoint.** `produced_at` and `persisted_at` are both host
  clocks inside the same Compose stack, so there is no cross-machine skew — but the result consumer is
  a single shared component, so its backlog is charged to both engines.

## 3. Requirements for a run to count

A comparison result should be published only if all of these hold, and each should be recorded next to
the numbers:

1. `raw_feature_mismatch == 0`.
2. `missing == 0` for both engines, or the missing count is reported. `contracts.md` defines drain
   states (`complete` / `stable` / `timeout`) precisely so an unfinished run is not read as a fast one;
   a run still in `draining` lifecycle is not finished.
3. A **warm-up run is excluded**. JVM JIT, Python worker start-up, and the first Spark micro-batch are
   not steady state. `contracts.md` specifies `warmup: { run_id, events, excluded: true }` for exactly
   this.
4. **Repeats**, with the pooled result reported together with the per-repeat rows, and the run
   **order** recorded — the second engine to run benefits from warm OS page cache.
5. **Offered rate vs processed rate reported separately** (`offered_eps` vs `processed_eps`). If the
   generator could not sustain the offered rate, you measured the generator.
6. **Resource limits stated.** These are 11 GB of `mem_limit`s on one machine, Spark in `local[2]` with
   a 1400 MB driver, Flink with a single TaskManager and 2 task slots, Kafka with `-Xmx1g`. Both
   engines are contending for one host's CPU, and the generator, scoring service, result consumer and
   PostgreSQL are contending with them.
7. **Sample size `n` shown** with every percentile.
8. **Versions pinned in the output**: Flink 1.20.5, Spark 3.5.9, Kafka 4.1.2, the model version, and
   `SPARK_TRIGGER_INTERVAL` — Spark's trigger interval alone sets a floor on its latency and must never
   be omitted.

## 4. What such a result would and would not prove

It **would** support a statement of this shape:

> "On a single-node Docker Compose stack of the configuration in `apache-stack.md`, at N events/s over
> M events with K repeats, Flink's p95 producer-to-database latency was A ms and Spark's was B ms, with
> zero missing events and zero raw-feature mismatches, Spark trigger interval 1 s."

It would **not** support:

- "Flink is faster than Spark." Different deployment, sizing, parallelism, trigger interval, sink
  configuration, or state backend can reverse the ordering. A single-node Compose stack with
  `local[2]` is close to the worst case for Spark, which is designed to scale out.
- Any statement about engine-internal processing time (section 2).
- Any extrapolation to another rate, another payload, a cluster, or managed Flink/Spark.
- Any claim about model quality — the model is identical on both sides, on purpose.
- Any "winner". `contracts.md` already encodes this: the comparison result carries
  `winner: null`.

A comparison here is a **demonstration that two engines produce the same decisions on the same stream
with the same model**, with the timing reported as a property of *this* deployment. The interesting
output is `action_agree` / `action_disagree` / `raw_feature_mismatch` / `late_disagree`, not a
leaderboard.

## 5. The measurement surfaces that do exist

These are implemented and will work as soon as both engines are running — they are how a comparison
would be read:

| Endpoint | What it gives |
|---|---|
| `GET /api/v1/runs/{id}/comparison` | per-engine counts, actions, duplicates, `latency_ms {p50,p95,p99,n}`, plus `matching` (`both`, `only_flink`, `only_spark`, `action_agree`, `action_disagree`, `raw_feature_mismatch`, `late_disagree`) and up to 100 concrete disagreements |
| `GET /api/v1/runs/{id}/late` | per-engine late counts, `windows_late_dropped`, and up to 25 examples with `event_time`, `watermark_ms` and `lag_ms` |
| `GET /api/v1/runs/{id}` → `counters` | `generated`, `attempts`, `acked`, `duplicates_sent`, and per engine `persisted`, `flagged`, `failed`, `late`, `duplicate_deliveries` |
| `GET /api/v1/system/data-quality` | `malformed`, `duplicates`, `late`, `unmatched`, `stale`, plus a per-run reconciliation table |
| `GET /api/v1/system/operations` | Flink checkpoint age / restarts / watermarks, Spark batch id and duration and input/processed rows per second, Kafka consumer lag, and `model.inference_ms` |
| `GET /api/v1/system/trace/{event_id}` | the per-stage timeline of one event through each engine |
| `fraudwatch.metrics.spark` | every raw `StreamingQueryProgress` JSON from Spark's own listener |

Note two honest details in that design. `RunCounters.engines.<engine>.processed` is **always `null`**
and listed in `not_instrumented` — the engine-internal processed count is not exported, so it is never
faked as `0`. And per `contracts.md`, `null` throughout the API "always means 'not recorded / not
instrumented' and is never replaced by `0`", while `"not_instrumented"` appears as a literal string for
Flink watermarks and backpressure when they are unavailable. Read a `null` as "we did not measure
this", never as zero.

## 6. To make measurements possible

1. Implement the experiment runner in `services/ops-agent/app/` to the `contracts.md` section 7.7
   contract (`comparison`, `late_events`, `recovery`), and set `OPS_AGENT_URL`.
2. Start the `spark` profile so both engines consume the same topic.
3. Run a warm-up, then repeats, and record every item in section 3 alongside the result.
4. Store the output as evidence with `provenance: "measured"` (`GET /evidence` distinguishes
   `measured` / `prerecorded` / `illustrative` — use it honestly).
5. Only then replace `_not measured yet_` anywhere in this repository, and record the run id in
   [`measured-results.md`](measured-results.md) alongside the numbers.
