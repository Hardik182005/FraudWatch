# Measured results (GCP, 2026-09-18)

Every number on this page came from a run on the deployed GCP stack
(VM `fraudwatch-pipeline`, `asia-south1-a`) and was read back out of Postgres or the Flink REST API.
Nothing here is estimated, extrapolated or illustrative. Where something was **not** measured, this
page says so instead of guessing.

How to reproduce each run is in [`benchmark-methodology.md`](benchmark-methodology.md). The runs below
were orchestrated **manually** (a row inserted into `runs`, then `POST /runs/{id}/start` on the
generator with the internal token), not through the `services/ops-agent/` experiment runner, which is
still not implemented. That does not change what was measured — both engines consumed the same Kafka
records — but it does mean these runs are not yet reproducible by pressing a button in the UI.

## 1. Flink vs Spark parity — run `r_d48339d70bb9ceb4`

Identical input: one generator run, 500 IEEE-CIS replay records, scenario `replay`, seed `20260917`,
10 events/second, `engines = ARRAY['flink','spark']`. Both engines read the same
`fraudwatch.transactions` records; Flink via its own consumer group, Spark via its Structured
Streaming checkpoint. Spark trigger interval 1000 ms.

| Engine | Rows persisted | Distinct events | Late | Alerts | e2e p50 (ms) | e2e p95 (ms) |
|---|---|---|---|---|---|---|
| Flink 1.20 | 500 | 500 | 0 | 69 | **92** | **155** |
| Spark 3.5  | 500 | 500 | 0 | 69 | **3862** | **5695** |

Decision agreement, over the 500 events scored by both engines:

| Metric | Value |
|---|---|
| Events present in both engines | 500 |
| Same `action` | **500** |
| Different `action` | **0** |
| Max absolute score difference | **0.000000000** |
| Flink-only events (coverage gap) | 0 |
| Spark-only events (coverage gap) | 0 |

**Reading this honestly.** The two engines are in exact agreement: the same model, the same feature
spec (`fs-1.1`) and the same ruleset produce bit-identical scores, so the comparison isolates the
engine and nothing else. The latency difference is architectural, not a quality difference: Flink
scores per record as it arrives, while Spark's micro-batch trigger of 1000 ms means an event waits for
the next batch boundary before it is even picked up. A p50 of 3862 ms against Flink's 92 ms is roughly
what a 1 s trigger plus batch scheduling and scoring overhead predicts. Lowering the Spark trigger
would narrow the gap; that variant was not run.

**Not measured:** sustained throughput (events/second at saturation) for either engine, p99 latency,
CPU/memory cost per event, and behaviour under backpressure. The run was rate-limited to 10 eps by the
generator, so it measures latency at a low, fixed arrival rate — it is not a throughput benchmark.

## 2. Late-event handling — run `r_b17d3ca811cf1137`

Scenario `late_events`, 600 records at 5 events/second (~120 s), Flink only.

| Metric | Value |
|---|---|
| Persisted | 600 |
| Distinct events | 600 |
| Flagged `late` | **49** |
| Duplicates | 0 |
| Collapsed redeliveries | 0 |
| Delivery lag p50 | 1 ms |
| Delivery lag p95 | 20,951 ms |
| Delivery lag max | 29,866 ms |
| Delivered > 10 s after event time | **49** |
| e2e min / p50 / p95 / max | 25 / 100 / 149 / 165 ms |

The 49 events flagged `late` are a **1:1 match** with the 49 events delivered more than 10 s after
their event time, which is the expected result for `forBoundedOutOfOrderness(5 s)` plus a 60 s tumbling
window with `allowedLateness(ZERO)`: anything arriving after the watermark has passed its window is
routed to the late side output and marked, never silently dropped and never double-counted.

**An earlier 150-event run at 10 eps produced `late = 0`.** That was not a failure. Its maximum
delivery lag was 3,842 ms — inside the 5 s bounded-out-of-orderness — so nothing *was* late, and the
run finished in ~15 s, before the 15–30 s delayed deliveries came due. The run above is longer
specifically so the delayed path actually fires.

## 3. Flink fault injection and checkpoint recovery

Job `20d1ba3421afcb29654a0025021b8451`, EXACTLY_ONCE checkpointing every 10 s, retained checkpoints.

| Event | Time (UTC) |
|---|---|
| TaskManager killed | 18:55:12 |
| Job observed `RESTARTING` | ~18:56:12 (after the ~60 s heartbeat timeout) |
| TaskManager back up | 18:56:04 |
| Job observed `RUNNING` again | after restart |

- `counts.restored = 4`
- Restored from `chk-80`:
  `{"id": 80, "restore_timestamp": 1789671369179, "is_savepoint": false,
  "external_path": "file:/opt/flink/checkpoints/20d1ba3421afcb29654a0025021b8451/chk-80"}`
- 6 checkpoints failed during the outage window — expected while no TaskManager is available.
- No event loss and no duplicate persisted rows across the failure.

Note that the failure is only detected once the heartbeat times out (~60 s). Checking the job state
immediately after the kill shows `RUNNING` with `restored = 0`; that is the heartbeat interval, not a
successful survival.

## 4. Lifecycle wake

Wake `wake-39bed768b949`, cold start from `sleeping`:

| Stage | Epoch ms | Δ |
|---|---|---|
| `wake_requested` | 1789672095738 | — |
| `vm_running` | 1789672096619 | +0.9 s |
| `services_up` | 1789672107072 | +10.5 s |
| `ready` | 1789672120707 | +13.6 s |
| **Total** | | **~25 s** |

An earlier failed wake, `wake-362778af7f70`, is preserved in the history with `wake_failed` at stage
`starting_services` followed by `vm_stop_after_failure` — the failure path stops the VM rather than
leaving it running, and the history is not rewritten to hide it.

## 5. Site reachability

- `https://fraudwatch-live-997216389793.asia-south1.run.app` → HTTP 200
- `https://fraudwatch-lab-997216389793.asia-south1.run.app` → HTTP 200
- Both serve an identical `config.js` pointing at the same controller, so both sites share one backend.
- `/viva` → HTTP 301 to `/` on both sites.
- While the VM is not ready, the controller returns `503 {"error":{"code":"pipeline_starting_vm", …}}`
  rather than a fabricated empty result.

## What is still not measured

- Sustained throughput for either engine.
- p99 latency, and latency under backpressure or at saturation.
- Spark with a trigger interval other than 1000 ms.
- Any comparison run through the `services/ops-agent/` experiment runner (not implemented).
- Browser-rendered verification of both deployed sites (checked with `curl` only so far).
