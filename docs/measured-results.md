# Measured results (GCP, 2026-09-18)

Every number on this page came from a run on the deployed GCP stack
(VM `fraudwatch-pipeline`, `asia-south1-a`) and was read back out of Postgres or the Flink REST API.
Nothing here is estimated, extrapolated or illustrative. Where something was **not** measured, this
page says so instead of guessing.

How to reproduce each run is in [`benchmark-methodology.md`](benchmark-methodology.md). The runs below
were orchestrated **manually** (a row inserted into `runs`, then `POST /runs/{id}/start` on the
generator with the internal token), not through the `services/ops-agent/` experiment runner. That does
not change what was measured — both engines consumed the same Kafka records — but it does mean these
particular runs were not produced by the runner. The runner now exists (`services/ops-agent/`) and has
executed a `late_events` experiment end to end locally; it has not yet re-run the comparison above.

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

## 2b. Late-event handling through the experiment runner — `exp_20260918_070300_29d804`

The first run measured through `services/ops-agent/` rather than by hand. Queued with
`POST /api/v1/experiments` (operator token) as `{"kind": "late_events", "params": {"events": 600,
"rate_eps": 5, "seed": 20260917}}`; the agent claimed it, created run `r_352925ea14c95961`, drove the
generator and read the result out of PostgreSQL. **Local Docker Compose stack, not GCP.**

| Metric | Value |
|---|---|
| Drain | `complete` |
| Persisted (Flink) | 600 of 600 |
| Planned late (15-30 s delay, beyond the bound) | **49** |
| Of those, flagged `late` by Flink | **49** |
| Planned late but **not** flagged | 0 |
| Flagged late without being planned | 0 |
| Delayed 1-4 s (inside the bound) | 59 |
| Of those, flagged `late` | **0** |

The second half of that table is the part that carries the argument. The generator injects two delay
classes on purpose: 15-30 s, which is beyond the source's `forBoundedOutOfOrderness(Duration.ofSeconds(5))`,
and 1-4 s, which is inside it. Every record in the first class was flagged; no record in the second
class was. Lateness therefore tracks the watermark bound, not merely "arrived out of order" — a
distinction a raw `late` count alone cannot demonstrate.

The experiment refuses to run when `events / rate_eps` is under 60 s: a shorter run finishes its event
budget before the 15-30 s delayed records come due, delivers none of them, and would report `late = 0`
as though the engine had seen nothing late. That is the failure the 150-event run in section 2 hit, and
it is now a rejected request rather than a misleading zero.

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

## 5. Idle shutdown — operation `stop-0cdb13d13b32`

Measured at the **production** settings (`PIPELINE_IDLE_TIMEOUT_MINUTES=45`,
`PIPELINE_MIN_UPTIME_MINUTES=10`), not at a shortened test timeout. Every line below is an event from
`GET /lifecycle/history`, in UTC.

| Time (UTC) | Event | Detail |
|---|---|---|
| 2026-09-17 19:08:40 | `ready` | wake `wake-39bed768b949`; last activity 19:08:15 |
| 2026-09-17 19:53:43 | `idle_shutdown_started` | `idle_minutes: 45.0`, `last_activity_at` 19:08:15 |
| 2026-09-17 19:53:44 | `drain_requested` | `{"failed": ["r_5cf6b929471a5adc"], "stopped": []}` |
| 2026-09-17 19:53:54 | `vm_stop_requested` | `undrained_runs: 0`, GCE op `operation-1789674834658-…` |
| 2026-09-17 19:54:26 | `sleeping` | `stop_duration_ms: 42153` |

Idle interval actually observed: 19:08:15 → 19:53:43 = **45 min 28 s** after the last meaningful
activity, against a 45-minute policy. VM stop took **42.2 s** from request to `sleeping`.

One honest detail: the drain call reported `failed: ["r_5cf6b929471a5adc"]` and stopped nothing. The
subsequent `/internal/active-work` check nonetheless reported `undrained_runs: 0`, so the shutdown
proceeded. The events are consistent with a run that was already finished when drain was called, but
the history does not prove that, and the drain failure itself is not explained by the recorded events.

Together with §4, the full loop `sleeping → starting_vm → starting_services → restoring → ready →
stopping → sleeping` has now been observed end to end in production, including one failed wake that
stopped the VM instead of leaving it running.

## 6. Site reachability

- `https://fraudwatch-live-997216389793.asia-south1.run.app` → HTTP 200
- `https://fraudwatch-lab-997216389793.asia-south1.run.app` → HTTP 200
- Both serve an identical `config.js` pointing at the same controller, so both sites share one backend.
- `/viva` → HTTP 301 to `/` on both sites.
- Every SPA route returns 200: Live `/console`, `/alerts`, `/scenario`, `/pipeline`; Lab `/study`,
  `/ca1`, `/ca2`, `/experiments`, `/architecture`, `/evidence`.
- Every built asset is served, including the lazy-loaded 3D chunks (`sceneKit`, `LiveScene3D`,
  `LabScene3D`, `Pipeline3D`, and the 938 KB `three`/`OrbitControls` chunk).
- Checked against revisions `fraudwatch-live-00003-wc4` and `fraudwatch-lab-00003-4gs`.
- While the VM is not ready, the controller returns `503 {"error":{"code":"pipeline_starting_vm", …}}`
  rather than a fabricated empty result.

## 7. Browser-rendered verification of both deployed sites (2026-09-18)

Both sites were loaded at their Cloud Run origins in a real Chromium (Playwright 1.63.0, headless,
1440x900), not with `curl`. The Live pass drives the app as a visitor does: it starts a real demo
session, starts a real bounded scenario run, waits for the session's events-used counter to move, and
only then photographs the console. Screenshots are in `reports/media/screenshots/`, walkthrough
recordings in `reports/media/video/`, and the raw logs in `capture-log.json` / `tour-log.json`.

| Check | Result |
|---|---|
| Routes loaded in-browser | 12 of 12 returned HTTP 200 |
| Live routes | `/`, `/console`, `/scenario`, `/alerts`, `/pipeline` |
| Lab routes | `/`, `/ca1`, `/ca2`, `/experiments`, `/architecture`, `/system`, `/evidence` |
| WebGL | a `canvas` element initialised on every 3D hero; the 2D fallback was not needed |
| Console **errors** | **0** (8 remaining messages are THREE.js deprecation and WebGL performance warnings) |
| Live session | events-used moved `0 of 5,000` → `80 of 5,000` before capture |

### 7.1 A CORS defect found here, and fixed

The first capture attempt produced **192 console errors**, all one failure: every API call from both
deployed sites was blocked by `No 'Access-Control-Allow-Origin' header is present`. `ALLOWED_ORIGINS`
on `fraudwatch-control` listed only the project-number host form
(`fraudwatch-live-997216389793.asia-south1.run.app`), while a browser loads the sites from the hash
form (`fraudwatch-live-2djxmgtj5a-el.a.run.app`). Both host forms serve the sites and the origin check
is an exact string match, so the hash form was rejected.

This is exactly the class of defect the `curl` checks in §6 could not see: every route returned 200
there because the HTML and assets are served correctly. It is only the browser's cross-origin check on
the *subsequent* API calls that failed.

**Fixed 2026-09-18.** `ALLOWED_ORIGINS` now carries all four host forms; deployed as Cloud Run revision
`fraudwatch-control-00004-dgw`. Verified directly — a request carrying
`Origin: https://fraudwatch-live-2djxmgtj5a-el.a.run.app` now comes back with a matching
`access-control-allow-origin`, and the same for the Lab origin. The recapture after the fix recorded
**0 console errors**, down from 192.

### 7.2 What the browser then showed (wake `wake-6de56048a5f8`)

With the pipeline woken from `sleeping` and a bounded `ieee_replay` run started from `/scenario`, the
Live console rendered real data rather than an empty shell. Read off the captured console at
07:54 UTC (13:24 IST), session `s_A6CL0nG8Pu`, run `r_e3ec03db2a`:

| Panel | Value shown |
|---|---|
| Scored | 353 (291 allowed, 0 unscored) |
| Alerts | 48, with 0 late events |
| Reviews | 14 queued |
| Throughput (last 1 min) | 5.9 ev/s |
| End-to-end latency | p50 **110 ms**, p95 **163 ms**, p99 **188 ms** (353 events, last 300 s) |

The alert queue shows per-alert scores, engine `Flink`, and `R1_BURST` firing on many rows. The
1-minute event-time window panel correctly read "No closed windows yet — a window is emitted once the
watermark passes its end", which is the honest state that early in a run rather than a fabricated zero.

Two caveats on these numbers. They are a **live UI reading at one instant**, not a controlled
benchmark: the run was still in flight, so the counts are a snapshot, not a total. And the latency
percentiles are the deployed pipeline at the scenario's own rate — they agree closely with the Flink
figures in §1 (p50 92 ms, p95 155 ms) but were not produced under the §1 protocol.

### 7.3 Cross-site synchronisation (Live → Lab)

`scripts/verify_cross_site.mjs`, 2026-09-18 09:47 UTC, deployed origins, real Chromium. The script starts
a demo session and a bounded run on Live, clicks **Open in Lab** (which mints a read-only share link via
`POST /sessions/{id}/share`), and follows the link.

| Check | Result |
|---|---|
| Run carried in Live's share link | `r_febd244cfefcf63a`, engine `flink`, view token present |
| Lab landing-page chip | "Live · run r_febd244cfefcf63a" |
| Lab `/study` | "Shared Live context" banner and "Run selected from Live" naming `r_febd244cfefcf63a` |
| Console errors (both sites) | 0 |

Lab was opened in a separate tab and read the run from the link and the shared backend, not from Live's
browser state. Log and screenshots: `reports/media/screenshots/cross-site-*`.

## What is still not measured

- Sustained throughput for either engine.
- p99 latency, and latency under backpressure or at saturation.
- Spark with a trigger interval other than 1000 ms.
- Any comparison run through the `services/ops-agent/` experiment runner. The runner is implemented
  and has completed a `late_events` experiment locally, but the comparison in §1 predates it and has
  not been reproduced through it.
- A controlled benchmark of what §7.2 shows. The console was observed carrying real data in a browser,
  but that is a live UI snapshot mid-run, not a measured run under the §1 protocol.
- Why the drain call in §5 reported a failure while `active-work` reported nothing in flight.
