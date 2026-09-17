# Status checklist

The 12-item completion checklist from `FraudWatch-Complete-Consolidated-Prompt.md`, scored
conservatively against what is in the repository and what responded to a request on **2026-09-18**.

Rules used for the rating:

- **Done** — the code exists and its claim is verifiable from a committed artifact or a command that
  was actually run.
- **Partial** — implemented but with a named gap.
- **Not started** — no implementation.

Where something is unverified, it says so rather than getting the benefit of the doubt. Files under
`apps/`, `packages/`, `tests/` and `infra/` were being edited by other agents while this was written,
so frontend items are rated from the code present at the time of reading and may have moved on.

| # | Checklist item | Status | Evidence / gap |
|---|---|---|---|
| 1 | FraudWatch Live landing page with interactive 3D hero and direct console access | **Done** (code) | `apps/live/src/pages/Landing.tsx` renders `LandingHero` + `SceneStage`; `packages/ui/src/landing/LiveScene3D.tsx` uses `@react-three/fiber` 9.7.0 / `three` 0.186.0; three `Link to="/console"` buttons. Not verified in a browser by me. |
| 2 | FraudWatch Lab landing page with a distinct Flink/Spark 3D hero | **Done** (code) | `apps/lab/src/landing.tsx` + `packages/ui/src/landing/LabScene3D.tsx` — a separate scene component from the Live one. Not verified in a browser by me. |
| 3 | Viva navigation/page removed and old links handled | **Done** | `apps/lab/src/App.tsx`: `<Route path="/viva" element={<Navigate to="/" replace />} />` — the route is gone and the old link redirects rather than 404s. |
| 4 | Real LightGBM scoring with verified preprocessing and honest evaluation | **Done** | Committed artifact `ml/artifacts/fw-ieee-stream-1.1.0/` (model + `metadata.json` + `evaluation.json`), copied into the scoring image by `services/scoring/Dockerfile`. Preprocessing parity verified twice: `prepare_manifest.json` offline check (405 uids, 34,801 events, max relative difference 3.67e-13) and `FeatureParityTest` run during the Flink image build. Evaluation is on a chronological held-out split with PR-AUC reported over accuracy and a logistic-regression baseline — see [`model-card.md`](model-card.md). |
| 5 | Bounded preset scenarios, actual detections, alerts, and investigation evidence | **Done** | `GET /api/v1/scenarios` presets with `bounds` and `caveats`; run creation clamped by `DEMO_MAX_EVENTS_PER_SESSION` / `DEMO_MAX_EVENTS_PER_SECOND`; Live routes `/scenario`, `/alerts`, `/tx/:eventId`; per-event TreeSHAP from the scoring service plus `rules-1.1` evidence; `GET /system/trace/{event_id}` per-stage timeline. |
| 6 | Executable CA2 workspace and working ops-agent / experiment launcher | **Not started** (runner) | `services/ops-agent/app/` is an **empty directory**. The Lab `/ca2` and `/experiments` pages and the `POST /experiments` contract exist, but nothing can execute an experiment. `OPS_AGENT_URL=http://ops-agent:8003` in `.env.example` points at a service with no code. |
| 7 | Fair real Flink/Spark measurements, late-event behaviour, and recovery evidence | **Not started** (measurements); **Partial** (surfaces) | Consequence of #6: **no latency, throughput or comparison figure has been measured and none exists in this repository.** The read surfaces are implemented — `GET /runs/{id}/comparison` (incl. `raw_feature_mismatch`, `action_disagree`, `late_disagree`) and `GET /runs/{id}/late` — and both engines implement late handling (Flink side-output + `allowedLateness(0)`, Spark `GroupState` watermark check). Recovery evidence: Flink retains checkpoints (`RETAIN_ON_CANCELLATION`) and `submit.sh` restores from the newest complete one, but no recovery experiment has been run. See [`benchmark-methodology.md`](benchmark-methodology.md). |
| 8 | Shared System Intelligence with structured logs, traces, health, and data quality | **Done** (implemented) | `services/api/app/routes/system.py` plus `health.py`: `/system/operations`, `/system/trace/{event_id}`, `/system/data-quality`, `/api/v1/health` with per-component status. Honest by construction: `null` means "not recorded" and is never replaced by `0`, `RunCounters…processed` is always `null` and listed in `not_instrumented`, Flink watermarks/backpressure can be the literal `"not_instrumented"`, and DLQ-derived counts are operator-only. Not exercised against a live cloud deployment by me. |
| 9 | Both websites synchronized through durable cloud state | **Partial** | The mechanism exists — durable state in PostgreSQL behind one shared API, a Lab "Shared Live context" banner carrying `session` and `run` from Live, and the controller's Firestore state document (`CONTROL_STATE_COLLECTION=fraudwatch-runtime`) read by both sites' lifecycle component (`packages/ui/src/lifecycle/lifecycle.tsx`). **Not yet verified in the cloud**: both sites are now deployed (revisions `fraudwatch-live-00002-grj` and `fraudwatch-lab-00002-z5j`, 100% of traffic, 2026-09-18), but neither has been opened in a browser, so cross-site synchronisation has not been observed end to end (see #11). |
| 10 | Automatic cloud wake, safe idle shutdown, and successful subsequent restart | **Partial** | The controller is deployed and real: `GET /health` → HTTP 200, `GET /lifecycle/status` → HTTP 200 with a live state document reporting `auto_wake_enabled: true`, `idle_timeout_minutes: 45.0`, `startup_timeout_seconds: 600`, and a `vm_status_observed` read directly from Compute Engine. Wake is triggered per page load by `POST /lifecycle/ensure-ready` (`packages/contracts/src/client.ts`), idle stop by a Cloud Tasks timer, with per-day wake and uptime budgets enforced in `services/controller/app.py`. **But the most recent wake failed:** `state: "failed"`, `stage: "starting_services"`, `error: "startup exceeded 600 s at stage 'waiting for the API container'"`, `vm_status_observed: null`. So automatic wake is implemented and reachable; **a successful wake → ready → idle-stop → restart cycle is not verified.** |
| 11 | No laptop dependency after deployment and no paid-billing upgrade | **Partial** | Architecturally satisfied: Cloud Run with min-instances 0, a Compute Engine VM stopped when idle, Firestore state, Cloud Tasks timers — nothing requires a local machine. Billing posture is Free Trial credit only, with the controller's per-day wake/uptime caps as the guard, and every cost number in [`gcp-deployment.md`](gcp-deployment.md) is an **estimate of credit consumption, not money charged**. **Update (2026-09-18):** both sites are now deployed — Cloud Run revisions `fraudwatch-live-00002-grj` and `fraudwatch-lab-00002-z5j`, each serving 100% of traffic as a static nginx bundle with `min-instances 0`, `max-instances 2`, `cpu 1`, `256Mi`, `concurrency 200`. An earlier check the same day returned HTTP 404 and predates these revisions. **Remaining gap:** neither page has been opened and exercised in a browser, so rendering, routing and the SSE stream in the cloud are still unverified. |
| 12 | Browser/integration checks, working typecheck/build, complete setup guides, and evidence-based report | **Partial** | Present: Playwright e2e specs (`tests/e2e/specs/` — `live.landing`, `live.console`, `live.responsive`, `lab.landing`, `lab.pages`) with `npm run test:e2e`; vitest units (`packages/ui/src/format.test.ts`, `packages/ui/src/viz/math.test.ts`); backend integration tests (`tests/integration/smoke_scoring.py`, `tests/streaming/parity_integration.py`, `tests/streaming/run_integration.sh`); a build-time Java/Python parity test. Setup guides now complete: [`local-setup.md`](local-setup.md), [`apache-stack.md`](apache-stack.md), [`gcp-deployment.md`](gcp-deployment.md), [`contracts.md`](contracts.md), [`model-card.md`](model-card.md), [`benchmark-methodology.md`](benchmark-methodology.md). **Verified 2026-09-18:** `npm run typecheck` and `npm run build` both passed locally (one machine, one run; **not** a CI result — there is no `.github/workflows/`). **Still unverified:** `npm run test` and `npm run test:e2e` were not run. **New evidence:** a measured Flink fault-tolerance run (TaskManager kill → restore from `chk-80`, 15 rows / 15 distinct `event_id` / 0 duplicates) is recorded in the README's "Measured evidence" section and drawn in [`figures.md`](figures.md); it is a fault-tolerance demonstration, not a benchmark. **Missing:** the evidence-based report draft; no report document exists in `docs/`, and the benchmark evidence it would need does not exist either (#7). |

## Summary

| Status | Items |
|---|---|
| Done | 1, 2, 3, 4, 5, 8 |
| Partial | 9, 10, 11, 12 |
| Not started | 6, 7 |

## The three gaps that matter

1. **`services/ops-agent/app/` is empty.** It blocks item 6 directly and item 7 entirely. Until it is
   implemented, every benchmark figure in this project stays `_not measured yet_`, and nothing should
   be published as a Flink-versus-Spark result.
2. **Both sites are deployed but not browser-verified.** Revisions `fraudwatch-live-00002-grj` and
   `fraudwatch-lab-00002-z5j` serve 100% of traffic as of 2026-09-18 (the earlier 404 check predates
   them). Items 9, 10 and 11 still cannot be fully closed, because no one has opened either page in a
   browser and walked it through, and a wake attempt earlier that day had failed at `starting_services`
   after the 600 s startup timeout.
3. **No report draft exists**, and the evidence it should cite (item 7) has not been produced.

## Other honest notes found while documenting

- **`ExportEvent.amount_inr`.** `contracts.md` section 7.2 names a field `amount_inr`, which
  contradicts the repository-wide rule that amounts are `TransactionAmt` in an **unspecified source
  unit with no currency and no conversion** (stated in `metadata.json`, `prepare_manifest.json`,
  `evaluation.json` and `packages/fwcore/fwcore/rules.py`). The field name should be treated as a
  legacy misnomer, not as evidence of a currency; renaming it is a contract change and is outside this
  documentation pass.
- **`AUTO_DEMO_ENABLED` / `AUTO_DEMO_MAX_EVENTS`** are documented in `gcp-deployment.md` and listed in
  `.env.example`, but **no code in this repository reads them.** They are annotated as such in
  `.env.example`.
- **The replay pack is gitignored**, so `services/generator/Dockerfile`'s `COPY` of
  `ml/artifacts/replay/ieee-test-replay-1.0/{records,selections,manifest}.json` fails on a fresh clone
  until the pack is built from the raw Kaggle CSVs. Documented in [`local-setup.md`](local-setup.md)
  section 3. (`labels.json` is deliberately not copied into the image.)
- **Two source files reference a `docs/apache-streaming.md` that does not exist**:
  `infra/compose/compose.streaming.yaml` (header comment, line 9) and
  `packages/fwcore/fwcore/replay.py` (module docstring). No document in `docs/` links to that
  filename — the streaming documentation is [`apache-stack.md`](apache-stack.md). Fixing those two
  comments is outside this documentation pass.
- **Two bugs found and fixed during the 2026-09-18 Flink run:** the replay import pointed at
  `fwcore.simulation` instead of `fwcore.replay` for `PATTERNS`, and a request used the invalid
  scenario `'normal'` (valid scenarios: `replay`, `selected_examples`, `late_events`, `duplicates`,
  `account_burst`). Both fixes are in `packages/` / `services/` code owned by other agents; recorded
  here as observation only.
- **No `.github/workflows/` and no `LICENSE` file** exist, so the README carries no CI badge and no
  licence badge.
- **`FLINK_PARALLELISM > 2` can never be scheduled** with the committed Compose setup — one TaskManager
  with `taskmanager.numberOfTaskSlots: 2` — and `submit.sh` will time out waiting for slots.
- **`R3_TEST_THEN_LARGE` fires 0 times** on the held-out test split (`evaluation.json`). Reported as
  zero rather than retuned.
