# Demo script (5–7 minutes)

A walkthrough that only shows things this repository actually implements. Every route, endpoint and
number named below was verified against the code; where something is not measured, the script says so
out loud instead of filling the gap.

## Before you start — current deployment reality

Checked on 2026-09-18 from this machine:

| Target | Result |
|---|---|
| `GET https://fraudwatch-control-997216389793.asia-south1.run.app/health` | HTTP 200, `{"status":"ok"}` |
| `GET …/lifecycle/status` | HTTP 200, real state document; at the time of checking `state: "failed"`, `stage: "starting_services"`, `error: "startup exceeded 600 s at stage 'waiting for the API container'"`, `vm_status_observed: null` |
| `https://fraudwatch-live-997216389793.asia-south1.run.app/` | **Deployed** — Cloud Run revision `fraudwatch-live-00002-grj`, 100% of traffic. Returned HTTP 404 on an earlier check the same day, before this revision. **Not yet opened in a browser.** |
| `https://fraudwatch-lab-997216389793.asia-south1.run.app/` | **Deployed** — revision `fraudwatch-lab-00002-z5j`, 100% of traffic. **Not yet opened in a browser.** |

**The controller is deployed and real, and both sites now have a revision serving 100% of traffic —
but neither has been opened in a browser yet, and a wake attempt that day had failed at
`starting_services`.** So: open both site URLs and check `GET /lifecycle/status` immediately before
presenting, and be ready to fall back to the local stack ([`local-setup.md`](local-setup.md)) — the demo
is identical locally apart from the wake step. Never present the cloud URLs as live without re-checking.

Rehearse the whole thing once end to end. A first cold wake can take up to
`PIPELINE_STARTUP_TIMEOUT_SECONDS` (600 s), which is longer than this entire demo.

## 0:00 — 0:45 · What it is, and the honest framing

> FraudWatch scores card transactions on a stream. The same transactions go through Apache Flink and
> Apache Spark, both calling the same model, so you can see two engines agree — or disagree — on
> identical input.
>
> Three things up front. The data is the IEEE-CIS Fraud Detection dataset from Kaggle — real
> anonymised records from Vesta, not synthetic. There is no account id in it, so an "account" here is a
> pseudonymous key built from `card1 + addr1 + P_emaildomain`. And the amounts have no currency: the
> dataset never says what unit `TransactionAmt` is in, so we show bare numbers and convert nothing.

That framing is in the product itself: the Live site's footer says "Educational project, not a banking
integration… no real payments are processed."

## 0:45 — 1:30 · Wake-on-visit (cloud only)

Open the Live site's landing page (`/`) and let it trigger the wake.

> Nothing is running until someone visits. The sites are Cloud Run services with min-instances 0; the
> streaming stack is a Compute Engine VM that is **stopped**. The landing page calls the controller's
> `POST /lifecycle/ensure-ready`, which starts the VM. After 45 minutes without activity a Cloud Tasks
> timer stops it again. That is `PIPELINE_IDLE_TIMEOUT_MINUTES=45`.

Show the controller's own state, live:

```bash
curl -s https://fraudwatch-control-997216389793.asia-south1.run.app/lifecycle/status | jq
```

Point at the real fields: `state`, `stage`, `auto_wake_enabled`, `idle_timeout_minutes`,
`idle_deadline_at`, `startup_timeout_seconds`, and `vm_status_observed` — which is read from Compute
Engine directly, independently of what the controller believes.

> This runs on Free Trial credit, so the controller enforces a wake budget: a maximum number of wakes
> per day and a maximum uptime per day, and it refuses to wake beyond them. Details are in
> `docs/gcp-deployment.md`. The cost figures there are **estimates of credit consumption**, not money
> charged.

If the state is `failed`, say so and move to the local stack. A failed wake shown honestly is a better
demo than a pretended success.

## 1:30 — 3:00 · Start a scenario and watch it flow

Go to `/scenario` on the Live site and pick a preset.

> Presets come from `GET /api/v1/scenarios`. Each one describes the **input conditions** the generator
> will create — a burst, small test transactions then a large one, a spending deviation, deliberately
> delayed arrivals, replayed duplicates. Note what a preset does *not* do: it never promises what the
> model will output. The contract calls that out explicitly.

Start the run, then go to `/console`.

> Events stream in over SSE. Every row is a real IEEE-CIS record: amount, pseudo-account, product code,
> card network, device type — and the engine's decision: allow, review, or alert.

Then `/pipeline`.

> This is the path each event took: generator → Kafka topic `fraudwatch.transactions` →
> Flink → the scoring service → the results topic → the result consumer → PostgreSQL → this API → this
> page. Four partitions on the input topic, and the broker stamps every record with LogAppendTime, so
> arrival order is recorded independently of event time.

Mention the caps rather than being asked about them: public runs are clamped by
`DEMO_MAX_EVENTS_PER_SESSION` and `DEMO_MAX_EVENTS_PER_SECOND`, and a non-operator run targets Flink
only.

## 3:00 — 4:15 · One alert, opened all the way up

Go to `/alerts`, click into an alert — `/tx/:eventId`.

> Here is the decision for one transaction. Three separate things, deliberately not mixed together:
>
> **The score.** A LightGBM model, `fw-ieee-stream-1.1.0`, trained on 15 features that can all be
> computed from the stream — log amount, prior counts over 60 seconds, 10 minutes and one hour, time
> since the last transaction, the amount relative to the account's running mean, plus product code,
> card network, card type, email domain, device type. The artifact's own metadata says the score is "a
> model score in `[0,1]`; not a calibrated probability" — so treat it as a ranking, not as "an 11%
> chance of fraud".
>
> **The explanation.** TreeSHAP contributions in log-odds, computed live by the scoring service. It
> explains the model, not the world.
>
> **The rules.** Separate, explicit, and independent of the label: a burst rule, a spend-profile
> deviation rule, and a small-tests-then-large rule. These are evidence, not model attributions.

If the audience is technical, show `GET /api/v1/system/trace/{event_id}`:

> Per-stage timestamps for that one event: generated → kafka_acked → engine_received → features_ready →
> scored → engine_emitted → persisted → alert_evaluated. Where a timestamp does not exist it is `null`,
> and in this API `null` always means "not recorded", never zero.

## 4:15 — 5:30 · The Lab site: the parts that are honest about being incomplete

Open the Lab site: `/study`, then `/ca1` and `/ca2`, `/architecture`, `/evidence`.

> The Lab site is the academic companion. Its footer states the rule it follows: measured evidence is
> labelled separately from explanation. `GET /evidence` tags every item as `measured`, `prerecorded` or
> `illustrative`.

On `/experiments`, be direct:

> This is where the Flink-versus-Spark comparison belongs. The experiment runner —
> `services/ops-agent/app/` — claims a queued experiment, drives the generator and measures the result
> out of PostgreSQL, so anything shown here is a real run with a run id behind it. Throughput at
> saturation is still **`_not measured yet_`**: every run so far was rate-limited, so what you see is
> latency at a fixed offered rate, and I am not going to quote a throughput number.
> `docs/benchmark-methodology.md` sets out what a fair comparison requires: the same Kafka topic,
> the same producer-assigned event ids, the same scoring service and artifact, the same 5-second
> watermark and 60-second window, a discarded warm-up run, repeats, and the offered rate reported
> separately from the processed rate.

If Spark is running (locally: the `spark` Compose profile), show
`GET /api/v1/runs/{id}/comparison` and read out the fields that matter:

> `both`, `only_flink`, `only_spark`, `action_agree`, `action_disagree`, `raw_feature_mismatch`,
> `late_disagree`. The one to watch is `raw_feature_mismatch`: zero means the Java implementation in
> Flink and the Python `fwcore` in Spark computed identical features for every event. The Flink image
> build enforces that too — it runs a parity test and fails the build if they diverge.

## 5:30 — 6:30 · Model quality, stated properly

Pull up [`model-card.md`](model-card.md), or `GET /api/v1/model`.

> On the held-out test split — the last 15% chronologically, 88,581 transactions, 3.48% fraud — the
> model gets ROC-AUC 0.782 and PR-AUC 0.138. At the alert threshold: precision 0.151, recall 0.367,
> flagging 8.5% of traffic. A logistic-regression baseline trained the same way gets PR-AUC 0.096.
>
> Now the part that matters. Accuracy at that threshold is 0.906, and the baseline's is *higher*, 0.921
> — because predicting "never fraud" scores about 96.5%. So accuracy is meaningless here; PR-AUC and
> the precision/recall pair are the honest numbers.
>
> Alert plus review together catch about 48% of the fraud. Roughly half is missed. On accounts never
> seen in training, recall at the same threshold drops to 0.21. And validation PR-AUC was 0.183 versus
> 0.138 on test — that gap is temporal drift, and it is visible only because the split is chronological
> rather than shuffled.
>
> One more: the third rule, small-test-transactions-then-a-large-one, fires zero times on the held-out
> split. That pattern is not in this data at those thresholds, and we report it as zero rather than
> retuning until it looks good.

## 6:30 — 7:00 · Close

> Kafka 4.1.2 in KRaft mode, Flink 1.20.5 with exactly-once checkpoints every 10 seconds, Spark 3.5.9
> Structured Streaming, one LightGBM artifact behind both engines, PostgreSQL 17, a FastAPI layer, two
> React 19 sites, and a wake-on-visit controller so the cloud deployment costs nothing while idle.
>
> What is measured: a Flink TaskManager kill, recovery from checkpoint `chk-80`, and 15 rows with 15
> distinct event ids and zero duplicates afterwards.
>
> What is not done: the experiment runner is unimplemented, so no throughput, no latency percentiles
> and no Flink-versus-Spark comparison have been measured; both sites are deployed but neither has been
> verified in a browser; and the model misses about half the fraud at its operating point.
> `docs/status-checklist.md` lists all of it.

## Questions you will be asked

**"What's the accuracy?"** — About 90.6% at the alert threshold, and that number is useless: always
answering "not fraud" scores about 96.5% and catches nothing. The real answer is PR-AUC 0.138 versus
0.096 for the baseline, with recall 0.367 at precision 0.151.

**"Is Flink faster than Spark?"** — Unknown. Not measured, and this repository has no runner to measure
it. Even if it were measured on a single-node Compose stack with Spark in `local[2]`, it would say
nothing about either engine in general.

**"Is the score a probability?"** — No. The artifact's metadata says so explicitly. Calibration was
measured (Brier 0.032, ECE 0.0055), but low ECE at a 3.5% base rate is cheap.

**"Rupees or dollars?"** — Neither. The dataset does not state a unit for `TransactionAmt`, so nothing
is converted and no symbol is shown. The rule thresholds are in those same raw units.

**"Is this real data?"** — Yes: IEEE-CIS Fraud Detection, Vesta's anonymised records via Kaggle. The
raw CSVs are not redistributed here; the replay pack is built locally and is gitignored, and labels are
never sent into Kafka or to the scoring service.

**"Exactly-once?"** — Flink's *checkpointing* is exactly-once, which is about state consistency. Both
Kafka result sinks are at-least-once with idempotent producers, so duplicates are possible; they are
collapsed downstream by an upsert on `(engine, event_id)`, and Spark additionally applies
`dropDuplicatesWithinWatermark`. Calling the whole pipeline end-to-end exactly-once would be wrong.

**"What does it cost?"** — Nothing out of pocket. It runs on Free Trial credit with min-instances 0 and
a VM that stops after 45 idle minutes, and the controller caps wakes and uptime per day. The figures in
`docs/gcp-deployment.md` are estimates of *credit consumption*, not of money charged.
