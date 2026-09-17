# Local setup

How to run the whole FraudWatch stack on one machine. Everything here was read from
`infra/compose/compose.yaml`, `infra/compose/compose.streaming.yaml`, the service `Dockerfile`s,
`package.json`, `.npmrc` and `ml/`. For the cloud deployment see
[`gcp-deployment.md`](gcp-deployment.md); for streaming internals see
[`apache-stack.md`](apache-stack.md).

## 1. Prerequisites

| Tool | Version | Why |
|---|---|---|
| Docker Desktop with Compose V2 | current | every backend service, Kafka, Flink, Spark, PostgreSQL |
| Node.js | **≥ 22** (`package.json` → `engines.node`) | Vite 8, TypeScript 7, the two websites |
| npm | ships with Node | the root scripts |
| Python | **3.12** (`ml/pyproject.toml` pins `==3.12.*`) | training, replay-pack build, integration tests |
| `uv` | current | the documented way to run the `ml/` scripts |
| Java 17 + Maven 3.9 | only outside Docker | the Flink image builds the jar itself |

Disk: the IEEE-CIS raw CSVs are about **710 MB** together (683,351,067 + 26,529,680 bytes per
`ml/data/processed/prepare_manifest.json`), plus Docker images and the Kafka/Postgres volumes.

Memory: the Compose stack sets explicit `mem_limit`s that add up to roughly **11 GB** when Flink and
Spark both run (Kafka 1.5 GB, Postgres 768 MB, scoring 1 GB, generator 512 MB, result-consumer
512 MB, api 512 MB, Flink JobManager 1.2 GB, TaskManager 2.3 GB, submitter 768 MB, Spark 2.5 GB).
Without the `spark` profile it is roughly **8.5 GB**. Give Docker Desktop headroom accordingly.

## 2. Create `.env`

```bash
cp .env.example .env
```

Then replace every `CHANGE_ME` value. Compose uses the `${VAR:?message}` form for five variables and
**refuses to start** if any of them is empty:

- `POSTGRES_PASSWORD`
- `INTERNAL_API_TOKEN`
- `SESSION_SIGNING_SECRET`
- `OPERATOR_AUTH_SECRET`
- `CLIENT_HASH_SALT`

Generate each one separately:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

`.env` is gitignored (`.gitignore` ignores `.env` and `.env.*` except `.env.example`). Never print
its contents into a terminal you are sharing or into an issue.

Optional: `OPERATOR_PASSPHRASE_HASH` enables operator passphrase login. Produce it with
`python scripts/hash_passphrase.py` and paste **only the hash**. Leaving it empty disables that login
path.

## 3. Get the dataset and build the replay pack

The `generator` image `COPY`s the replay pack into the image, so **the image build fails if the pack
does not exist**:

```
COPY ml/artifacts/replay/ieee-test-replay-1.0/records.json \
     ml/artifacts/replay/ieee-test-replay-1.0/selections.json \
     ml/artifacts/replay/ieee-test-replay-1.0/manifest.json /models/replay/ieee-test-replay-1.0/
```

The pack is gitignored (row-level Kaggle-derived data), so a fresh clone must build it:

1. Accept the rules of the Kaggle competition
   [`ieee-fraud-detection`](https://www.kaggle.com/competitions/ieee-fraud-detection) and download
   `train_transaction.csv` and `train_identity.csv`.
2. Put them in `ml/data/raw/ieee-cis/`.
3. Build the features and then the pack:

```bash
cd ml
uv run python training/ieee_data.py          # -> data/processed/ieee_stream_features.parquet + prepare_manifest.json
uv run python training/build_replay_pack.py  # -> artifacts/replay/ieee-test-replay-1.0/
```

`ieee_data.py` took **83.7 s** on the machine recorded in `prepare_manifest.json`; your time will
differ.

The model artifact `ml/artifacts/fw-ieee-stream-1.1.0/` (model, metadata, evaluation) **is** committed,
so you do not have to retrain to run the stack. To retrain anyway:

```bash
cd ml && uv run python training/train_stream_model.py
```

`labels.json` from the replay pack is deliberately **not** copied into the generator image: labels
never enter Kafka or a scoring request.

## 4. Start the stack

Always run Compose from the repository root and always pass `--env-file` — without it Compose looks
for a `.env` next to the compose file and will not find yours:

```bash
docker compose --env-file .env -p fraudwatch \
  -f infra/compose/compose.yaml \
  -f infra/compose/compose.streaming.yaml up -d
```

That starts: `kafka`, `kafka-init` (one-shot topic creation), `postgres`, `scoring`, `generator`,
`result-consumer`, `api`, `flink-jobmanager`, `flink-taskmanager`, `flink-submitter`.

Spark is behind the `spark` profile and is **not** started above — that is intentional, so a normal
local run does not pay for a 2.5 GB JVM:

```bash
docker compose --env-file .env -p fraudwatch \
  -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml \
  --profile spark up -d spark
```

The first run builds seven images from the repository root as build context. The Flink image also
compiles the Java job and runs `FeatureParityTest`, so expect the longest wait there.

### Stop and clean up

```bash
# stop, keep data
docker compose --env-file .env -p fraudwatch \
  -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml stop

# stop and remove containers, keep volumes
docker compose --env-file .env -p fraudwatch \
  -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml down

# remove volumes too: Kafka log, Postgres data, Flink checkpoints/savepoints, Spark checkpoints
docker compose --env-file .env -p fraudwatch \
  -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml down -v
```

`down -v` destroys the Flink checkpoints, so the next start has no state to restore from.

## 5. Ports

| Service | Host address | Container port | Notes |
|---|---|---|---|
| api | `${API_BIND:-127.0.0.1}:${API_HOST_PORT:-8000}` | 8000 | `/api/v1` REST + SSE |
| Kafka (`HOST` listener) | `127.0.0.1:29092` | 29092 | advertised as `localhost:29092`; local tests only |
| PostgreSQL | `127.0.0.1:${POSTGRES_HOST_PORT:-15432}` | 5432 | local tests only |
| Flink Web UI / REST | `127.0.0.1:${FLINK_UI_HOST_PORT:-8081}` | 8081 | `web.submit.enable: false`, `web.cancel.enable: true` |
| Spark UI | `127.0.0.1:${SPARK_UI_HOST_PORT:-4040}` | 4040 | only with the `spark` profile |
| scoring | not published | 8001 | in-network only (`http://scoring:8001`) |
| generator | not published | 8002 | in-network only (`http://generator:8002`) |
| result-consumer | not published | — | no HTTP server |
| FraudWatch Live (dev) | `localhost:5173` | — | `strictPort: true` |
| FraudWatch Lab (dev) | `localhost:5174` | — | `strictPort: true` |
| FraudWatch Live (preview) | `localhost:4173` | — | production build |
| FraudWatch Lab (preview) | `localhost:4174` | — | production build |

Everything except the dev servers is bound to `127.0.0.1` by default. Nothing is exposed on `0.0.0.0`.

## 6. Run the websites

`.npmrc` sets `workspaces=false` because the repository lives on an exFAT volume that cannot hold the
symlinks npm uses for workspaces. **Consequence: `npm run -w <pkg> …` and `npm install -w <pkg>` do
not work here.** All frontend dependencies are pinned in the root `package.json`, and
`@fraudwatch/ui` / `@fraudwatch/contracts` resolve through Vite aliases and TypeScript paths.

Only the root scripts are supported:

```bash
npm install

npm run dev:live         # http://localhost:5173
npm run dev:lab          # http://localhost:5174

npm run build            # build:live then build:lab
npm run build:live
npm run build:lab

npm run preview:live     # http://localhost:4173 (serves the built bundle)
npm run preview:lab      # http://localhost:4174

npm run typecheck        # contracts, ui, live, lab, tests/e2e
npm run test             # vitest over packages/contracts and packages/ui
npm run test:e2e:install # one-time: download the Playwright Chromium build
npm run test:e2e         # Playwright, both sites
```

By default the dev sites call `http://localhost:8000/api/v1`. Override with `VITE_API_BASE_URL`, or by
editing `apps/<app>/public/config.js` (in containers that file is regenerated at start from
`API_BASE_URL`, `LIVE_APP_URL`, `LAB_APP_URL`, `CONTROL_URL` — see `infra/gcp/web/40-fraudwatch-config.sh`).

## 7. First-run checks

Run these in order. Each one tells you which layer is wrong.

```bash
# 1. Containers: every service up, kafka-init exited 0
docker compose --env-file .env -p fraudwatch \
  -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml ps

# 2. Topics created (kafka-init prints "topics ready" and describes the input topic)
docker compose --env-file .env -p fraudwatch -f infra/compose/compose.yaml logs kafka-init

# 3. API liveness and the full component health report
curl -s http://127.0.0.1:8000/healthz
curl -s http://127.0.0.1:8000/api/v1/health

# 4. Scoring service (model loaded?)
docker compose --env-file .env -p fraudwatch -f infra/compose/compose.yaml exec api \
  curl -fsS http://scoring:8001/healthz

# 5. Flink job RUNNING
curl -s http://127.0.0.1:8081/jobs/overview

# 6. Model metadata as the API reports it
curl -s http://127.0.0.1:8000/api/v1/model
```

`GET /api/v1/health` returns `{ as_of, overall, components[] }` with one component per
`api`, `postgres`, `kafka`, `generator`, `scoring`, `result_consumer`, `flink`, `spark`. A `spark`
status of `idle` is correct when the `spark` profile is not running.

Database migrations are applied automatically: `services/api/app/main.py` calls
`fwsvc.db.migrate()` on startup, which applies every `.sql` file under
`services/common/fwsvc/migrations/` in filename order under an advisory lock. You never run
migrations by hand.

## 8. Backend tests

```bash
pytest tests/integration/smoke_scoring.py     # scoring service smoke test
pytest tests/streaming/parity_integration.py  # Java/Python feature parity
bash tests/streaming/run_integration.sh       # streaming integration harness (tests/streaming/compose.test.yaml)
```

The Flink image build already runs `FeatureParityTest` against `tests/streaming/fixtures` and fails
the build if Java features diverge from `fwcore`, so a successful image build is itself a parity
check.

## 9. Troubleshooting

**Compose exits immediately with "set POSTGRES_PASSWORD in .env".**
You did not pass `--env-file .env`, or the variable is empty. All five required secrets use
`${VAR:?…}` and fail fast by design.

**`npm run -w packages/ui build` fails / the package is not found.**
Expected. `.npmrc` disables workspaces (see section 6). Use the root scripts.

**The `generator` image fails at the `COPY … replay …` line.**
The replay pack is missing. Build it (section 3). It is gitignored on purpose.

**The Flink job never reaches RUNNING.**
`flink-submitter` waits for the JobManager REST API and for at least `FLINK_PARALLELISM` free task
slots, and gives up after `FLINK_SUBMITTER_WAIT_S` (default 300 s). The default TaskManager offers
`taskmanager.numberOfTaskSlots: 2`, so `FLINK_PARALLELISM` above 2 will never be satisfiable with one
TaskManager. Check `docker compose … logs flink-submitter`.

**The Flink job restarts in a loop.**
The job's restart strategy is fixed-delay, 10 attempts, 5 s apart. The usual cause is `scoring` being
unreachable; the submitter depends on `scoring` being healthy, but a scoring crash after submission
will surface as job restarts. Check `logs scoring`.

**A `FLINK_PARALLELISM` or state-layout change makes the job fail on restore.**
The submitter restores from the newest complete retained checkpoint. To start clean, set
`FLINK_RESTORE_PATH` to an explicit path, or remove the `flink-checkpoints` volume
(`down -v` removes it along with everything else).

**No events appear on the site.**
Nothing is produced until you start a run. Confirm `generator` is healthy, then start a run from the
Live site's scenario page or through `POST /api/v1/runs`. Also confirm the run targets an engine that
is actually running — a run targeting `spark` produces nothing when the `spark` profile is down.

**Events are produced but nothing is persisted.**
Check `result-consumer` logs, and check the DLQ topic `fraudwatch.dlq` for validation failures.
`GET /api/v1/system/data-quality` summarises malformed, duplicate, late and unmatched counts
(DLQ-derived counts are operator-only).

**Port already in use.** Both Vite servers use `strictPort: true` and will not silently move. Change
`API_HOST_PORT`, `POSTGRES_HOST_PORT`, `FLINK_UI_HOST_PORT` or `SPARK_UI_HOST_PORT` in `.env` for the
backend ports.

**Kafka will not start after you changed `KAFKA_CLUSTER_ID`.** The existing `kafka-data` volume is
formatted with the old id. Either restore the old value or `down -v`.

**CORS errors in the browser.** Add your origin to `ALLOWED_ORIGINS` (default
`http://localhost:5173,http://localhost:5174`) and restart `api`.

**Windows line endings.** `.gitattributes` and the images' `sed -i 's/\r$//'` steps handle the shell
scripts that are copied into containers. If you add a new shell script that runs inside a container,
keep it LF.
