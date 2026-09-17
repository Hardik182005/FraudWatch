# The Apache stack

Kafka, Flink and Spark as they are actually configured in this repository. Every value below was read
from `infra/compose/compose.yaml`, `infra/compose/compose.streaming.yaml`,
`streaming/flink/src/main/java/org/fraudwatch/flink/FraudWatchJob.java`, `streaming/flink/submit.sh`,
`streaming/flink/Dockerfile`, `streaming/spark/job.py`, `streaming/spark/run.sh` and
`streaming/spark/Dockerfile`. The wire formats are in [`contracts.md`](contracts.md).

> Note: `compose.streaming.yaml`'s header comment points at `docs/apache-streaming.md`. That file does
> not exist; this file is the streaming documentation.

## 1. Apache Kafka 4.1.2, KRaft mode

Image `apache/kafka:4.1.2`. One container that is both broker and controller — there is no ZooKeeper
and no second node.

| Setting | Value |
|---|---|
| `KAFKA_PROCESS_ROLES` | `broker,controller` |
| `KAFKA_NODE_ID` / `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1` / `1@kafka:9093` |
| Listeners | `PLAINTEXT://:9092` (in-network), `CONTROLLER://:9093`, `HOST://:29092` |
| Advertised | `PLAINTEXT://kafka:9092`, `HOST://localhost:29092` |
| `CLUSTER_ID` | `${KAFKA_CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qg}` |
| Replication factors (offsets, txn log) | 1 (single node) |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` — topics come only from `kafka-init` |
| `KAFKA_NUM_PARTITIONS` | 1 (default; the topics below set their own) |
| Retention | 24 h **or** 2 GiB per log, whichever hits first |
| Heap | `-Xms512m -Xmx1g`, container `mem_limit: 1500m` |

Everything is PLAINTEXT. That is acceptable only because the broker is reachable solely inside the
Compose network plus `127.0.0.1:29092` on the host. Do not expose 29092.

### Topics (`kafka-init`, one-shot, `--if-not-exists`)

| Topic | Partitions | Config |
|---|---|---|
| `fraudwatch.transactions` | 4 | `message.timestamp.type=LogAppendTime` |
| `fraudwatch.results.flink` | 4 | — |
| `fraudwatch.results.spark` | 4 | — |
| `fraudwatch.windows.flink` | 1 | — |
| `fraudwatch.windows.spark` | 1 | — |
| `fraudwatch.dlq` | 1 | — |
| `fraudwatch.metrics.spark` | 1 | — |

`LogAppendTime` on the input topic matters: the broker stamps each record with its own append time, so
the Kafka timestamp is a broker-side arrival clock and is independent of the `event_time` the engines
use for watermarks. Both engines carry the Kafka partition/offset/timestamp through to their result
records.

`kafka-init` ends with `kafka-topics.sh --describe` on the transactions topic and prints
`topics ready`; `flink-submitter` and `spark` both declare
`kafka-init: { condition: service_completed_successfully }`, so neither starts before the topics exist.

Useful inspection commands (run inside the `kafka` container):

```bash
C='docker compose --env-file .env -p fraudwatch -f infra/compose/compose.yaml exec kafka'
$C /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
$C /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups
$C /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
     --topic fraudwatch.dlq --from-beginning --max-messages 5
```

Consumer groups are independent by design so the two engines read the same input without competing:
Flink uses `FLINK_GROUP_ID` (default `fw-flink-v1`); Spark's Structured Streaming Kafka source manages
its own offsets in its checkpoint and does not commit to a shared group; `result-consumer` uses its own
group per results topic.

## 2. Apache Flink 1.20.5

Image built from `streaming/flink/Dockerfile`: `maven:3.9.11-eclipse-temurin-17` build stage producing a
shaded `fraudwatch-flink.jar`, final stage `flink:1.20.5-java17` plus `jq`. Image labels record
`org.fraudwatch.flink.version=1.20.5` and `org.fraudwatch.kafka-connector=3.4.0-1.20`.

**The build runs the tests.** `mvn package` executes the unit tests and `FeatureParityTest` against
`tests/streaming/fixtures`, and the build fails if the Java feature implementation diverges from the
Python `fwcore`. A green image build is a parity guarantee.

### Cluster (session mode)

`flink-jobmanager` and `flink-taskmanager` share one `FLINK_PROPERTIES` block:

```
jobmanager.memory.process.size: 1024m
taskmanager.memory.process.size: 2048m
taskmanager.memory.managed.fraction: 0.1
taskmanager.numberOfTaskSlots: 2
parallelism.default: 2
state.backend.type: hashmap
execution.checkpointing.dir:            file:///opt/flink/checkpoints
execution.checkpointing.savepoint-dir:  file:///opt/flink/savepoints
web.cancel.enable: true
web.submit.enable: false
```

`web.submit.enable: false` means **you cannot upload a jar through the Web UI** — submission is only
via `submit.sh`. Cancelling from the UI is allowed. The UI/REST endpoint is published on
`127.0.0.1:${FLINK_UI_HOST_PORT:-8081}`.

`state.backend.type: hashmap` keeps all keyed state on the JVM heap; checkpoints are written to the
`flink-checkpoints` Docker volume, which both the JobManager, the TaskManager and the submitter mount.
There is one TaskManager offering 2 slots, so `FLINK_PARALLELISM > 2` can never be scheduled.

### Job graph

`FraudWatchJob` (job name `fraudwatch-flink`), per its own javadoc and code:

```
KafkaSource(fraudwatch.transactions)
  -> watermarks: forBoundedOutOfOrderness(5 s), withIdleness(30 s),
     timestamp = record.eventTime (invalid rows get Long.MIN_VALUE so they cannot move the watermark)
  -> process(RouteFunction)  [uid "route"]
       invalid -> side output -> fraudwatch.dlq
       heartbeats dropped; transactions not targeting flink dropped
  -> keyBy(session:account) -> process(FeatureFunction)  [uid "features"]
       keyed ValueState, TTL FLINK_STATE_TTL_HOURS (default 72 h), late policy applied here
  -> AsyncDataStream.unorderedWait(ScoringAsyncFunction, 15 000 ms, capacity)
       inner HTTP: 3 attempts, 200 ms backoff, 3 s per attempt
  -> fraudwatch.results.flink   (AT_LEAST_ONCE sink)
  -> filter(!late) -> keyBy(session)
     -> TumblingEventTimeWindows.of(60 s), allowedLateness(ZERO), sideOutputLateData
     -> aggregate -> fraudwatch.windows.flink
     side output -> LateCounter -> DiscardingSink
```

The async operator timeout (15 s) is deliberately far above the worst-case inner retry budget
(3 x 3 s + 2 x 200 ms), so the operator timeout only fires on a genuinely stuck request.

Every operator has an explicit `uid(...)`, which is what makes restoring state across a resubmit or a
job change possible.

### Checkpointing

Set in the job's own `Configuration` (so it is part of the job, not just the cluster):

| Key | Value |
|---|---|
| `execution.checkpointing.interval` | `10 s` |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` |
| `execution.checkpointing.min-pause` | `2 s` |
| `execution.checkpointing.timeout` | `60 s` |
| `execution.checkpointing.max-concurrent-checkpoints` | `1` |
| `execution.checkpointing.externalized-checkpoint-retention` | `RETAIN_ON_CANCELLATION` |
| `execution.checkpointing.storage` / `dir` | `filesystem` / `FLINK_CHECKPOINT_DIR` (`file:///opt/flink/checkpoints`) |
| `restart-strategy.type` | `fixed-delay`, 10 attempts, `5 s` apart |
| `pipeline.auto-watermark-interval` | `200 ms` |

`EXACTLY_ONCE` here is Flink's **checkpoint alignment mode** — it is about consistent state snapshots.
The Kafka sinks use `DeliveryGuarantee.AT_LEAST_ONCE` with idempotent producers (`acks=all`,
`enable.idempotence=true`, `linger.ms=5`, `compression.type=lz4`), so results can be duplicated after a
failover. End-to-end exactly-once is recovered downstream, not in Kafka: `result-consumer` upserts on
`(engine, event_id)`. Do not describe the pipeline as end-to-end exactly-once.

The source uses `committedOffsets(OffsetResetStrategy.LATEST)` with
`commit.offsets.on.checkpoint=true` and 60 s partition discovery. Offsets committed to the group are
for observability; recovery correctness comes from the checkpoint.

`RETAIN_ON_CANCELLATION` is what makes the submitter's restore possible — a cancelled job leaves its
checkpoints on the volume.

### Submitting and restoring (`streaming/flink/submit.sh`)

`flink-submitter` runs `submit.sh` as its entrypoint, with
`restart: on-failure:5`, after the JobManager is healthy, the TaskManager has started, `kafka-init`
completed and `scoring` is healthy. The script is idempotent:

1. Wait (up to `FLINK_SUBMITTER_WAIT_S`, default 300 s) for `GET /overview` to report at least one
   TaskManager and `slots-total >= FLINK_PARALLELISM`.
2. Query `GET /jobs/overview`; if a job named `fraudwatch-flink` is in `RUNNING`, `RESTARTING`,
   `CREATED`, `INITIALIZING`, `RECONCILING`, `CANCELLING` or `FAILING`, **do nothing**.
3. Otherwise submit the jar. Restore point: `FLINK_RESTORE_PATH` if set, else the newest *complete*
   retained checkpoint — a `chk-N` directory that actually contains a `_metadata` file, chosen by
   modification time. An incomplete checkpoint directory is therefore never used.
4. Prune old job checkpoint directories, keeping `FLINK_KEEP_CHECKPOINT_JOB_DIRS` (default 3) and
   always keeping the one being restored from.
5. Wait until the job is `RUNNING`, then exit 0.

`FLINK_SUBMITTER_MODE=watch` loops every `FLINK_SUBMITTER_INTERVAL_S` (default 30 s) instead of
exiting, which re-submits after a JobManager restart or after the restart strategy is exhausted.
Default is `once`.

Note that the job's `main()` runs in the **submitter's** JVM (`JVM_ARGS: -Xmx384m`), so
`KAFKA_BOOTSTRAP_SERVERS`, `SCORING_SERVICE_URL`, `FLINK_RUN_LABEL`, `FLINK_GROUP_ID`,
`FLINK_STATE_TTL_HOURS` and the topic names are read there and baked into the job graph. Changing any
of them requires cancelling and resubmitting, not just restarting a TaskManager.

Manual operations:

```bash
curl -s http://127.0.0.1:8081/overview        | jq         # cluster, slots, flink-version
curl -s http://127.0.0.1:8081/jobs/overview   | jq         # job id and state
JID=$(curl -s http://127.0.0.1:8081/jobs/overview | jq -r '.jobs[0].jid')
curl -s http://127.0.0.1:8081/jobs/$JID/checkpoints | jq '.counts, .latest.completed.external_path'
curl -s http://127.0.0.1:8081/jobs/$JID/exceptions  | jq '.["root-exception"]'

# cancel, then re-run the submitter (it will restore from the retained checkpoint)
curl -s -X PATCH "http://127.0.0.1:8081/jobs/$JID?mode=cancel"
docker compose --env-file .env -p fraudwatch \
  -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml up flink-submitter
```

## 3. Apache Spark 3.5.9 Structured Streaming

Image `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu`, with
`spark-sql-kafka-0-10_2.12:3.5.9`, `spark-token-provider-kafka-0-10_2.12:3.5.9`,
`kafka-clients:3.4.1` and `commons-pool2:2.11.1` baked into `/opt/spark/jars` with pinned SHA-1s — no
`--packages` download at runtime. `fwcore` is pip-installed into the image and the build fails unless
`import fwcore.features, fwcore.schema` succeeds, so the Spark job uses exactly the same Python feature
code as the scoring service. `numpy < 2` is pinned because of PySpark 3.5's pandas/Arrow integration.

Spark sits behind the Compose profile `spark` and is **not** started by a plain `up -d`:

```bash
docker compose --env-file .env -p fraudwatch \
  -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml \
  --profile spark up -d spark
```

It runs `spark-submit --master local[2]` (`streaming/spark/run.sh`) with
`spark.sql.shuffle.partitions=4`, `spark.sql.streaming.stateStore.stateSchemaCheck=true`,
`spark.sql.execution.arrow.pyspark.enabled=true`, `spark.python.worker.reuse=true`, driver memory
`SPARK_DRIVER_MEMORY` (default `1400m`) inside a 2500 MB container, UI on
`127.0.0.1:${SPARK_UI_HOST_PORT:-4040}`, and `stop_grace_period: 40s` so SIGTERM can stop the queries
cleanly (the job installs SIGTERM/SIGINT handlers).

### Query `main`

```
Kafka(fraudwatch.transactions)   maxOffsetsPerTrigger = SPARK_MAX_OFFSETS_PER_TRIGGER (20000)
  -> classify (pandas UDF, fwcore.schema.decode_event semantics): txn | heartbeat | invalid | ignore
  -> event_ts (null for invalid) -> withWatermark("event_ts", "5 seconds")
  -> groupBy(state key).applyInPandasWithState(process_group, Append, GroupStateTimeout.NoTimeout)
  -> foreachBatch:
       invalid -> fraudwatch.dlq
       txns    -> POST /v1/score in chunks of <= 500 (3 attempts, 200 ms backoff, 3 s timeout)
               -> fraudwatch.results.spark, keyed by event_id
```

Two details in `process_group` are load-bearing and easy to break:

- Heartbeats and transactions not targeting Spark are routed to the key `__ignore__`, and invalid rows
  to `__invalid__`, **instead of being filtered before the stateful operator**. This is deliberate:
  Catalyst pushes a filter that does not reference the event-time column below `EventTimeWatermark`,
  which would stop heartbeats from advancing the watermark and stall the windows.
- Within a key, rows are sorted by `(partition, offset)`; a row is late when
  `event_time < GroupState.getCurrentWatermarkMs()`, and state is updated only for non-late rows.
  Spark reports watermark `0` before a watermark exists, which the job maps to `None`.

Trigger is `processingTime=SPARK_TRIGGER_INTERVAL` (default `1 second`) — micro-batch, not continuous
processing. Scoring chunks are dispatched through a `ThreadPoolExecutor` of
`SPARK_SCORING_CONCURRENCY` (default 2). Start position is `SPARK_STARTING_OFFSETS` (default `latest`),
overridden by `SPARK_STARTING_TIMESTAMP_MS` with `startingOffsetsByTimestampStrategy=latest`.

### Query `windows`

Reads `fraudwatch.results.spark`, keeps this launch's non-late records, applies `withWatermark` 5 s,
then `dropDuplicatesWithinWatermark(["event_id"])` — the explicit defence against the at-least-once
result sink — then a 60 s tumbling window per `demo_session_id` in append mode, written to
`fraudwatch.windows.spark` via `foreachBatch`.

`SPARK_WINDOWS_ENABLED=false` disables this second query.

### Metrics

A `StreamingQueryListener` publishes every `StreamingQueryProgress` JSON to
`fraudwatch.metrics.spark`. That is where Spark's own batch durations, input rates and state-operator
numbers come from; the API surfaces them rather than re-measuring.

### Checkpoints

`SPARK_CHECKPOINT_URI`, default `/opt/spark-checkpoints/<SPARK_RUN_LABEL|default>` on the
`spark-checkpoints` volume, with `/windows` as a separate sub-location for the second query. Two
things are fixed before the first checkpoint and cannot be changed afterwards for the same checkpoint
directory: `spark.sql.shuffle.partitions=4` and the state schema (`stateSchemaCheck=true` will fail
loudly rather than silently mis-read state). To change either, use a new `SPARK_RUN_LABEL` — which
gives a new checkpoint path — or delete the `spark-checkpoints` volume.

## 4. Watermarks and late events, side by side

| | Flink | Spark |
|---|---|---|
| Allowed lateness for the stream | `forBoundedOutOfOrderness(5 s)` | `withWatermark("event_ts", "5 seconds")` |
| Idle-source handling | `withIdleness(30 s)` | none; heartbeats routed through the stateful operator keep the watermark moving |
| Watermark emission | `pipeline.auto-watermark-interval: 200 ms` | per micro-batch |
| Late decision point | keyed `FeatureFunction` | `process_group`, against `GroupState.getCurrentWatermarkMs()` |
| Late records | still scored and emitted with `late=true`, excluded from windows | same |
| Window | 60 s tumbling event-time per session, `allowedLateness(ZERO)`, late data to a side output that is counted then discarded | 60 s tumbling per `demo_session_id`, append mode |
| Duplicate defence | downstream upsert on `(engine, event_id)` | `dropDuplicatesWithinWatermark(event_id)` plus the same downstream upsert |
| State retention | `ValueState` TTL `FLINK_STATE_TTL_HOURS` (72 h) | `GroupStateTimeout.NoTimeout` — state is kept for the life of the checkpoint |

Both engines use the same 5 s out-of-orderness and the same 60 s window, and both call the same feature
code (`fwcore` in Python, the parity-tested Java port in Flink) and the same scoring service. That is
what makes any side-by-side comparison meaningful at all — see
[`benchmark-methodology.md`](benchmark-methodology.md) for what such a comparison does and does not
prove.
