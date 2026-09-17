"""FraudWatch Spark Structured Streaming job (contracts v1, docs/contracts.md sections 1-5).

Query ``main`` (micro-batch, trigger ``SPARK_TRIGGER_INTERVAL``)::

    Kafka(fraudwatch.transactions)
      -> classify (pandas UDF, fwcore.schema.decode_event semantics): txn | heartbeat | invalid | ignore
      -> event_ts (null for invalid rows) -> withWatermark("event_ts", "5 seconds")
      -> groupBy(state key).applyInPandasWithState(Append, NoTimeout)
            * heartbeats / txns not targeting spark are routed to the key "__ignore__" and invalid rows to
              "__invalid__". They are NOT filtered before the stateful operator on purpose: Catalyst pushes
              filters that do not reference the event-time column below EventTimeWatermark, which would
              stop heartbeats from advancing the watermark.
            * per key: rows sorted by (partition, offset); late = event_time < GroupState.getCurrentWatermarkMs();
              fwcore.features.compute_raw, then update only if not late; state kept as arrays.
      -> foreachBatch: invalid -> fraudwatch.dlq; txns -> POST /v1/score in chunks of <= 500
         (3 attempts, 200 ms backoff, 3 s timeout) -> fraudwatch.results.spark (key event_id)

Query ``windows``: fraudwatch.results.spark (this launch's records, late=false) -> withWatermark 5 s
  -> dropDuplicatesWithinWatermark(event_id) -> 60 s tumbling window per demo_session_id (append)
  -> foreachBatch -> fraudwatch.windows.spark

A StreamingQueryListener publishes every StreamingQueryProgress JSON to fraudwatch.metrics.spark.

Environment (defaults):
  KAFKA_BOOTSTRAP_SERVERS (kafka:9092)        SCORING_SERVICE_URL (http://scoring:8001)
  SPARK_RUN_LABEL (unset)                     SPARK_CHECKPOINT_URI (/opt/spark-checkpoints/<label|default>)
  SPARK_STARTING_OFFSETS (latest)             SPARK_STARTING_TIMESTAMP_MS (unset; overrides offsets)
  SPARK_FILTER_SESSION_IDS / SPARK_FILTER_RUN_IDS (comma lists, unset = all)
  SPARK_EMIT_DLQ (true unless a filter is set) SPARK_TRIGGER_INTERVAL (1 second)
  SPARK_MAX_OFFSETS_PER_TRIGGER (20000)       SPARK_SCORING_CONCURRENCY (2)
  SPARK_WINDOWS_ENABLED (true)                SPARK_LOG_LEVEL (WARN)
  TOPIC_TRANSACTIONS / TOPIC_RESULTS_SPARK / TOPIC_WINDOWS_SPARK / TOPIC_DLQ / TOPIC_METRICS_SPARK
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

import pandas as pd
import requests
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from fwcore.features import AccountState, compute_raw, update
from fwcore.schema import EventValidationError, validate_event

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s fraudwatch-spark %(message)s")
log = logging.getLogger("fraudwatch-spark")


def env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return default if v is None or not v.strip() else v.strip()


def env_list(name: str) -> list[str]:
    v = env(name)
    return [x.strip() for x in v.split(",") if x.strip()] if v else []


BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_URL = env("SCORING_SERVICE_URL", "http://scoring:8001").rstrip("/") + "/v1/score"
RUN_LABEL = env("SPARK_RUN_LABEL")
TOPIC_TXN = env("TOPIC_TRANSACTIONS", "fraudwatch.transactions")
TOPIC_RESULTS = env("TOPIC_RESULTS_SPARK", "fraudwatch.results.spark")
TOPIC_WINDOWS = env("TOPIC_WINDOWS_SPARK", "fraudwatch.windows.spark")
TOPIC_DLQ = env("TOPIC_DLQ", "fraudwatch.dlq")
TOPIC_METRICS = env("TOPIC_METRICS_SPARK", "fraudwatch.metrics.spark")
CHECKPOINT = env("SPARK_CHECKPOINT_URI", f"{env('SPARK_CHECKPOINT_ROOT', '/opt/spark-checkpoints')}/{RUN_LABEL or 'default'}").rstrip("/")
STARTING_OFFSETS = env("SPARK_STARTING_OFFSETS", "latest")
STARTING_TS = env("SPARK_STARTING_TIMESTAMP_MS")
FILTER_SESSIONS = frozenset(env_list("SPARK_FILTER_SESSION_IDS"))
FILTER_RUNS = frozenset(env_list("SPARK_FILTER_RUN_IDS"))
EMIT_DLQ = env("SPARK_EMIT_DLQ", "false" if (FILTER_SESSIONS or FILTER_RUNS) else "true").lower() == "true"
TRIGGER = env("SPARK_TRIGGER_INTERVAL", "1 second")
MAX_OFFSETS = env("SPARK_MAX_OFFSETS_PER_TRIGGER", "20000")
SCORING_CONCURRENCY = int(env("SPARK_SCORING_CONCURRENCY", "2"))
WINDOWS_ENABLED = env("SPARK_WINDOWS_ENABLED", "true").lower() == "true"

MAX_ATTEMPTS = 3
BACKOFF_S = 0.2
ATTEMPT_TIMEOUT_S = 3.0
CHUNK = 500
RAW_LIMIT = 4096
INT64_MIN, INT64_MAX = -(2 ** 63), 2 ** 63 - 1
# Spark timestamps are microseconds; event times outside this range cannot be represented and are not
# used for the watermark (the record itself is still processed with its integer event_time).
TS_SAFE_MAX_MS = 253_402_300_799_999

IGNORE_KEY = "__ignore__"
INVALID_KEY = "__invalid__"

CLASSIFY_SCHEMA = T.StructType([
    T.StructField("kind", T.StringType()),
    T.StructField("state_key", T.StringType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.LongType()),
    T.StructField("wm_ms", T.LongType()),
    T.StructField("amount_inr", T.DoubleType()),
    T.StructField("text", T.StringType()),
    T.StructField("error", T.StringType()),
])

OUTPUT_SCHEMA = T.StructType([
    T.StructField("out_kind", T.StringType()),
    T.StructField("state_key", T.StringType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("error", T.StringType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("kafka_ts", T.LongType()),
    T.StructField("event_time", T.LongType()),
    T.StructField("late", T.BooleanType()),
    T.StructField("watermark_ms", T.LongType()),
    T.StructField("raw_json", T.StringType()),
    T.StructField("received_at", T.LongType()),
])
OUTPUT_COLUMNS = [f.name for f in OUTPUT_SCHEMA.fields]

STATE_SCHEMA = T.StructType([
    T.StructField("entry_ts", T.ArrayType(T.LongType(), containsNull=False)),
    T.StructField("entry_amt", T.ArrayType(T.DoubleType(), containsNull=False)),
    T.StructField("total_count", T.LongType()),
    T.StructField("amount_mean", T.DoubleType()),
    T.StructField("max_ts", T.LongType()),
])


# --------------------------------------------------------------------------------------------------
# classification (runs in Python workers)
# --------------------------------------------------------------------------------------------------
def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def classify_one(value) -> tuple:
    """(kind, state_key, event_id, event_time, wm_ms, amount_inr, text, error) for one Kafka value."""
    if value is None:
        return ("invalid", INVALID_KEY, None, None, None, None, "", "invalid JSON: empty record value")
    raw = bytes(value)
    try:
        try:
            event = json.loads(raw)
        except (ValueError, UnicodeDecodeError, RecursionError) as exc:
            raise EventValidationError(f"invalid JSON: {exc}") from exc
        validate_event(event)
        # Documented addition shared with the Flink job: integer fields must fit a signed 64-bit long.
        if event.get("kind") == "heartbeat":
            for f in ("event_time", "produced_at"):
                if not INT64_MIN <= event[f] <= INT64_MAX:
                    raise EventValidationError(f"heartbeat.{f} out of range")
        else:
            for f in ("seq", "event_time", "produced_at"):
                if not INT64_MIN <= event[f] <= INT64_MAX:
                    raise EventValidationError(f"{f} out of range")
    except Exception as exc:  # noqa: BLE001 - every failure is a DLQ record, never a crash
        msg = str(exc) if isinstance(exc, EventValidationError) else f"invalid JSON: {type(exc).__name__}: {exc}"
        text = raw.decode("utf-8", errors="replace")[:RAW_LIMIT]
        return ("invalid", INVALID_KEY, None, None, None, None, text, msg)

    et = event["event_time"]
    wm = et if 0 <= et <= TS_SAFE_MAX_MS else None
    if event.get("kind") == "heartbeat":
        return ("heartbeat", IGNORE_KEY, None, et, wm, None, None, None)
    if "spark" not in event["target_engines"] \
            or (FILTER_SESSIONS and event["demo_session_id"] not in FILTER_SESSIONS) \
            or (FILTER_RUNS and event["run_id"] not in FILTER_RUNS):
        return ("ignore", IGNORE_KEY, None, et, wm, None, None, None)
    key = f"{event['demo_session_id']}:{event['account_id']}"
    return ("txn", key, event["event_id"], et, wm, float(event["amount_inr"]),
            raw.decode("utf-8", errors="replace"), None)


@pandas_udf(CLASSIFY_SCHEMA)
def classify(values: pd.Series) -> pd.DataFrame:
    rows = [classify_one(v) for v in values]
    df = pd.DataFrame(rows, columns=[f.name for f in CLASSIFY_SCHEMA.fields])
    for c in ("event_time", "wm_ms"):
        df[c] = df[c].astype("Int64")
    df["amount_inr"] = df["amount_inr"].astype("float64")
    return df


# --------------------------------------------------------------------------------------------------
# stateful per-account features (runs in Python workers)
# --------------------------------------------------------------------------------------------------
def _state_from(state: GroupState) -> AccountState:
    st = AccountState()
    if state.exists:
        entry_ts, entry_amt, total, mean, max_ts = state.get
        st.entries = [(int(t), float(a)) for t, a in zip(entry_ts, entry_amt)]
        st.total_count = int(total)
        st.amount_mean = float(mean)
        st.max_ts = int(max_ts)
    return st


def process_group(key: tuple, pdfs: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    (skey,) = key
    frames = list(pdfs)
    if skey == IGNORE_KEY or not frames:
        return
    pdf = pd.concat(frames, ignore_index=True)
    now = int(state.getCurrentProcessingTimeMs())  # micro-batch timestamp
    out = []
    if skey == INVALID_KEY:
        for r in pdf.itertuples(index=False):
            out.append((r.kind, skey, None, r.text, r.error, int(r.partition), int(r.offset), int(r.kafka_ts),
                        None, False, None, None, now))
        yield pd.DataFrame(out, columns=OUTPUT_COLUMNS)
        return

    wm_raw = int(state.getCurrentWatermarkMs())
    wm = wm_raw if wm_raw > 0 else None  # Spark reports 0 before the first watermark exists
    pdf = pdf.sort_values(["partition", "offset"], kind="mergesort")
    st = _state_from(state)
    changed = False
    for r in pdf.itertuples(index=False):
        et = int(r.event_time)
        amount = float(r.amount_inr)
        late = wm is not None and et < wm
        raw = compute_raw(st, et, amount)
        if not late:
            update(st, et, amount)
            changed = True
        out.append(("txn", skey, r.event_id, r.text, None, int(r.partition), int(r.offset), int(r.kafka_ts),
                    et, late, wm, json.dumps(raw, separators=(",", ":"), allow_nan=True), now))
    if changed:
        state.update(([e[0] for e in st.entries], [e[1] for e in st.entries],
                      st.total_count, st.amount_mean, st.max_ts))
    df = pd.DataFrame(out, columns=OUTPUT_COLUMNS)
    df["watermark_ms"] = df["watermark_ms"].astype("Int64")
    yield df


# --------------------------------------------------------------------------------------------------
# driver side: scoring + Kafka output
# --------------------------------------------------------------------------------------------------
_producer: Producer | None = None
_producer_lock = threading.Lock()
_http = threading.local()


def producer() -> Producer:
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = Producer({
                "bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True,
                "linger.ms": 5, "compression.type": "lz4", "client.id": f"fraudwatch-spark-{RUN_LABEL or 'default'}",
            })
        return _producer


class DeliveryTracker:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def __call__(self, err, msg) -> None:
        if err is not None:
            self.errors.append(str(err))


def produce_all(records: list[tuple[str, bytes | None, bytes]]) -> None:
    """Produce (topic, key, value) records and block until all are acknowledged; raise on any failure."""
    if not records:
        return
    p = producer()
    tracker = DeliveryTracker()
    for topic, key, value in records:
        while True:
            try:
                p.produce(topic, key=key, value=value, on_delivery=tracker)
                break
            except BufferError:
                p.poll(0.2)
        p.poll(0)
    remaining = p.flush(60)
    if remaining or tracker.errors:
        raise RuntimeError(f"Kafka produce failed: remaining={remaining} errors={tracker.errors[:3]}")


def _session() -> requests.Session:
    s = getattr(_http, "session", None)
    if s is None:
        s = requests.Session()
        _http.session = s
    return s


def score_chunk(rows: list) -> list[tuple]:
    """Score <= 500 rows. Returns per row (status, attempts, error, response_json, started_at, completed_at)."""
    body = ('{"engine":"spark","items":[' + ",".join(
        '{"event":%s,"raw":%s,"late":%s,"watermark_ms":%s}' % (
            r.text, r.raw_json, "true" if r.late else "false",
            "null" if r.watermark_ms is None else int(r.watermark_ms))
        for r in rows) + "]}").encode("utf-8")
    started = int(time.time() * 1000)
    error = None
    attempt = 0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        retryable = False
        try:
            resp = _session().post(SCORING_URL, data=body, headers={"Content-Type": "application/json"},
                                   timeout=(ATTEMPT_TIMEOUT_S, ATTEMPT_TIMEOUT_S))
            if resp.status_code == 200:
                try:
                    items = {it.get("event_id"): it for it in resp.json()["items"]}
                except Exception as exc:  # noqa: BLE001
                    error = f"invalid scoring response: {exc}"
                    break
                done = int(time.time() * 1000)
                out = []
                for r in rows:
                    it = items.get(r.event_id)
                    if it is None:
                        out.append(("failed", attempt, "invalid scoring response: response has no item for event_id",
                                    None, started, done))
                    else:
                        out.append(("ok", attempt, None, json.dumps(it, separators=(",", ":")), started, done))
                return out
            text = resp.text or ""
            error = f"HTTP {resp.status_code}: {text[:300]}"
            retryable = resp.status_code >= 500 or resp.status_code in (408, 429)
        except requests.RequestException as exc:
            error = f"{type(exc).__name__}: {exc}"
            retryable = True
        if not retryable or attempt == MAX_ATTEMPTS:
            break
        time.sleep(BACKOFF_S)
    done = int(time.time() * 1000)
    return [("failed", attempt, error, None, started, done) for _ in rows]


def jstr(v) -> str:
    return json.dumps(v, ensure_ascii=False)


class MainBatch:
    def __init__(self, app_id: str) -> None:
        self.app_id = app_id
        self.pool = ThreadPoolExecutor(max_workers=max(1, SCORING_CONCURRENCY), thread_name_prefix="scoring")
        self.totals = {"txn": 0, "late": 0, "invalid": 0, "scoring_failed": 0, "scoring_retries": 0}

    def __call__(self, df, batch_id: int) -> None:
        t0 = time.time()
        rows = df.collect()
        txns = [r for r in rows if r.out_kind == "txn"]
        invalid = [r for r in rows if r.out_kind == "invalid"]
        records: list[tuple[str, bytes | None, bytes]] = []

        if EMIT_DLQ:
            for r in invalid:
                rec = ('{"engine":"spark","error":%s,"raw":%s,"kafka":{"partition":%d,"offset":%d,"timestamp":%d},'
                       '"received_at":%d}') % (jstr(r.error), jstr(r.text or ""), r.partition, r.offset, r.kafka_ts,
                                                r.received_at)
                records.append((TOPIC_DLQ, None, rec.encode("utf-8")))

        chunks = [txns[i:i + CHUNK] for i in range(0, len(txns), CHUNK)]
        results = list(self.pool.map(score_chunk, chunks))
        label = "null" if RUN_LABEL is None else jstr(RUN_LABEL)
        failed = retries = late = 0
        for chunk, scored in zip(chunks, results):
            for r, (status, attempts, error, response, started, completed) in zip(chunk, scored):
                failed += status != "ok"
                retries += attempts - 1
                late += bool(r.late)
                rec = ('{"schema_version":"1.0","record_type":"scored_txn","engine":"spark",'
                       '"engine_job_id":%s,"engine_run_label":%s,"event":%s,'
                       '"kafka":{"partition":%d,"offset":%d,"timestamp":%d},"late":%s,"watermark_ms":%s,'
                       '"raw_features":%s,"scoring":{"status":%s,"attempts":%d,"error":%s,"response":%s},'
                       '"timing":{"engine_received_at":%d,"scoring_started_at":%d,"scoring_completed_at":%d,'
                       '"emitted_at":%d},"spark":{"batch_id":%d}}') % (
                    jstr(self.app_id), label, r.text, r.partition, r.offset, r.kafka_ts,
                    "true" if r.late else "false", "null" if r.watermark_ms is None else int(r.watermark_ms),
                    r.raw_json, jstr(status), attempts, "null" if error is None else jstr(error),
                    "null" if response is None else response,
                    r.received_at, started, completed, int(time.time() * 1000), batch_id)
                records.append((TOPIC_RESULTS, r.event_id.encode("utf-8"), rec.encode("utf-8")))
        produce_all(records)
        self.totals["txn"] += len(txns)
        self.totals["late"] += late
        self.totals["invalid"] += len(invalid)
        self.totals["scoring_failed"] += failed
        self.totals["scoring_retries"] += retries
        if rows:
            log.info("main batch=%d txns=%d late=%d invalid=%d scoring_failed=%d retries=%d chunks=%d took=%.0fms totals=%s",
                     batch_id, len(txns), late, len(invalid), failed, retries, len(chunks),
                     (time.time() - t0) * 1000, self.totals)


def windows_batch(df, batch_id: int) -> None:
    rows = df.collect()
    records = []
    now = int(time.time() * 1000)
    for r in rows:
        ws = int(r.window_start)
        rec = {"schema_version": "1.0", "record_type": "window", "engine": "spark",
               "demo_session_id": r.demo_session_id, "window_start": ws, "window_end": int(r.window_end),
               "txn_count": int(r.txn_count), "amount_sum_inr": float(r.amount_sum_inr),
               "alert_count": int(r.alert_count), "review_count": int(r.review_count),
               "unscored_count": int(r.unscored_count), "emitted_at": now}
        records.append((TOPIC_WINDOWS, f"spark:{r.demo_session_id}:{ws}".encode("utf-8"),
                        json.dumps(rec, separators=(",", ":")).encode("utf-8")))
    produce_all(records)
    if rows:
        log.info("windows batch=%d emitted=%d", batch_id, len(rows))


class MetricsListener(StreamingQueryListener):
    """Publishes each StreamingQueryProgress (JSON, verbatim) to fraudwatch.metrics.spark."""

    def onQueryStarted(self, event) -> None:
        log.info("query started id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event) -> None:
        try:
            p = producer()
            p.produce(TOPIC_METRICS, value=event.progress.json.encode("utf-8"))
            p.poll(0)
        except Exception as exc:  # noqa: BLE001 - metrics must never break the job
            log.warning("could not publish progress: %s", exc)

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        log.info("query terminated id=%s exception=%s", event.id, event.exception)


# --------------------------------------------------------------------------------------------------
def kafka_reader(spark: SparkSession, topic: str):
    r = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", BOOTSTRAP)
         .option("subscribe", topic)
         .option("failOnDataLoss", "false")
         .option("groupIdPrefix", "fw-spark")
         .option("maxOffsetsPerTrigger", MAX_OFFSETS))
    return r


def build_main(spark: SparkSession, app_id: str):
    reader = kafka_reader(spark, TOPIC_TXN)
    if STARTING_TS:
        reader = (reader.option("startingTimestamp", str(int(STARTING_TS)))
                  .option("startingOffsetsByTimestampStrategy", "latest"))
    else:
        reader = reader.option("startingOffsets", STARTING_OFFSETS)
    src = reader.load()
    classified = src.select(
        classify(F.col("value")).alias("c"),
        F.col("partition"), F.col("offset"), F.expr("unix_millis(timestamp)").alias("kafka_ts"),
    ).select("c.*", "partition", "offset", "kafka_ts")
    stream = (classified
              .withColumn("event_ts", F.expr("timestamp_millis(wm_ms)"))
              .withWatermark("event_ts", "5 seconds")
              .groupBy("state_key")
              .applyInPandasWithState(process_group, OUTPUT_SCHEMA, STATE_SCHEMA, "Append",
                                      GroupStateTimeout.NoTimeout))
    return (stream.writeStream
            .queryName(f"fraudwatch-spark-main{'-' + RUN_LABEL if RUN_LABEL else ''}")
            .outputMode("append")
            .trigger(processingTime=TRIGGER)
            .option("checkpointLocation", f"{CHECKPOINT}/main")
            .foreachBatch(MainBatch(app_id)))


def build_windows(spark: SparkSession, start_ms: int):
    schema = T.StructType([
        T.StructField("engine_run_label", T.StringType()),
        T.StructField("late", T.BooleanType()),
        T.StructField("event", T.StructType([
            T.StructField("event_id", T.StringType()),
            T.StructField("demo_session_id", T.StringType()),
            T.StructField("event_time", T.LongType()),
            T.StructField("amount_inr", T.DoubleType()),
        ])),
        T.StructField("scoring", T.StructType([
            T.StructField("response", T.StructType([
                T.StructField("policy", T.StructType([T.StructField("action", T.StringType())])),
            ])),
        ])),
    ])
    src = (kafka_reader(spark, TOPIC_RESULTS)
           .option("startingTimestamp", str(start_ms))
           .option("startingOffsetsByTimestampStrategy", "latest")
           .load())
    label_match = F.col("r.engine_run_label").eqNullSafe(F.lit(RUN_LABEL).cast("string"))
    recs = (src.select(F.from_json(F.col("value").cast("string"), schema).alias("r"))
            .where(label_match & (F.col("r.late") == F.lit(False)))
            .select(F.col("r.event.event_id").alias("event_id"),
                    F.col("r.event.demo_session_id").alias("demo_session_id"),
                    F.col("r.event.amount_inr").alias("amount_inr"),
                    F.col("r.scoring.response.policy.action").alias("action"),
                    F.expr("timestamp_millis(r.event.event_time)").alias("event_ts"))
            .withWatermark("event_ts", "5 seconds")
            .dropDuplicatesWithinWatermark(["event_id"]))
    agg = (recs.groupBy(F.col("demo_session_id"), F.window("event_ts", "60 seconds").alias("w"))
           .agg(F.count(F.lit(1)).alias("txn_count"),
                F.sum("amount_inr").alias("amount_sum_inr"),
                F.sum(F.when(F.col("action") == "alert", 1).otherwise(0)).alias("alert_count"),
                F.sum(F.when(F.col("action") == "review", 1).otherwise(0)).alias("review_count"),
                F.sum(F.when(F.col("action").isNull() | (F.col("action") == "unscored"), 1).otherwise(0)).alias("unscored_count"))
           .select("demo_session_id",
                   F.expr("unix_millis(w.start)").alias("window_start"),
                   F.expr("unix_millis(w.end)").alias("window_end"),
                   "txn_count", "amount_sum_inr", "alert_count", "review_count", "unscored_count"))
    return (agg.writeStream
            .queryName(f"fraudwatch-spark-windows{'-' + RUN_LABEL if RUN_LABEL else ''}")
            .outputMode("append")
            .trigger(processingTime=TRIGGER)
            .option("checkpointLocation", f"{CHECKPOINT}/windows")
            .foreachBatch(windows_batch))


def main() -> int:
    start_ms = int(time.time() * 1000)
    spark = (SparkSession.builder.appName(f"fraudwatch-spark{'-' + RUN_LABEL if RUN_LABEL else ''}")
             .config("spark.sql.shuffle.partitions", "4")  # fixed before the first checkpoint
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))
    app_id = spark.sparkContext.applicationId
    log.info("FRAUDWATCH_SPARK_APP_ID=%s label=%s checkpoint=%s bootstrap=%s scoring=%s trigger=%s "
             "starting=%s filters(sessions=%d runs=%d) dlq=%s",
             app_id, RUN_LABEL, CHECKPOINT, BOOTSTRAP, SCORING_URL, TRIGGER,
             f"timestamp:{STARTING_TS}" if STARTING_TS else STARTING_OFFSETS,
             len(FILTER_SESSIONS), len(FILTER_RUNS), EMIT_DLQ)
    spark.streams.addListener(MetricsListener())

    stopping = threading.Event()

    def on_signal(signum, _frame):
        log.info("signal %s received: stopping queries", signum)
        stopping.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    queries = []
    if WINDOWS_ENABLED:
        queries.append(build_windows(spark, start_ms).start())
    queries.append(build_main(spark, app_id).start())
    log.info("started queries: %s", [(q.name, q.id) for q in queries])

    exit_code = 0
    while not stopping.is_set():
        failed = [q for q in queries if not q.isActive]
        if failed:
            for q in failed:
                log.error("query %s stopped: %s", q.name, q.exception())
            exit_code = 1
            break
        time.sleep(2)
    for q in queries:
        try:
            q.stop()
        except Exception as exc:  # noqa: BLE001
            log.warning("stop %s: %s", q.name, exc)
    if _producer is not None:
        _producer.flush(10)
    spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
