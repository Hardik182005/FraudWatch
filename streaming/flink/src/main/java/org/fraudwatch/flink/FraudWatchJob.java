package org.fraudwatch.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * FraudWatch Flink job (contracts v1).
 *
 * <pre>
 * KafkaSource(fraudwatch.transactions) -> watermarks(bounded 5 s, idleness 30 s)
 *   -> route: invalid -> fraudwatch.dlq | heartbeats dropped | txns not for flink dropped
 *   -> keyBy(session:account) -> features (ValueState, late policy)
 *   -> async scoring (3 attempts, 200 ms backoff, 3 s timeout) -> fraudwatch.results.flink
 *   -> non-late -> keyBy(session) -> 60 s tumbling event-time window -> fraudwatch.windows.flink
 * </pre>
 */
public class FraudWatchJob {
    private static final Logger LOG = LoggerFactory.getLogger(FraudWatchJob.class);

    public static final String JOB_NAME = "fraudwatch-flink";

    static String env(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v.trim();
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String scoringUrl = env("SCORING_SERVICE_URL", "http://scoring:8001");
        String runLabel = env("FLINK_RUN_LABEL", null);
        String inputTopic = env("TOPIC_TRANSACTIONS", "fraudwatch.transactions");
        String resultsTopic = env("TOPIC_RESULTS_FLINK", "fraudwatch.results.flink");
        String windowsTopic = env("TOPIC_WINDOWS_FLINK", "fraudwatch.windows.flink");
        String dlqTopic = env("TOPIC_DLQ", "fraudwatch.dlq");
        String groupId = env("FLINK_GROUP_ID", "fw-flink-v1");
        String checkpointDir = env("FLINK_CHECKPOINT_DIR", "file:///opt/flink/checkpoints");
        int parallelism = Integer.parseInt(env("FLINK_PARALLELISM", "2"));
        int asyncCapacity = Integer.parseInt(env("FLINK_ASYNC_CAPACITY", "50"));
        long stateTtlHours = Long.parseLong(env("FLINK_STATE_TTL_HOURS", "72"));

        Configuration conf = new Configuration();
        conf.setString("execution.checkpointing.interval", "10 s");
        conf.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        conf.setString("execution.checkpointing.min-pause", "2 s");
        conf.setString("execution.checkpointing.timeout", "60 s");
        conf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
        conf.setString("execution.checkpointing.max-concurrent-checkpoints", "1");
        conf.setString("state.backend.type", "hashmap");
        conf.setString("execution.checkpointing.storage", "filesystem");
        conf.setString("execution.checkpointing.dir", checkpointDir);
        // pre-1.20 key names (still honoured as fallbacks)
        conf.setString("state.checkpoint-storage", "filesystem");
        conf.setString("state.checkpoints.dir", checkpointDir);
        conf.setString("restart-strategy.type", "fixed-delay");
        conf.setString("restart-strategy.fixed-delay.attempts", "10");
        conf.setString("restart-strategy.fixed-delay.delay", "5 s");
        conf.setString("pipeline.auto-watermark-interval", "200 ms");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.configure(conf, FraudWatchJob.class.getClassLoader());
        env.setParallelism(parallelism);

        LOG.info("FraudWatch job config: bootstrap={} scoring={} parallelism={} checkpointDir={} runLabel={}",
                bootstrap, scoringUrl, parallelism, checkpointDir, runLabel);

        KafkaSource<Records.InputRecord> source = KafkaSource.<Records.InputRecord>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new InputDeserializer())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("partition.discovery.interval.ms", "60000")
                .build();

        WatermarkStrategy<Records.InputRecord> wms = WatermarkStrategy
                .<Records.InputRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((r, kafkaTs) -> Records.KIND_INVALID.equals(r.kind) ? Long.MIN_VALUE : r.eventTime)
                .withIdleness(Duration.ofSeconds(30));

        DataStream<Records.InputRecord> input = env.fromSource(source, wms, "kafka-transactions")
                .uid("kafka-source").name("kafka-transactions");

        SingleOutputStreamOperator<Records.InputRecord> routed = input
                .process(new RouteFunction())
                .uid("route").name("validate-route");

        // DLQ
        routed.getSideOutput(RouteFunction.INVALID)
                .sinkTo(KafkaSink.<Records.InputRecord>builder()
                        .setBootstrapServers(bootstrap)
                        .setRecordSerializer(new Sinks.DlqSerializer(dlqTopic))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setKafkaProducerConfig(producerProps())
                        .build())
                .uid("dlq-sink").name("kafka-dlq");

        // Keyed features
        SingleOutputStreamOperator<Records.FeatureRecord> features = routed
                .keyBy(Records.InputRecord::stateKey, TypeInformation.of(String.class))
                .process(new FeatureFunction(stateTtlHours))
                .uid("features").name("account-features");

        // Async scoring: worst case 3 x 3 s + 2 x 200 ms, operator timeout well above that
        SingleOutputStreamOperator<Records.ScoredRecord> scored = AsyncDataStream.unorderedWait(
                        features, new ScoringAsyncFunction(scoringUrl, runLabel), 15_000, TimeUnit.MILLISECONDS, asyncCapacity)
                .uid("scoring-async").name("scoring-async");

        scored.sinkTo(KafkaSink.<Records.ScoredRecord>builder()
                        .setBootstrapServers(bootstrap)
                        .setRecordSerializer(new Sinks.ResultSerializer(resultsTopic))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setKafkaProducerConfig(producerProps())
                        .build())
                .uid("results-sink").name("kafka-results");

        // Windows over non-late scored records
        OutputTag<Records.ScoredRecord> windowLate = new OutputTag<Records.ScoredRecord>("window-late") {
            private static final long serialVersionUID = 1L;
        };
        SingleOutputStreamOperator<Records.WindowAgg> windows = scored
                .filter(s -> !s.f.late).uid("window-filter").name("non-late")
                .keyBy(s -> s.f.input.sessionId, TypeInformation.of(String.class))
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(60)))
                .allowedLateness(Duration.ZERO)
                .sideOutputLateData(windowLate)
                .aggregate(new Windows.Agg(), new Windows.Finish())
                .uid("window-agg").name("session-window-60s");

        windows.sinkTo(KafkaSink.<Records.WindowAgg>builder()
                        .setBootstrapServers(bootstrap)
                        .setRecordSerializer(new Sinks.WindowSerializer(windowsTopic))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setKafkaProducerConfig(producerProps())
                        .build())
                .uid("windows-sink").name("kafka-windows");

        windows.getSideOutput(windowLate)
                .map(new Windows.LateCounter()).uid("window-late-counter").name("window-late-counter")
                .sinkTo(new DiscardingSink<>()).uid("window-late-discard").name("window-late-discard");

        env.execute(JOB_NAME);
    }

    static Properties producerProps() {
        Properties p = new Properties();
        p.setProperty("acks", "all");
        p.setProperty("enable.idempotence", "true");
        p.setProperty("linger.ms", "5");
        p.setProperty("compression.type", "lz4");
        return p;
    }
}
