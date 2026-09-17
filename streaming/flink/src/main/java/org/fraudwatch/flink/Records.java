package org.fraudwatch.flink;

/** Flink POJO types flowing through the job (public fields, public no-arg constructors). */
public final class Records {
    private Records() {}

    public static final String KIND_TXN = "txn";
    public static final String KIND_HEARTBEAT = "heartbeat";
    public static final String KIND_INVALID = "invalid";

    /** Deserialized Kafka record: a valid txn, a heartbeat, or an invalid record. */
    public static class InputRecord {
        public String kind;
        /** Original record value text (verbatim) for valid txns. */
        public String eventJson;
        /** First 4096 characters of the value for invalid records. */
        public String rawText;
        public String error;
        public String eventId;
        public String sessionId;
        public String accountId;
        /** Event time (ms); Long.MIN_VALUE for invalid records. */
        public long eventTime;
        public double amountInr;
        public boolean targetsFlink;
        public int partition;
        public long offset;
        public long kafkaTimestamp;
        /** When the engine first handled the record (deserialization). */
        public long receivedAt;

        public InputRecord() {}

        public String stateKey() {
            return sessionId + ":" + accountId;
        }
    }

    /** Output of the keyed feature operator. */
    public static class FeatureRecord {
        public InputRecord input;
        public boolean late;
        /** Operator watermark at processing time; null when no watermark yet. */
        public Long watermarkMs;
        public Features.RawFeatures raw;

        public FeatureRecord() {}
    }

    /** Output of async scoring. */
    public static class ScoredRecord {
        public FeatureRecord f;
        public String engineJobId;
        public String engineRunLabel;
        /** "ok" | "failed" */
        public String status;
        public int attempts;
        public String error;
        /** One scoring response item as JSON text (null when failed). */
        public String responseJson;
        /** policy.action from the response (null when failed). */
        public String action;
        public long scoringStartedAt;
        public long scoringCompletedAt;

        public ScoredRecord() {}
    }

    /** Window accumulator / result. */
    public static class WindowAgg {
        public String sessionId;
        public long windowStart;
        public long windowEnd;
        public long txnCount;
        public double amountSumInr;
        public long alertCount;
        public long reviewCount;
        public long unscoredCount;

        public WindowAgg() {}
    }
}
