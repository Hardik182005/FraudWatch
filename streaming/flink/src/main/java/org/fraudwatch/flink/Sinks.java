package org.fraudwatch.flink;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/** Kafka record serializers for results, windows and DLQ (formats: contracts section 5). */
public final class Sinks {
    private Sinks() {}

    /** fraudwatch.results.flink, key = event_id. emitted_at = serialization time. */
    public static class ResultSerializer implements KafkaRecordSerializationSchema<Records.ScoredRecord> {
        private static final long serialVersionUID = 1L;
        private final String topic;

        public ResultSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Records.ScoredRecord s, KafkaSinkContext ctx, Long timestamp) {
            return new ProducerRecord<>(topic, null, s.f.input.eventId.getBytes(StandardCharsets.UTF_8), toJson(s, System.currentTimeMillis()));
        }

        public static byte[] toJson(Records.ScoredRecord s, long emittedAt) {
            Records.InputRecord in = s.f.input;
            return Json.build(g -> {
                g.writeStartObject();
                g.writeStringField("schema_version", "1.0");
                g.writeStringField("record_type", "scored_txn");
                g.writeStringField("engine", "flink");
                Json.nullableString(g, "engine_job_id", s.engineJobId);
                Json.nullableString(g, "engine_run_label", s.engineRunLabel);
                g.writeFieldName("event");
                g.writeRawValue(in.eventJson);
                g.writeObjectFieldStart("kafka");
                g.writeNumberField("partition", in.partition);
                g.writeNumberField("offset", in.offset);
                g.writeNumberField("timestamp", in.kafkaTimestamp);
                g.writeEndObject();
                g.writeBooleanField("late", s.f.late);
                Json.nullableLong(g, "watermark_ms", s.f.watermarkMs);
                g.writeFieldName("raw_features");
                Json.rawFeatures(g, s.f.raw);
                g.writeObjectFieldStart("scoring");
                g.writeStringField("status", s.status);
                g.writeNumberField("attempts", s.attempts);
                Json.nullableString(g, "error", s.error);
                g.writeFieldName("response");
                if (s.responseJson == null) {
                    g.writeNull();
                } else {
                    g.writeRawValue(s.responseJson);
                }
                g.writeEndObject();
                g.writeObjectFieldStart("timing");
                g.writeNumberField("engine_received_at", in.receivedAt);
                g.writeNumberField("scoring_started_at", s.scoringStartedAt);
                g.writeNumberField("scoring_completed_at", s.scoringCompletedAt);
                g.writeNumberField("emitted_at", emittedAt);
                g.writeEndObject();
                g.writeEndObject();
            });
        }
    }

    /** fraudwatch.windows.flink, key = engine:session:window_start. */
    public static class WindowSerializer implements KafkaRecordSerializationSchema<Records.WindowAgg> {
        private static final long serialVersionUID = 1L;
        private final String topic;

        public WindowSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Records.WindowAgg w, KafkaSinkContext ctx, Long timestamp) {
            long emittedAt = System.currentTimeMillis();
            byte[] value = Json.build(g -> {
                g.writeStartObject();
                g.writeStringField("schema_version", "1.0");
                g.writeStringField("record_type", "window");
                g.writeStringField("engine", "flink");
                g.writeStringField("demo_session_id", w.sessionId);
                g.writeNumberField("window_start", w.windowStart);
                g.writeNumberField("window_end", w.windowEnd);
                g.writeNumberField("txn_count", w.txnCount);
                g.writeNumberField("amount_sum_inr", w.amountSumInr);
                g.writeNumberField("alert_count", w.alertCount);
                g.writeNumberField("review_count", w.reviewCount);
                g.writeNumberField("unscored_count", w.unscoredCount);
                g.writeNumberField("emitted_at", emittedAt);
                g.writeEndObject();
            });
            String key = "flink:" + w.sessionId + ":" + w.windowStart;
            return new ProducerRecord<>(topic, null, key.getBytes(StandardCharsets.UTF_8), value);
        }
    }

    /** fraudwatch.dlq, no key. */
    public static class DlqSerializer implements KafkaRecordSerializationSchema<Records.InputRecord> {
        private static final long serialVersionUID = 1L;
        private final String topic;

        public DlqSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Records.InputRecord r, KafkaSinkContext ctx, Long timestamp) {
            byte[] value = Json.build(g -> {
                g.writeStartObject();
                g.writeStringField("engine", "flink");
                Json.nullableString(g, "error", r.error);
                Json.nullableString(g, "raw", r.rawText);
                g.writeObjectFieldStart("kafka");
                g.writeNumberField("partition", r.partition);
                g.writeNumberField("offset", r.offset);
                g.writeNumberField("timestamp", r.kafkaTimestamp);
                g.writeEndObject();
                g.writeNumberField("received_at", r.receivedAt);
                g.writeEndObject();
            });
            return new ProducerRecord<>(topic, null, null, value);
        }
    }
}
