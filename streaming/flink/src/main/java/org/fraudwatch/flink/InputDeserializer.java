package org.fraudwatch.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/** Kafka value to InputRecord (valid txn / heartbeat / invalid with error). Never throws. */
public class InputDeserializer implements KafkaRecordDeserializationSchema<Records.InputRecord> {
    private static final long serialVersionUID = 1L;
    static final int RAW_LIMIT = 4096;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Records.InputRecord> out) {
        out.collect(toInput(record.value(), record.partition(), record.offset(), record.timestamp(),
                System.currentTimeMillis()));
    }

    public static Records.InputRecord toInput(byte[] value, int partition, long offset, long kafkaTs, long receivedAt) {
        Records.InputRecord r = new Records.InputRecord();
        r.partition = partition;
        r.offset = offset;
        r.kafkaTimestamp = kafkaTs;
        r.receivedAt = receivedAt;
        r.eventTime = Long.MIN_VALUE;
        try {
            JsonNode node = EventValidator.decode(value);
            if (EventValidator.isHeartbeat(node)) {
                r.kind = Records.KIND_HEARTBEAT;
                r.eventTime = node.get("event_time").longValue();
                return r;
            }
            r.kind = Records.KIND_TXN;
            r.eventJson = new String(value, StandardCharsets.UTF_8);
            r.eventId = node.get("event_id").textValue();
            r.sessionId = node.get("demo_session_id").textValue();
            r.accountId = node.get("account_id").textValue();
            r.eventTime = node.get("event_time").longValue();
            r.amountInr = EventValidator.amountOf(node);
            boolean flink = false;
            for (JsonNode t : node.get("target_engines")) {
                if ("flink".equals(t.textValue())) {
                    flink = true;
                }
            }
            r.targetsFlink = flink;
        } catch (EventValidator.ValidationException e) {
            r.kind = Records.KIND_INVALID;
            r.error = e.getMessage();
            r.rawText = value == null ? "" : truncate(new String(value, StandardCharsets.UTF_8), RAW_LIMIT);
        }
        return r;
    }

    static String truncate(String s, int codePoints) {
        if (s.codePointCount(0, s.length()) <= codePoints) {
            return s;
        }
        return s.substring(0, s.offsetByCodePoints(0, codePoints));
    }

    @Override
    public TypeInformation<Records.InputRecord> getProducedType() {
        return TypeInformation.of(Records.InputRecord.class);
    }
}
