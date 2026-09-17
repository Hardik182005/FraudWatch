package org.fraudwatch.flink;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Small JSON writing helpers (streaming generator, raw embedding of verbatim JSON). */
public final class Json {
    public static final JsonFactory FACTORY = new JsonFactory();

    private Json() {}

    public interface Body {
        void write(JsonGenerator g) throws IOException;
    }

    public static byte[] build(Body body) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        try (JsonGenerator g = FACTORY.createGenerator(out)) {
            body.write(g);
        } catch (IOException e) {
            throw new IllegalStateException("JSON serialization failed", e);
        }
        return out.toByteArray();
    }

    public static void nullableLong(JsonGenerator g, String name, Long v) throws IOException {
        if (v == null) {
            g.writeNullField(name);
        } else {
            g.writeNumberField(name, v.longValue());
        }
    }

    public static void nullableDouble(JsonGenerator g, String name, Double v) throws IOException {
        if (v == null) {
            g.writeNullField(name);
        } else {
            g.writeNumberField(name, v.doubleValue());
        }
    }

    public static void nullableString(JsonGenerator g, String name, String v) throws IOException {
        if (v == null) {
            g.writeNullField(name);
        } else {
            g.writeStringField(name, v);
        }
    }

    /** Raw features object in the exact key order of fwcore.features.compute_raw. */
    public static void rawFeatures(JsonGenerator g, Features.RawFeatures r) throws IOException {
        g.writeStartObject();
        g.writeNumberField("amount", r.amount);
        g.writeNumberField("prior_count_60s", r.priorCount60s);
        g.writeNumberField("prior_count_10m", r.priorCount10m);
        g.writeNumberField("prior_count_1h", r.priorCount1h);
        g.writeNumberField("prior_amount_sum_1h", r.priorAmountSum1h);
        g.writeNumberField("small_count_10m", r.smallCount10m);
        nullableDouble(g, "secs_since_last", r.secsSinceLast);
        g.writeNumberField("prior_count_total", r.priorCountTotal);
        nullableDouble(g, "prior_mean_amount", r.priorMeanAmount);
        nullableDouble(g, "amount_ratio", r.amountRatio);
        g.writeEndObject();
    }
}
