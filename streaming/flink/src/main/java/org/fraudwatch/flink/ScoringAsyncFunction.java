package org.fraudwatch.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Calls POST {SCORING_SERVICE_URL}/v1/score with one item per record.
 * Policy (contracts section 3): 3 attempts total, 200 ms fixed backoff, 3 s timeout per attempt.
 * Retried: connection errors, timeouts, HTTP 408/429/5xx. Not retried: other HTTP 4xx and
 * unparseable 200 responses. After the final failure the record is emitted with status "failed"
 * (never dropped, never given an invented score).
 */
public class ScoringAsyncFunction extends RichAsyncFunction<Records.FeatureRecord, Records.ScoredRecord> {
    private static final long serialVersionUID = 1L;

    public static final int MAX_ATTEMPTS = 3;
    public static final long BACKOFF_MS = 200L;
    public static final long ATTEMPT_TIMEOUT_MS = 3000L;

    private final String scoringUrl;
    private final String runLabel;

    private transient HttpClient client;
    private transient URI uri;
    private transient String jobId;
    private transient Counter retries;
    private transient Counter failures;
    private transient Counter ok;

    public ScoringAsyncFunction(String scoringUrl, String runLabel) {
        this.scoringUrl = scoringUrl;
        this.runLabel = runLabel;
    }

    @Override
    public void open(OpenContext openContext) {
        client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(ATTEMPT_TIMEOUT_MS))
                .build();
        String base = scoringUrl.endsWith("/") ? scoringUrl.substring(0, scoringUrl.length() - 1) : scoringUrl;
        uri = URI.create(base + "/v1/score");
        jobId = getRuntimeContext().getJobInfo().getJobId().toString();
        retries = getRuntimeContext().getMetricGroup().counter("scoring_retries", new ThreadSafeSimpleCounter());
        failures = getRuntimeContext().getMetricGroup().counter("scoring_failures", new ThreadSafeSimpleCounter());
        ok = getRuntimeContext().getMetricGroup().counter("scoring_ok", new ThreadSafeSimpleCounter());
    }

    static byte[] payload(Records.FeatureRecord f) {
        return Json.build(g -> {
            g.writeStartObject();
            g.writeStringField("engine", "flink");
            g.writeArrayFieldStart("items");
            g.writeStartObject();
            g.writeFieldName("event");
            g.writeRawValue(f.input.eventJson);
            g.writeFieldName("raw");
            Json.rawFeatures(g, f.raw);
            g.writeBooleanField("late", f.late);
            Json.nullableLong(g, "watermark_ms", f.watermarkMs);
            g.writeEndObject();
            g.writeEndArray();
            g.writeEndObject();
        });
    }

    @Override
    public void asyncInvoke(Records.FeatureRecord in, ResultFuture<Records.ScoredRecord> resultFuture) {
        byte[] body = payload(in);
        attempt(in, body, 1, System.currentTimeMillis(), resultFuture);
    }

    private void attempt(Records.FeatureRecord in, byte[] body, int attempt, long startedAt,
                         ResultFuture<Records.ScoredRecord> rf) {
        HttpRequest req = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMillis(ATTEMPT_TIMEOUT_MS))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        CompletableFuture<HttpResponse<String>> fut;
        try {
            fut = client.sendAsync(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            fut = CompletableFuture.failedFuture(e);
        }
        fut.whenComplete((resp, err) -> {
            String error;
            boolean retryable;
            if (err == null && resp.statusCode() == 200) {
                try {
                    JsonNode root = EventValidator.MAPPER.readTree(resp.body());
                    JsonNode item = null;
                    for (JsonNode it : root.path("items")) {
                        if (in.input.eventId.equals(it.path("event_id").asText(null))) {
                            item = it;
                            break;
                        }
                    }
                    if (item == null) {
                        throw new IllegalStateException("response has no item for event_id");
                    }
                    ok.inc();
                    complete(rf, in, "ok", attempt, null, item.toString(),
                            item.path("policy").path("action").asText(null), startedAt);
                    return;
                } catch (Exception e) {
                    error = "invalid scoring response: " + e.getMessage();
                    retryable = false;
                }
            } else if (err != null) {
                Throwable c = err.getCause() != null && err instanceof java.util.concurrent.CompletionException ? err.getCause() : err;
                error = c.getClass().getSimpleName() + (c.getMessage() == null ? "" : ": " + c.getMessage());
                retryable = true;
            } else {
                int s = resp.statusCode();
                String b = resp.body() == null ? "" : resp.body();
                error = "HTTP " + s + ": " + (b.length() > 300 ? b.substring(0, 300) : b);
                retryable = s >= 500 || s == 429 || s == 408;
            }
            if (retryable && attempt < MAX_ATTEMPTS) {
                retries.inc();
                CompletableFuture.delayedExecutor(BACKOFF_MS, TimeUnit.MILLISECONDS)
                        .execute(() -> attempt(in, body, attempt + 1, startedAt, rf));
            } else {
                failures.inc();
                complete(rf, in, "failed", attempt, error, null, null, startedAt);
            }
        });
    }

    private void complete(ResultFuture<Records.ScoredRecord> rf, Records.FeatureRecord in, String status, int attempts,
                          String error, String responseJson, String action, long startedAt) {
        Records.ScoredRecord s = new Records.ScoredRecord();
        s.f = in;
        s.engineJobId = jobId;
        s.engineRunLabel = runLabel;
        s.status = status;
        s.attempts = attempts;
        s.error = error;
        s.responseJson = responseJson;
        s.action = action;
        s.scoringStartedAt = startedAt;
        s.scoringCompletedAt = System.currentTimeMillis();
        rf.complete(Collections.singletonList(s));
    }

    /** Operator-level safety net (should not trigger: attempts are bounded well below it). */
    @Override
    public void timeout(Records.FeatureRecord in, ResultFuture<Records.ScoredRecord> rf) {
        failures.inc();
        complete(rf, in, "failed", MAX_ATTEMPTS, "async operator timeout", null, null, System.currentTimeMillis());
    }
}
