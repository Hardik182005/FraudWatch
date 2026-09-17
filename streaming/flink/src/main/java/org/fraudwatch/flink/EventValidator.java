package org.fraudwatch.flink;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Port of {@code fwcore.schema.decode_event / validate_event}. Error messages match the Python
 * reference (except the text of JSON syntax errors, which is parser specific).
 *
 * <p>Python type semantics are reproduced on Jackson nodes: {@code int} = integral JSON number
 * (booleans rejected), {@code (int, float)} = any JSON number <em>or boolean</em> (Python's
 * {@code bool} is an {@code int} subclass), {@code str} = textual, {@code list} = array.
 *
 * <p>One documented addition shared with the Spark job: integer epoch fields ({@code event_time},
 * {@code produced_at}, {@code seq}) must fit in a signed 64-bit integer ("&lt;field&gt; out of range").
 */
public final class EventValidator {

    public static final String SCHEMA_VERSION = "1.0";
    public static final double MAX_AMOUNT_INR = 10_000_000.0;

    static final List<String> PRODUCT_CODES = Arrays.asList("W", "C", "R", "H", "S");
    static final List<String> CARD_NETWORKS = Arrays.asList("visa", "mastercard", "american express", "discover", "unknown");
    static final List<String> CARD_TYPES = Arrays.asList("debit", "credit", "other", "unknown");
    static final List<String> EMAIL_DOMAINS = Arrays.asList("gmail.com", "yahoo.com", "hotmail.com", "anonymous.com", "aol.com", "other", "missing");
    static final List<String> DEVICE_TYPES = Arrays.asList("desktop", "mobile", "unknown");
    static final List<String> ENGINES = Arrays.asList("flink", "spark");

    enum T { STR, INT, NUM, BOOL, LIST }

    /** Insertion ordered, same order as {@code _REQUIRED} in schema.py. */
    static final Map<String, T> REQUIRED = new LinkedHashMap<>();
    static {
        REQUIRED.put("schema_version", T.STR);
        REQUIRED.put("kind", T.STR);
        REQUIRED.put("event_id", T.STR);
        REQUIRED.put("demo_session_id", T.STR);
        REQUIRED.put("run_id", T.STR);
        REQUIRED.put("seq", T.INT);
        REQUIRED.put("account_id", T.STR);
        REQUIRED.put("event_time", T.INT);
        REQUIRED.put("produced_at", T.INT);
        REQUIRED.put("amount_inr", T.NUM);
        REQUIRED.put("product_cd", T.STR);
        REQUIRED.put("card_network", T.STR);
        REQUIRED.put("card_type", T.STR);
        REQUIRED.put("email_domain", T.STR);
        REQUIRED.put("device_type", T.STR);
        REQUIRED.put("has_identity", T.BOOL);
        REQUIRED.put("target_engines", T.LIST);
    }

    /** Python's json module accepts NaN/Infinity and rejects trailing data. */
    public static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();

    public static final class ValidationException extends Exception {
        public ValidationException(String message) {
            super(message);
        }
    }

    private EventValidator() {}

    /** Decode + validate. Returns the parsed tree for a valid txn or heartbeat. */
    public static JsonNode decode(byte[] raw) throws ValidationException {
        if (raw == null) {
            throw new ValidationException("invalid JSON: empty record value");
        }
        JsonNode node;
        try {
            node = MAPPER.readTree(raw);
        } catch (Exception e) {
            String msg = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            int nl = msg.indexOf('\n');
            throw new ValidationException("invalid JSON: " + (nl > 0 ? msg.substring(0, nl) : msg));
        }
        if (node == null || node.isMissingNode()) {
            throw new ValidationException("invalid JSON: empty document");
        }
        validate(node);
        return node;
    }

    public static boolean isHeartbeat(JsonNode event) {
        JsonNode k = event.get("kind");
        return k != null && k.isTextual() && "heartbeat".equals(k.textValue());
    }

    public static void validate(JsonNode event) throws ValidationException {
        if (event == null || !event.isObject()) {
            throw new ValidationException("event must be a JSON object");
        }
        if (isHeartbeat(event)) {
            for (String f : new String[]{"event_time", "produced_at"}) {
                JsonNode v = event.get(f);
                if (v == null || !v.isIntegralNumber()) {
                    throw new ValidationException("heartbeat." + f + " must be an integer epoch ms");
                }
                if (!v.canConvertToLong()) {
                    throw new ValidationException("heartbeat." + f + " out of range");
                }
            }
            return;
        }
        for (Map.Entry<String, T> e : REQUIRED.entrySet()) {
            String field = e.getKey();
            JsonNode value = event.get(field);
            if (value == null) {
                throw new ValidationException("missing field: " + field);
            }
            T typ = e.getValue();
            if (typ == T.INT && value.isBoolean()) {
                throw new ValidationException(field + " must be an integer");
            }
            boolean ok;
            switch (typ) {
                case STR: ok = value.isTextual(); break;
                case INT: ok = value.isIntegralNumber(); break;
                case NUM: ok = value.isNumber() || value.isBoolean(); break;
                case BOOL: ok = value.isBoolean(); break;
                case LIST: ok = value.isArray(); break;
                default: ok = false;
            }
            if (!ok) {
                throw new ValidationException(field + " has wrong type");
            }
        }
        if (!SCHEMA_VERSION.equals(event.get("schema_version").textValue())) {
            throw new ValidationException("unsupported schema_version");
        }
        if (!"txn".equals(event.get("kind").textValue())) {
            throw new ValidationException("kind must be txn or heartbeat");
        }
        double amount = amountOf(event);
        if (!(0 < amount && amount <= MAX_AMOUNT_INR)) {
            throw new ValidationException("amount_inr out of range");
        }
        if (!PRODUCT_CODES.contains(event.get("product_cd").textValue())) {
            throw new ValidationException("invalid product_cd");
        }
        if (!CARD_NETWORKS.contains(event.get("card_network").textValue())) {
            throw new ValidationException("invalid card_network");
        }
        if (!CARD_TYPES.contains(event.get("card_type").textValue())) {
            throw new ValidationException("invalid card_type");
        }
        if (!EMAIL_DOMAINS.contains(event.get("email_domain").textValue())) {
            throw new ValidationException("invalid email_domain");
        }
        if (!DEVICE_TYPES.contains(event.get("device_type").textValue())) {
            throw new ValidationException("invalid device_type");
        }
        JsonNode targets = event.get("target_engines");
        if (targets.size() == 0) {
            throw new ValidationException("invalid target_engines");
        }
        for (JsonNode t : targets) {
            if (!t.isTextual() || !ENGINES.contains(t.textValue())) {
                throw new ValidationException("invalid target_engines");
            }
        }
        String acct = event.get("account_id").textValue();
        String sess = event.get("demo_session_id").textValue();
        if (acct.codePointCount(0, acct.length()) > 64 || sess.codePointCount(0, sess.length()) > 64) {
            throw new ValidationException("identifier too long");
        }
        // Documented addition (also applied by the Spark job): epoch/int fields must fit int64.
        for (String f : new String[]{"seq", "event_time", "produced_at"}) {
            if (!event.get(f).canConvertToLong()) {
                throw new ValidationException(f + " out of range");
            }
        }
    }

    /** {@code float(event["amount_inr"])} (a JSON boolean counts as 1.0 / 0.0 like Python). */
    public static double amountOf(JsonNode event) {
        JsonNode a = event.get("amount_inr");
        if (a.isBoolean()) {
            return a.booleanValue() ? 1.0 : 0.0;
        }
        return a.doubleValue();
    }
}
