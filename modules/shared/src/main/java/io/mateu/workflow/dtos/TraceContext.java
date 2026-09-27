package io.mateu.workflow.dtos;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A W3C Trace Context — {@code traceparent}, {@code tracestate} — and W3C {@code baggage}, as it
 * travels with a process.
 *
 * <p>It is how a process joins the trace of whoever asked for it. A service that starts a process
 * from inside its own trace (a booking arriving from a CRS, say) hands this over — as Kafka record
 * headers named exactly as the W3C spec names them, or as the {@code traceContext} field of
 * {@code ProcessCreationRequested} — and the engine keeps it with the process: its step-overs,
 * dispatches and its recorded process span become children of the caller's span, and every task
 * and event it publishes for the process carries the context on as headers, so a worker that
 * extracts them continues the same trace.
 *
 * <p>Deliberately a plain value with no tracing library behind it: the shared module is what every
 * client depends on, and a client with no tracing at all must still be able to read and write it.
 *
 * @param traceparent the W3C {@code traceparent}, {@code 00-<32 hex trace id>-<16 hex span id>-<2 hex flags>}
 * @param tracestate  the W3C {@code tracestate}, vendor-specific; null when there is none
 * @param baggage     the W3C {@code baggage}; null when there is none
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TraceContext(String traceparent, String tracestate, String baggage) {

    /** The W3C header names, which are also the Kafka record header names the engine reads and writes. */
    public static final String TRACEPARENT = "traceparent";
    public static final String TRACESTATE = "tracestate";
    public static final String BAGGAGE = "baggage";

    /** The spec caps {@code tracestate} at 32 members; 512 characters covers that comfortably. */
    public static final int MAX_TRACESTATE_LENGTH = 512;

    /**
     * Baggage is capped by the spec at 8192 bytes per header. The engine keeps less: it stores the
     * value with the process and on every outbox row, and baggage large enough to need more is
     * baggage nobody should be copying onto every task. Longer values are dropped, not truncated —
     * half a baggage list is a wrong one.
     */
    public static final int MAX_BAGGAGE_LENGTH = 2048;

    private static final Pattern TRACEPARENT_FORMAT =
            Pattern.compile("^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");

    private static final String INVALID_TRACE_ID = "00000000000000000000000000000000";
    private static final String INVALID_SPAN_ID = "0000000000000000";

    /**
     * A context with only a {@code traceparent}, or null when that is null or malformed — which is
     * the shape a caller that knows only its current span id needs.
     */
    public static TraceContext of(String traceparent) {
        return of(traceparent, null, null);
    }

    /**
     * The context these three values make, sanitised, or null when {@code traceparent} is missing or
     * not a valid W3C traceparent: without a valid parent there is nothing to join, and the extras
     * alone mean nothing. An oversized {@code tracestate} or {@code baggage} is dropped rather than
     * kept.
     */
    public static TraceContext of(String traceparent, String tracestate, String baggage) {
        if (!isValidTraceparent(traceparent)) {
            return null;
        }
        return new TraceContext(traceparent.trim(), bounded(tracestate, MAX_TRACESTATE_LENGTH),
                bounded(baggage, MAX_BAGGAGE_LENGTH));
    }

    /**
     * Reads the context from message headers, whatever form their values arrive in: a Kafka binder
     * hands unmapped headers over as {@code byte[]}, other transports as {@code String}. Null when
     * there is no valid {@code traceparent} among them.
     *
     * @param header looks a header up by name; returns null when absent
     */
    public static TraceContext fromHeaders(Function<String, Object> header) {
        if (header == null) {
            return null;
        }
        return of(text(header.apply(TRACEPARENT)), text(header.apply(TRACESTATE)), text(header.apply(BAGGAGE)));
    }

    /** {@link #fromHeaders(Function)} over a map, e.g. Spring's {@code MessageHeaders}. */
    public static TraceContext fromHeaders(Map<String, ?> headers) {
        return headers == null ? null : fromHeaders(headers::get);
    }

    /**
     * The context as headers, W3C names, only the values present — the map to put on an outgoing
     * message.
     */
    public Map<String, String> toHeaders() {
        var headers = new LinkedHashMap<String, String>(4);
        headers.put(TRACEPARENT, traceparent);
        if (tracestate != null) {
            headers.put(TRACESTATE, tracestate);
        }
        if (baggage != null) {
            headers.put(BAGGAGE, baggage);
        }
        return headers;
    }

    /** The trace id: the 32 hex characters every span of the trace shares. */
    @JsonIgnore
    public String traceId() {
        return traceparent == null || traceparent.length() < 35 ? null : traceparent.substring(3, 35);
    }

    /** The same trace and extras, under another parent span — what a hop forwards after its own span. */
    public TraceContext withTraceparent(String newTraceparent) {
        return isValidTraceparent(newTraceparent) ? new TraceContext(newTraceparent, tracestate, baggage) : this;
    }

    public static boolean isValidTraceparent(String value) {
        if (value == null) {
            return false;
        }
        var trimmed = value.trim();
        if (!TRACEPARENT_FORMAT.matcher(trimmed).matches()) {
            return false;
        }
        // Version ff is forbidden, and an all-zero trace or span id means "no context".
        return !trimmed.startsWith("ff")
                && !INVALID_TRACE_ID.equals(trimmed.substring(3, 35))
                && !INVALID_SPAN_ID.equals(trimmed.substring(36, 52));
    }

    private static String bounded(String value, int max) {
        if (value == null) {
            return null;
        }
        var trimmed = value.trim();
        return trimmed.isEmpty() || trimmed.length() > max ? null : trimmed;
    }

    private static String text(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        var text = value.toString();
        // A header mapper that JSON-encodes string headers delivers them quoted.
        if (text.length() >= 2 && text.startsWith("\"") && text.endsWith("\"")) {
            return text.substring(1, text.length() - 1);
        }
        return text;
    }
}
