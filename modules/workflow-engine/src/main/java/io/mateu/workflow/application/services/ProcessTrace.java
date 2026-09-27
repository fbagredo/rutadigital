package io.mateu.workflow.application.services;

import io.mateu.workflow.application.out.ProcessTraceContextRepository;
import io.mateu.workflow.application.out.WorkflowTracing;
import io.mateu.workflow.dtos.TraceContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The trace a process belongs to, derived from its id.
 *
 * <p>Every process has one trace, and everything the engine ever does for that process — the step
 * spans emitted when it finishes, and the live spans for the work in between — belongs to it. What
 * makes that possible without a column, a lookup or a header is that the anchor is <em>computed</em>
 * from the process id: any pod, at any time, on any hop, arrives at the same trace id for the same
 * process, with nothing to store and nothing to propagate.
 *
 * <p>Deriving it rather than carrying it is what makes this survive the things that break ordinary
 * context propagation. A workflow's events cross the outbox, a broker, a consumer group rebalance,
 * a pod restart and — for a durable wait — days of nothing happening at all. Any of those loses an
 * in-memory context, and a {@code traceparent} carried in a message header is lost the moment the
 * message is redelivered from a dead-letter queue or replayed by an operator. A derived anchor
 * cannot be lost, because it was never held.
 *
 * <p>The anchor itself is a phantom: no span is ever emitted with its id. It exists to give the
 * process's own span a parent to inherit a trace id from, and a backend renders a span whose parent
 * is absent as the root of its trace, which is exactly what the process span is.
 *
 * <h2>Joining the caller's trace</h2>
 *
 * <p>A process started from inside somebody else's trace — a booking a CRS sent, whose trace
 * already holds the CRS, the connector and the broker hop — should not start a trace of its own:
 * the point of tracing it is to read the booking end to end. So a process created with a W3C
 * context (Kafka record headers or the {@code traceContext} of {@code ProcessCreationRequested})
 * keeps that context, and its anchor is the caller's span instead of the phantom: the same
 * everything-descends-from-one-parent shape, one level further up. Unlike the derived anchor it is
 * stored — {@link ProcessTraceContextRepository}, one row per such process — which is what keeps it
 * as durable as the derived one: any pod, any time, reads the same row. Looked up once per process
 * per pod and cached, and only while something is being traced; a process started with no context
 * keeps the derived anchor.
 */
@Service
@Slf4j
public class ProcessTrace {

    /** The version field of a W3C traceparent; "00" is the only one defined. */
    private static final String W3C_VERSION = "00";

    /**
     * The trace-id value below which a process is traced, from the configured sampling probability.
     *
     * <p>Every span of a process descends from this anchor, and Spring Boot's default sampler is
     * {@code ParentBased(TraceIdRatioBased(p))} — which honours a remote parent's decision and only
     * consults the ratio when there is no parent. An anchor that always claimed to be sampled
     * therefore exported <em>every</em> process trace whatever {@code
     * management.tracing.sampling.probability} said: the property still governed the
     * auto-instrumented HTTP and JDBC traces, and quietly did not govern these.
     *
     * <p>So the decision is made here instead, by the same arithmetic the SDK's own sampler uses on
     * the same trace id — the low 8 bytes against {@code ratio × Long.MAX_VALUE}. The rate comes out
     * as configured, because the trace id is a hash and its low bytes are uniform.
     *
     * <p>Deciding it from the trace id rather than per span is what makes it <b>all or nothing per
     * process</b>, which matters more here than the rate does. The alternative — each span rolling
     * its own dice — would at 10% have given a tenth of the dispatches of a tenth of the processes:
     * scattered fragments, and never a whole process to read. This way a traced process is traced
     * end to end, on every pod that touches it, for its whole life.
     */
    private final long idUpperBound;

    private final WorkflowTracing workflowTracing;
    private final ProcessTraceContextRepository contexts;

    /**
     * Per pod, which processes joined a caller's trace and which did not — the negative answer too,
     * since that is nearly every process and asking the database on every step-over would be the
     * cost this is meant not to have. Bounded LRU: a process out of it is simply asked for again.
     */
    private final Map<String, TraceContext> cache;

    /** Marks "looked up, has none" in {@link #cache}. */
    private static final TraceContext NONE = new TraceContext("none", null, null);

    /** Derived anchors only, no store: what tests and a host with no persistence of its own use. */
    public ProcessTrace(double samplingProbability) {
        this(samplingProbability, WorkflowTracing.NOOP, null, 0);
    }

    @Autowired
    public ProcessTrace(
            @Value("${management.tracing.sampling.probability:0.1}") double samplingProbability,
            ObjectProvider<WorkflowTracing> workflowTracing,
            ObjectProvider<ProcessTraceContextRepository> contexts,
            @Value("${workflow.tracing.process-context-cache-size:10000}") int cacheSize) {
        this(samplingProbability, workflowTracing.getIfAvailable(() -> WorkflowTracing.NOOP),
                contexts.getIfAvailable(), cacheSize);
    }

    public ProcessTrace(double samplingProbability, WorkflowTracing workflowTracing,
                        ProcessTraceContextRepository contexts, int cacheSize) {
        this.idUpperBound = idUpperBoundFor(samplingProbability);
        this.workflowTracing = workflowTracing == null ? WorkflowTracing.NOOP : workflowTracing;
        this.contexts = contexts;
        var bound = Math.max(cacheSize, 0);
        this.cache = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, TraceContext> eldest) {
                return size() > bound;
            }
        });
    }

    /**
     * The whole context every span of this process descends from: the caller's, when the process
     * joined one at creation, else the derived anchor with no extras. Null when there is no process.
     */
    public TraceContext contextFor(String processId) {
        if (processId == null || processId.isBlank()) {
            return null;
        }
        var joined = joinedContext(processId);
        return joined != null ? joined : TraceContext.of(derivedAnchorFor(processId));
    }

    /**
     * Records that {@code processId} joined {@code context}'s trace — persisted, and cached on this
     * pod so the step-over its creation triggers does not have to read it back. A null or invalid
     * context, or tracing being off, records nothing.
     */
    public void join(String processId, TraceContext context) {
        if (processId == null || context == null || contexts == null || !workflowTracing.enabled()) {
            return;
        }
        contexts.save(processId, context);
        cache.put(processId, context);
    }

    /** Forgets deleted processes, so their ids cannot resurface with a stale context. */
    public void forget(java.util.Collection<String> processIds) {
        if (processIds == null) {
            return;
        }
        processIds.forEach(cache::remove);
        if (contexts != null) {
            contexts.deleteAllByProcessId(processIds);
        }
    }

    private TraceContext joinedContext(String processId) {
        // Zero-cost when off: no store, or nothing being traced, means no lookup at all.
        if (contexts == null || !workflowTracing.enabled()) {
            return null;
        }
        var cached = cache.get(processId);
        if (cached == null) {
            try {
                cached = contexts.findByProcessId(processId).orElse(NONE);
            } catch (RuntimeException e) {
                // A tracing lookup must never be a workflow failure: fall back to the derived anchor.
                log.debug("Could not read the trace context of process {}", processId, e);
                return null;
            }
            cache.put(processId, cached);
        }
        return cached == NONE ? null : cached;
    }

    /**
     * {@code io.opentelemetry.sdk.trace.samplers.TraceIdRatioBasedSampler}'s bound, reproduced
     * rather than called: the SDK is not a dependency of this module, and the engine's own tracing
     * port is deliberately free of OpenTelemetry types.
     */
    static long idUpperBoundFor(double ratio) {
        if (ratio <= 0.0) {
            return Long.MIN_VALUE;
        }
        if (ratio >= 1.0) {
            return Long.MAX_VALUE;
        }
        return (long) (ratio * Long.MAX_VALUE);
    }

    /**
     * The parent every span of this process descends from, as a W3C {@code traceparent}: the
     * caller's span when the process joined a caller's trace (see the class comment), otherwise the
     * phantom derived by {@link #derivedAnchorFor}. {@code null} when there is no process.
     */
    public String anchorFor(String processId) {
        var context = contextFor(processId);
        return context == null ? null : context.traceparent();
    }

    /**
     * The phantom anchor derived from the process id alone, whatever the process joined.
     *
     * <p>SHA-256 of the process id, split into the 16-byte trace id and the 8-byte span id. A hash
     * rather than the raw id because a trace id is exactly 16 bytes and a process id is a UUID
     * string, and because it spreads ids that share a prefix. The sampled flag is the ratio
     * decision described on {@link #idUpperBound}.
     */
    public String derivedAnchorFor(String processId) {
        if (processId == null || processId.isBlank()) {
            return null;
        }
        var digest = sha256(processId);
        var traceId = hex(digest, 0, 16);
        return String.join("-", W3C_VERSION, traceId, hex(digest, 16, 8), sampled(traceId) ? "01" : "00");
    }

    /** Whether this trace is one of the sampled ones. Stable for a trace id, so stable for a process. */
    public boolean sampled(String traceId) {
        if (idUpperBound == Long.MAX_VALUE) {
            return true;
        }
        if (idUpperBound == Long.MIN_VALUE) {
            return false;
        }
        // The low 8 bytes, which is the half the SDK treats as random. Math.abs so the comparison
        // is against a magnitude — Long.MIN_VALUE stays negative under abs, and is left to fall
        // through as not sampled, exactly as the SDK leaves it.
        long randomPart = Math.abs(Long.parseUnsignedLong(traceId.substring(16), 16));
        return randomPart >= 0 && randomPart < idUpperBound;
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required of every JVM; this cannot happen and is not worth a checked path.
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static String hex(byte[] bytes, int from, int length) {
        var out = new StringBuilder(length * 2);
        for (int i = from; i < from + length; i++) {
            out.append(Character.forDigit((bytes[i] >> 4) & 0xf, 16));
            out.append(Character.forDigit(bytes[i] & 0xf, 16));
        }
        return out.toString();
    }
}
