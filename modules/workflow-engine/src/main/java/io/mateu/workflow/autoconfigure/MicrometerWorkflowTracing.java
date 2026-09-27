package io.mateu.workflow.autoconfigure;

import io.mateu.workflow.application.out.WorkflowTracing;
import io.mateu.workflow.dtos.TraceContext;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Micrometer-backed {@link WorkflowTracing}, bridged to OpenTelemetry by whatever the host brings.
 *
 * <p>The {@link Tracer} and {@link Propagator} are resolved on first use, not when this object is
 * built. The bean is wired early — the outbox relay and step dispatch depend on it — and the tracer
 * is created by a Spring Boot auto-configuration that a host which component-scans this package (its
 * {@code @SpringBootApplication} sits at {@code io.mateu.workflow}) has not run yet at that point,
 * so asking for it then gets {@code null}, which is exactly how the whole bridge silently fell back
 * to the no-op. By the time the first process starts or the relay turns, the tracer is there; until
 * it is (or if there is none — an app carrying the tracing classes but no configured bridge), every
 * method is a no-op that runs the work untraced.
 *
 * <p>Every method also swallows its own failures. Tracing is a description of the work, and a
 * description that goes wrong must not stop the work: an exporter that is down, a context that
 * cannot be parsed or a span that cannot be started leaves the process running exactly as it would
 * have without any of this.
 */
@Slf4j
public class MicrometerWorkflowTracing implements WorkflowTracing {

    /** The version field of a W3C traceparent; "00" is the only one defined. */
    private static final String W3C_VERSION = "00";

    private static final String TRACEPARENT = "traceparent";

    private final Supplier<Tracer> tracerSupplier;
    private final Supplier<Propagator> propagatorSupplier;
    private volatile Tracer resolvedTracer;
    private volatile Propagator resolvedPropagator;

    /**
     * The {@code tracestate} and {@code baggage} of the context the current work continues, for
     * {@link #currentTraceContext()}. Held here rather than recovered from the span because the
     * outgoing {@code traceparent} is always the current span's, while these two are the caller's,
     * passed through untouched — and Micrometer's span context has no portable place for either.
     */
    private static final ThreadLocal<TraceContext> CONTINUED = new ThreadLocal<>();

    /** Eager: the beans are already in hand (tests, and the case where wiring order happens to work). */
    public MicrometerWorkflowTracing(Tracer tracer, Propagator propagator) {
        this.tracerSupplier = () -> tracer;
        this.propagatorSupplier = () -> propagator;
    }

    /** Lazy: the tracer and propagator are resolved on first use, dodging the bean-creation-order race. */
    public MicrometerWorkflowTracing(ObjectProvider<Tracer> tracerProvider,
                                     ObjectProvider<Propagator> propagatorProvider) {
        this.tracerSupplier = tracerProvider::getIfAvailable;
        this.propagatorSupplier = propagatorProvider::getIfAvailable;
    }

    /** The tracer once it exists, else {@code null}. Resolves each call until one appears, then caches. */
    private Tracer tracer() {
        var t = resolvedTracer;
        if (t == null) {
            t = tracerSupplier.get();
            if (t != null) {
                resolvedTracer = t;
            }
        }
        return t;
    }

    /** The propagator once it exists, else {@code null}. */
    private Propagator propagator() {
        var p = resolvedPropagator;
        if (p == null) {
            p = propagatorSupplier.get();
            if (p != null) {
                resolvedPropagator = p;
            }
        }
        return p;
    }

    @Override
    public boolean enabled() {
        return tracer() != null;
    }

    @Override
    public TraceContext currentTraceContext() {
        var traceParent = currentTraceParent();
        if (traceParent == null) {
            return null;
        }
        var continued = CONTINUED.get();
        return continued == null
                ? TraceContext.of(traceParent)
                : TraceContext.of(traceParent, continued.tracestate(), continued.baggage());
    }

    @Override
    public void continuing(TraceContext context, String spanName, Map<String, String> tags, Runnable work) {
        if (context == null) {
            continuing((String) null, spanName, tags, work);
            return;
        }
        var previous = CONTINUED.get();
        CONTINUED.set(context);
        try {
            continuing(context.traceparent(), spanName, tags, work);
        } finally {
            if (previous == null) {
                CONTINUED.remove();
            } else {
                CONTINUED.set(previous);
            }
        }
    }

    @Override
    public void tagCurrentSpan(String key, String value) {
        var tracer = tracer();
        if (tracer == null || key == null || value == null) {
            return;
        }
        try {
            var span = tracer.currentSpan();
            if (span != null) {
                span.tag(key, value);
            }
        } catch (RuntimeException e) {
            log.debug("Could not tag the current span with '{}'", key, e);
        }
    }

    @Override
    public String currentTraceParent() {
        var tracer = tracer();
        if (tracer == null) {
            return null;
        }
        try {
            var span = tracer.currentSpan();
            if (span == null) {
                return null;
            }
            var context = span.context();
            // Assembled rather than injected through the propagator: the carrier here is a database
            // column, not a set of message headers, and the format is fixed and small enough that
            // building it is clearer than adapting a map.
            return String.join("-", W3C_VERSION, context.traceId(), context.spanId(),
                    Boolean.TRUE.equals(context.sampled()) ? "01" : "00");
        } catch (RuntimeException e) {
            log.debug("Could not read the current trace context", e);
            return null;
        }
    }

    @Override
    public String recordSpan(String parentTraceParent, String name, Instant startedAt, Instant finishedAt,
                             Map<String, String> tags) {
        var tracer = tracer();
        var propagator = propagator();
        if (parentTraceParent == null || parentTraceParent.isBlank()
                || startedAt == null || finishedAt == null || tracer == null || propagator == null) {
            return null;
        }
        try {
            // extract() gives a builder already parented to the remote context, which is how a span
            // built here joins the process's trace without anything having been propagated to us.
            var builder = propagator.extract(Map.of(TRACEPARENT, parentTraceParent), Map::get).name(name);
            tags.forEach((key, value) -> {
                if (value != null) {
                    builder.tag(key, value);
                }
            });
            // The whole point: the span's clock is the work's clock, not this method's. A step that
            // ran for three seconds last Tuesday is a three-second span last Tuesday.
            var span = builder.startTimestamp(micros(startedAt), TimeUnit.MICROSECONDS).start();
            // Read before ending: after end() the span may be recycled by the implementation.
            var context = span.context();
            var traceParent = String.join("-", W3C_VERSION, context.traceId(), context.spanId(),
                    Boolean.TRUE.equals(context.sampled()) ? "01" : "00");
            // Never before it started: a negative duration is rejected outright by some backends and
            // silently rendered as zero by others, and a clock that went backwards between two pods
            // is exactly the kind of thing that would produce one.
            span.end(Math.max(micros(finishedAt), micros(startedAt)), TimeUnit.MICROSECONDS);
            return traceParent;
        } catch (RuntimeException e) {
            log.debug("Could not record the span '{}'", name, e);
            return null;
        }
    }

    private static long micros(Instant instant) {
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    @Override
    public void continuing(String traceParent, String spanName, Map<String, String> tags, Runnable work) {
        var tracer = tracer();
        var propagator = propagator();
        if (traceParent == null || traceParent.isBlank() || tracer == null || propagator == null) {
            work.run();
            return;
        }
        Span span = null;
        try {
            var builder = propagator.extract(Map.of(TRACEPARENT, traceParent), Map::get).name(spanName);
            tags.forEach((key, value) -> {
                if (value != null) {
                    builder.tag(key, value);
                }
            });
            span = builder.start();
        } catch (RuntimeException e) {
            log.debug("Could not resume the trace '{}'", traceParent, e);
        }
        if (span == null) {
            work.run();
            return;
        }
        try (var ignored = tracer.withSpan(span)) {
            work.run();
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public void continuing(String traceParent, String spanName, Runnable work) {
        var tracer = tracer();
        var propagator = propagator();
        if (traceParent == null || traceParent.isBlank() || tracer == null || propagator == null) {
            work.run();
            return;
        }
        Span span = null;
        try {
            span = propagator.extract(Map.of(TRACEPARENT, traceParent), Map::get)
                    .name(spanName)
                    .start();
        } catch (RuntimeException e) {
            log.debug("Could not resume the trace '{}'", traceParent, e);
        }
        if (span == null) {
            work.run();
            return;
        }
        try (var ignored = tracer.withSpan(span)) {
            work.run();
        } finally {
            span.end();
        }
    }

    @Override
    public <T> T span(String name, Map<String, String> tags, Supplier<T> work) {
        var tracer = tracer();
        if (tracer == null) {
            return work.get();
        }
        Span span = null;
        try {
            span = tracer.nextSpan().name(name);
            tags.forEach(span::tag);
            span.start();
        } catch (RuntimeException e) {
            log.debug("Could not start the span '{}'", name, e);
            span = null;
        }
        if (span == null) {
            return work.get();
        }
        try (var ignored = tracer.withSpan(span)) {
            return work.get();
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public <T> T spanLinkedTo(String name, String linkedTraceParent, Map<String, String> tags, Supplier<T> work) {
        var tracer = tracer();
        var linked = contextOf(tracer, linkedTraceParent);
        if (tracer == null || linked == null) {
            return span(name, tags, work);
        }
        Span span = null;
        try {
            var builder = tracer.spanBuilder().name(name).addLink(new io.micrometer.tracing.Link(linked));
            var current = tracer.currentSpan();
            if (current != null) {
                builder = builder.setParent(current.context());
            }
            for (var tag : tags.entrySet()) {
                builder = builder.tag(tag.getKey(), tag.getValue());
            }
            span = builder.start();
        } catch (RuntimeException e) {
            log.debug("Could not start the linked span '{}'", name, e);
            span = null;
        }
        if (span == null) {
            return work.get();
        }
        try (var ignored = tracer.withSpan(span)) {
            return work.get();
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /** A W3C {@code traceparent} ({@code 00-<trace>-<span>-<flags>}) as a context to link to, or null. */
    private static io.micrometer.tracing.TraceContext contextOf(Tracer tracer, String traceParent) {
        if (tracer == null || traceParent == null) {
            return null;
        }
        var parts = traceParent.split("-");
        if (parts.length != 4 || parts[1].length() != 32 || parts[2].length() != 16) {
            return null;
        }
        try {
            return tracer.traceContextBuilder()
                    .traceId(parts[1])
                    .spanId(parts[2])
                    .sampled("01".equals(parts[3]))
                    .build();
        } catch (RuntimeException e) {
            return null;
        }
    }
}
