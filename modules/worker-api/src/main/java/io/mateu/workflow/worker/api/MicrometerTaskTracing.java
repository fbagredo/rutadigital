package io.mateu.workflow.worker.api;

import io.mateu.workflow.dtos.TraceContext;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * {@link TaskTracing} over Micrometer Tracing: a {@code CONSUMER} span named after the step, parented
 * to the engine's dispatch through the propagator, current while the handler runs.
 *
 * <p>The tracer and propagator are resolved on first use, not at construction, for the reason the
 * engine's own bridge gives: a Boot tracing auto-configuration may not have created them yet when
 * this is wired, and resolving early would pin the no-op for good.
 */
public final class MicrometerTaskTracing implements TaskTracing {

    private static final Logger log = LoggerFactory.getLogger(MicrometerTaskTracing.class);

    private final Supplier<Tracer> tracerSupplier;
    private final Supplier<Propagator> propagatorSupplier;

    public MicrometerTaskTracing(Supplier<Tracer> tracer, Supplier<Propagator> propagator) {
        this.tracerSupplier = tracer;
        this.propagatorSupplier = propagator;
    }

    @Override
    public void run(TaskExecutionRequested task, TraceContext context, Runnable work) {
        var tracer = tracerSupplier.get();
        var propagator = propagatorSupplier.get();
        if (context == null || tracer == null || propagator == null) {
            work.run();
            return;
        }
        Span span;
        try {
            var builder = propagator.extract(context.toHeaders(), java.util.Map::get)
                    .name("eventconductor.task " + task.stepId())
                    .kind(Span.Kind.CONSUMER);
            tag(builder, "eventconductor.process.id", task.processId());
            tag(builder, "eventconductor.workflow.id", task.workflowDefinitionId());
            tag(builder, "eventconductor.step.id", task.stepId());
            tag(builder, "eventconductor.step.executionId", task.taskExecutionId());
            if (task.taskId() != null && !task.taskId().isBlank()) {
                tag(builder, "eventconductor.task.id", task.taskId());
            }
            span = builder.start();
        } catch (RuntimeException e) {
            log.debug("Could not continue the trace of task {}", task.taskExecutionId(), e);
            work.run();
            return;
        }
        try (var ignored = tracer.withSpan(span)) {
            work.run();
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }

    private static void tag(Span.Builder builder, String key, String value) {
        if (value != null) {
            builder.tag(key, value);
        }
    }
}
