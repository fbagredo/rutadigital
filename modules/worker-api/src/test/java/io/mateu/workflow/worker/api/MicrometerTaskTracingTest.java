package io.mateu.workflow.worker.api;

import io.mateu.workflow.dtos.TraceContext;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A task runs in a span that is a child of the engine's dispatch, with a real OpenTelemetry tracer. */
class MicrometerTaskTracingTest {

    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String DISPATCH_SPAN = "00f067aa0ba902b7";
    private static final TraceContext DISPATCH = TraceContext.of("00-" + TRACE_ID + "-" + DISPATCH_SPAN + "-01");

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final SdkTracerProvider provider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spans)).build();
    private final io.opentelemetry.api.trace.Tracer otel = provider.get("test");
    private final OtelTracer tracer = new OtelTracer(otel, new OtelCurrentTraceContext(), event -> { });
    private final OtelPropagator propagator = new OtelPropagator(
            ContextPropagators.create(W3CTraceContextPropagator.getInstance()), otel);
    private final MicrometerTaskTracing tracing = new MicrometerTaskTracing(() -> tracer, () -> propagator);

    private static final TaskExecutionRequested TASK =
            new TaskExecutionRequested("se-1", "p-1", "wf", "write-to-opera", "opera@1", List.of());

    @AfterEach
    void close() {
        provider.close();
    }

    @Test
    void theHandlerRunsInAConsumerSpanUnderTheDispatch() {
        var current = new AtomicReference<String>();

        tracing.run(TASK, DISPATCH, () -> current.set(tracer.currentSpan().context().traceId()));

        assertThat(current).hasValue(TRACE_ID);
        var span = spans.getFinishedSpanItems().get(0);
        assertThat(span.getName()).isEqualTo("eventconductor.task write-to-opera");
        assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(span.getTraceId()).isEqualTo(TRACE_ID);
        assertThat(span.getParentSpanId()).isEqualTo(DISPATCH_SPAN);
        assertThat(span.getAttributes().asMap().toString())
                .contains("eventconductor.process.id=p-1", "eventconductor.task.id=opera@1");
    }

    @Test
    void aFailingHandlerMarksTheSpanAndStillThrows() {
        assertThatThrownBy(() -> tracing.run(TASK, DISPATCH, () -> {
            throw new IllegalStateException("Opera down");
        })).hasMessage("Opera down");

        assertThat(spans.getFinishedSpanItems()).hasSize(1);
        assertThat(spans.getFinishedSpanItems().get(0).getStatus().getStatusCode())
                .isEqualTo(io.opentelemetry.api.trace.StatusCode.ERROR);
    }

    @Test
    void withNoContextOrNoTracerTheWorkRunsUntraced() {
        var ran = new AtomicReference<Boolean>(false);
        tracing.run(TASK, null, () -> ran.set(true));
        new MicrometerTaskTracing(() -> null, () -> null).run(TASK, DISPATCH, () -> ran.set(true));

        assertThat(ran).hasValue(true);
        assertThat(spans.getFinishedSpanItems()).isEmpty();
    }

    @Test
    void theDispatcherHandsTheContextToTheHandler() {
        var seen = new AtomicReference<TraceContext>();
        TaskHandler<Object, Object> handler = (in, ctx) -> {
            seen.set(ctx.traceContext());
            return null;
        };
        var registry = new TaskRegistry(List.of(new TaskRegistration<>("opera", 1, "t", Object.class, Object.class, handler)));
        var dispatcher = new TaskDispatcher(registry, new TaskReplySink() {
            public void running(TaskExecutionRequested task) {
            }

            public void completed(TaskExecutionRequested task, List<io.mateu.workflow.dtos.Variable> variables) {
            }

            public void failed(TaskExecutionRequested task, List<io.mateu.workflow.dtos.Variable> variables, String reason) {
            }
        }, null, new com.fasterxml.jackson.databind.ObjectMapper(), false, tracing);

        dispatcher.dispatch(TASK, DISPATCH);

        assertThat(seen).hasValue(DISPATCH);
        assertThat(spans.getFinishedSpanItems()).hasSize(1);
    }

    @Test
    void theAutoConfigurationBuildsTheBridgeLazily() {
        var factory = new org.springframework.beans.factory.support.DefaultListableBeanFactory();
        var built = new WorkerTracingAutoConfiguration().workerTaskTracing(
                factory.getBeanProvider(io.micrometer.tracing.Tracer.class),
                factory.getBeanProvider(io.micrometer.tracing.propagation.Propagator.class));
        var ran = new AtomicReference<Boolean>(false);

        built.run(TASK, DISPATCH, () -> ran.set(true));

        assertThat(ran).hasValue(true);
    }
}
