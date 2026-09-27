package io.mateu.workflow.infra.out.async;

import io.mateu.workflow.application.services.ProcessTrace;
import io.mateu.workflow.autoconfigure.MicrometerWorkflowTracing;
import io.mateu.workflow.ddd.DomainEvent;
import io.mateu.workflow.dtos.TraceContext;
import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.dtos.events.integration.ProcessCreationRequested;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import io.mateu.workflow.infra.in.async.OrchestratorKafkaConsumerConfig;
import io.mateu.workflow.infra.out.memory.InMemoryProcessTraceContextRepository;
import io.mateu.workflow.infra.out.persistence.OutboxMessageEntity;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A caller's W3C trace context, followed through the engine with a real OpenTelemetry tracer:
 * in on a {@code ProcessCreationRequested} record's headers, kept with the process, and out again on
 * the headers of every record the engine publishes for that process — a task straight from a
 * dispatch, and an event through the outbox — always in the caller's trace, as a child of it, and
 * with its baggage passed on.
 */
class TraceContextPropagationTest {

    private static final String CALLER_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String CALLER_SPAN_ID = "00f067aa0ba902b7";
    private static final String CALLER = "00-" + CALLER_TRACE_ID + "-" + CALLER_SPAN_ID + "-01";
    private static final String BAGGAGE = "booking=MRU01-42";

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final SdkTracerProvider provider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spans)).build();
    private final io.opentelemetry.api.trace.Tracer otelTracer = provider.get("test");
    private final OtelTracer tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), event -> { });
    private final OtelPropagator propagator = new OtelPropagator(ContextPropagators.create(
            TextMapPropagator.composite(W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())),
            otelTracer);
    private final MicrometerWorkflowTracing tracing = new MicrometerWorkflowTracing(tracer, propagator);
    private final InMemoryProcessTraceContextRepository store = new InMemoryProcessTraceContextRepository();
    private final ProcessTrace processTrace = new ProcessTrace(1.0, tracing, store, 100);

    private final StreamBridge bridge = mock(StreamBridge.class);
    private final List<Message<?>> sent = new ArrayList<>();

    {
        when(bridge.send(anyString(), any(Object.class))).thenAnswer(invocation -> {
            sent.add(invocation.getArgument(1));
            return true;
        });
    }

    @AfterEach
    void close() {
        provider.close();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String header(Message<?> message, String name) {
        var value = message.getHeaders().get(name);
        return value == null ? null : new String((byte[]) value, StandardCharsets.UTF_8);
    }

    private static Message<List<DomainEvent>> recordBatch(DomainEvent event, Map<String, Object> recordHeaders) {
        return MessageBuilder.<List<DomainEvent>>withPayload(List.of(event))
                .setHeader(KafkaHeaders.BATCH_CONVERTED_HEADERS, List.of(recordHeaders))
                .build();
    }

    private TraceContext incomingContext() {
        var creation = new ProcessCreationRequested("reservation", "MRU01-42", List.of());
        var events = OrchestratorKafkaConsumerConfig.withRecordTraceContexts(recordBatch(creation,
                Map.of("traceparent", bytes(CALLER), "baggage", bytes(BAGGAGE))));
        return ((ProcessCreationRequested) events.get(0)).traceContext();
    }

    @Test
    void aCreationRecordsTraceHeadersBecomeItsTraceContext() {
        var context = incomingContext();

        assertThat(context.traceparent()).isEqualTo(CALLER);
        assertThat(context.baggage()).isEqualTo(BAGGAGE);
    }

    @Test
    void rawKafkaHeadersAreReadWhenTheBinderMappedNone() {
        var creation = new ProcessCreationRequested("reservation", "k", List.of());
        var raw = new org.apache.kafka.common.header.internals.RecordHeaders();
        raw.add("traceparent", bytes(CALLER));
        var batch = MessageBuilder.<List<DomainEvent>>withPayload(List.of(creation))
                .setHeader(KafkaHeaders.NATIVE_HEADERS, List.of(raw))
                .build();

        var events = OrchestratorKafkaConsumerConfig.withRecordTraceContexts(batch);

        assertThat(((ProcessCreationRequested) events.get(0)).traceContext().traceparent()).isEqualTo(CALLER);
    }

    @Test
    void aTraceContextInThePayloadWinsOverTheRecordHeaders() {
        var own = TraceContext.of("00-11111111111111111111111111111111-2222222222222222-01");
        var creation = new ProcessCreationRequested("reservation", "k", List.of(), null, null, own);

        var events = OrchestratorKafkaConsumerConfig.withRecordTraceContexts(recordBatch(creation,
                Map.of("traceparent", bytes(CALLER))));

        assertThat(((ProcessCreationRequested) events.get(0)).traceContext()).isEqualTo(own);
    }

    @Test
    void aRecordWithoutTraceHeadersIsLeftExactlyAsItCame() {
        var creation = new ProcessCreationRequested("reservation", "k", List.of());

        var events = OrchestratorKafkaConsumerConfig.withRecordTraceContexts(recordBatch(creation,
                Map.of("traceparent", bytes("not-a-traceparent"))));

        assertThat(events.get(0)).isSameAs(creation);
    }

    @Test
    void aProcessThatJoinedTheCallersTraceIsAnchoredToTheCallersSpan() {
        processTrace.join("p-1", incomingContext());

        assertThat(processTrace.anchorFor("p-1")).isEqualTo(CALLER);
        assertThat(processTrace.contextFor("p-1").baggage()).isEqualTo(BAGGAGE);
        // One that joined nothing keeps the anchor derived from its id.
        assertThat(processTrace.anchorFor("p-2")).isEqualTo(processTrace.derivedAnchorFor("p-2"));
    }

    @Test
    void aTaskDispatchedForTheProcessCarriesTheCallersTraceOnItsHeaders() {
        processTrace.join("p-1", incomingContext());
        var task = new TaskExecutionRequested("se-1", "p-1", "reservation", "write-to-opera", "",
                List.of(new Variable("hotel", "MRU01")));

        tracing.continuing(processTrace.contextFor("p-1"), "eventconductor.dispatch-step",
                Map.of("eventconductor.step.id", "write-to-opera"),
                () -> PartitionedEvents.send(bridge, "downstream", task, tracing.currentTraceContext()));

        var record = sent.get(0);
        var traceparent = header(record, "traceparent");
        assertThat(TraceContext.of(traceparent).traceId()).isEqualTo(CALLER_TRACE_ID);
        // The dispatch span's own id, not the caller's: the worker is a child of the dispatch.
        assertThat(traceparent).doesNotContain(CALLER_SPAN_ID);
        assertThat(header(record, "baggage")).isEqualTo(BAGGAGE);
        // The partition key is still there beside them.
        assertThat(record.getHeaders().get(KafkaHeaders.KEY)).isEqualTo(bytes("p-1"));

        var dispatch = spans.getFinishedSpanItems().get(0);
        assertThat(dispatch.getName()).isEqualTo("eventconductor.dispatch-step");
        assertThat(dispatch.getTraceId()).isEqualTo(CALLER_TRACE_ID);
        assertThat(dispatch.getParentSpanId()).isEqualTo(CALLER_SPAN_ID);
        assertThat(traceparent).contains(dispatch.getSpanId());
    }

    @Test
    void anEventThroughTheOutboxIsPublishedInTheCallersTraceWithItsBaggage() {
        processTrace.join("p-1", incomingContext());
        var event = new TaskExecutionRequested("se-1", "p-1", "reservation", "write-to-opera", "", List.of());

        // Written in one step-over, on one thread…
        var row = new OutboxMessageEntity[1];
        tracing.continuing(processTrace.contextFor("p-1"), "eventconductor.step-over", Map.of(),
                () -> row[0] = new OutboxMessageEntity(event, tracing.currentTraceContext()));
        assertThat(row[0].getBaggage()).isEqualTo(BAGGAGE);

        // …and relayed later, from nothing but the row.
        tracing.continuing(row[0].traceContext(), "outbox relay", Map.of(),
                () -> PartitionedEvents.send(bridge, "downstream", event, tracing.currentTraceContext()));

        var record = sent.get(0);
        assertThat(TraceContext.of(header(record, "traceparent")).traceId()).isEqualTo(CALLER_TRACE_ID);
        assertThat(header(record, "baggage")).isEqualTo(BAGGAGE);
    }

    @Test
    void withNothingTracedNoTraceHeadersAreAdded() {
        var event = new TaskExecutionRequested("se-1", "p-1", "reservation", "s", "", List.of());

        PartitionedEvents.send(bridge, "downstream", event, tracing.currentTraceContext());

        assertThat(sent.get(0).getHeaders()).doesNotContainKey("traceparent");
    }

    @Test
    void withTracingOffAProcessJoinsNothingAndNothingIsLookedUp() {
        var off = new ProcessTrace(1.0, io.mateu.workflow.application.out.WorkflowTracing.NOOP, store, 100);

        off.join("p-1", TraceContext.of(CALLER));

        assertThat(store.findByProcessId("p-1")).isEmpty();
        assertThat(off.anchorFor("p-1")).isEqualTo(off.derivedAnchorFor("p-1"));
    }
}
