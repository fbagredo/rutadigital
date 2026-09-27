package io.mateu.workflow.dtos;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mateu.workflow.ddd.DomainEvent;
import io.mateu.workflow.dtos.events.integration.ProcessCreationRequested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TraceContextTest {

    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void readsTheW3cHeadersWhateverFormTheyArriveIn() {
        var context = TraceContext.fromHeaders(Map.of(
                "traceparent", TRACEPARENT.getBytes(StandardCharsets.UTF_8),
                "tracestate", "vendor=abc",
                // A header mapper that JSON-encodes strings delivers them quoted.
                "baggage", "\"booking=MRU01-42\""));

        assertThat(context).isEqualTo(new TraceContext(TRACEPARENT, "vendor=abc", "booking=MRU01-42"));
        assertThat(context.traceId()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
        assertThat(context.toHeaders()).containsExactly(
                Map.entry("traceparent", TRACEPARENT),
                Map.entry("tracestate", "vendor=abc"),
                Map.entry("baggage", "booking=MRU01-42"));
    }

    @Test
    void anythingButAValidTraceparentMeansNoContext() {
        assertThat(TraceContext.of(null)).isNull();
        assertThat(TraceContext.of("garbage")).isNull();
        assertThat(TraceContext.of("00-00000000000000000000000000000000-00f067aa0ba902b7-01")).isNull();
        assertThat(TraceContext.of("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")).isNull();
        assertThat(TraceContext.of("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")).isNull();
        assertThat(TraceContext.fromHeaders(Map.of("baggage", "a=b"))).isNull();
    }

    @Test
    void oversizedExtrasAreDroppedNotTruncated() {
        var context = TraceContext.of(TRACEPARENT, "x".repeat(TraceContext.MAX_TRACESTATE_LENGTH + 1),
                "k=" + "v".repeat(TraceContext.MAX_BAGGAGE_LENGTH));

        assertThat(context.traceparent()).isEqualTo(TRACEPARENT);
        assertThat(context.tracestate()).isNull();
        assertThat(context.baggage()).isNull();
    }

    @Test
    void aCreationWithoutAContextSerialisesExactlyAsBefore() throws Exception {
        var json = mapper.writerFor(DomainEvent.class)
                .writeValueAsString(new ProcessCreationRequested("wf", "bk", List.of()));

        assertThat(json).doesNotContain("traceContext");
    }

    @Test
    void aCreationCarriesItsContextAndOneFromBeforeTheFieldStillReads() throws Exception {
        var creation = new ProcessCreationRequested("wf", "bk", List.of(), null, null,
                TraceContext.of(TRACEPARENT, null, "booking=MRU01-42"));
        var json = mapper.writerFor(DomainEvent.class).writeValueAsString(creation);

        assertThat(json).contains("\"traceContext\":{\"traceparent\":\"" + TRACEPARENT + "\",\"baggage\":\"booking=MRU01-42\"}");
        assertThat(mapper.readValue(json, DomainEvent.class)).isEqualTo(creation);

        var old = "{\"type\":\"process-creation-requested\",\"workflowDefinitionId\":\"wf\",\"businessKey\":\"bk\","
                + "\"variables\":[],\"parentStepExecutionId\":null,\"caller\":null}";
        assertThat(((ProcessCreationRequested) mapper.readValue(old, DomainEvent.class)).traceContext()).isNull();
    }
}
