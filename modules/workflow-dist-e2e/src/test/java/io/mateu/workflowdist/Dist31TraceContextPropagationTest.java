package io.mateu.workflowdist;

import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.dtos.events.integration.ProcessCreationRequested;
import io.mateu.workflowdist.support.AbstractDistTest;
import io.mateu.workflowdist.support.DistInfra;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DIST-31 — W3C trace context propagation over real Kafka. A {@code ProcessCreationRequested} whose
 * record carries a caller's {@code traceparent} and {@code baggage} starts a process that keeps
 * that context (a {@code process_trace_context} row), and every task the engine dispatches for it
 * reaches the worker's topic with a {@code traceparent} in the caller's trace — a new span, the same
 * trace id — and the caller's baggage, as raw header bytes a propagator on the other side can read.
 */
class Dist31TraceContextPropagationTest extends AbstractDistTest {

    private static final String CALLER_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String CALLER_SPAN_ID = "b7ad6b7169203331";
    private static final String TRACEPARENT = "00-" + CALLER_TRACE_ID + "-" + CALLER_SPAN_ID + "-01";
    private static final String BAGGAGE = "booking=dist31";

    static ConfigurableApplicationContext orchestrator;

    @BeforeAll
    static void startPods() {
        DistInfra.ensureWorkerStarted();
        orchestrator = DistInfra.startOrchestrator(Map.of(
                "dist.tracing.enabled", "true",
                "management.tracing.sampling.probability", "1.0"));
    }

    @AfterAll
    static void stopPods() {
        orchestrator.close();
    }

    private static KafkaConsumer<byte[], byte[]> consumer(String topic) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DistInfra.kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dist31-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        var consumer = new KafkaConsumer<byte[], byte[]>(props);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private static String header(ConsumerRecord<byte[], byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Test
    void theCallersTraceTravelsWithTheProcessToEveryTask() {
        try (var consumer = consumer("work")) {
            DistInfra.publishUpstream(
                    new ProcessCreationRequested("dist-sequential-3", "dist31-1", List.of(new Variable("tenant", "acme"))),
                    Map.of("traceparent", TRACEPARENT, "baggage", BAGGAGE));

            awaitProcessCompleted("dist31-1");
            var processId = processId("dist31-1");

            assertThat(DistInfra.jdbc().queryForObject(
                    "SELECT trace_parent FROM process_trace_context WHERE process_id = ?", String.class, processId))
                    .isEqualTo(TRACEPARENT);

            var tasks = new ArrayList<ConsumerRecord<byte[], byte[]>>();
            await().atMost(DEFAULT_TIMEOUT).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                    if (new String(record.value(), StandardCharsets.UTF_8).contains(processId)) {
                        tasks.add(record);
                    }
                });
                return tasks.size() >= 3;
            });

            assertThat(tasks).allSatisfy(task -> {
                var traceparent = header(task, "traceparent");
                assertThat(traceparent).as("a raw W3C traceparent, not a JSON-quoted one")
                        .matches("00-" + CALLER_TRACE_ID + "-[0-9a-f]{16}-01");
                assertThat(traceparent).as("the dispatch's own span, a child of the caller's")
                        .doesNotContain(CALLER_SPAN_ID);
                assertThat(header(task, "baggage")).isEqualTo(BAGGAGE);
            });
        }
    }

    @Test
    void aCreationWithoutTraceHeadersJoinsNothing() {
        createProcess("dist-sequential-3", "dist31-2", new Variable("tenant", "acme"));

        awaitProcessCompleted("dist31-2");

        assertThat(DistInfra.jdbc().queryForObject(
                "SELECT count(*) FROM process_trace_context WHERE process_id = ?", Integer.class,
                processId("dist31-2"))).isZero();
    }
}
