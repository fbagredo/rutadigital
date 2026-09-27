package io.mateu.workflow.worker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import io.mateu.workflow.worker.api.TaskHandler;
import io.mateu.workflow.worker.api.TaskRegistration;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Against a real binder and broker: the task's offset is committed only after its reply is out.
 *
 * <p>The handler is held mid-task. While it is held, nothing may be committed for the task's
 * record — a crash at that point must redeliver the task. The reactive consumer this replaced
 * committed the record as soon as it was handed to its {@code Flux}, before the handler ran.
 */
class WorkerKafkaCommitTest {

    static final String GROUP = "worker-commit-it";
    static final CountDownLatch entered = new CountDownLatch(1);
    static final CountDownLatch release = new CountDownLatch(1);

    static EmbeddedKafkaKraftBroker broker;
    static ConfigurableApplicationContext app;

    record In(String locator) {}

    record Out(String echoed) {}

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class WorkerApp {
        @Bean
        TaskRegistration<In, Out> hold() {
            TaskHandler<In, Out> handler = (in, ctx) -> {
                entered.countDown();
                if (!release.await(60, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("never released");
                }
                return new Out(in.locator());
            };
            return new TaskRegistration<>("hold", 1, "downstream", In.class, Out.class, handler);
        }
    }

    @BeforeAll
    static void start() {
        broker = new EmbeddedKafkaKraftBroker(1, 1, "downstream", "upstream");
        broker.afterPropertiesSet();
        app = new SpringApplicationBuilder(WorkerApp.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "spring.application.name=" + GROUP,
                        "spring.cloud.stream.kafka.binder.brokers=" + broker.getBrokersAsString(),
                        "spring.cloud.stream.kafka.binder.configuration.auto.offset.reset=earliest",
                        "spring.cloud.stream.bindings.consumeWorkerEvent-in-0.group=" + GROUP)
                .run();
    }

    @AfterAll
    static void stop() {
        release.countDown();
        if (app != null) {
            app.close();
        }
        if (broker != null) {
            broker.destroy();
        }
    }

    @Test
    void the_offset_is_committed_only_after_the_reply_is_published() throws Exception {
        var task = new TaskExecutionRequested("tx-it", "p-it", "wf", "step", "hold@1",
                List.of(new Variable("locator", "12E45")));
        assertThat(app.getBean(StreamBridge.class).send("downstream", task)).isTrue();

        assertThat(entered.await(60, TimeUnit.SECONDS)).as("the handler started").isTrue();

        // Held mid-task: give the container ample time to commit, and check it did not.
        Thread.sleep(3_000);
        assertThat(committed()).as("committed while the handler is still running").isZero();

        release.countDown();

        var reply = awaitReply("tx-it");
        assertThat(reply).contains("\"COMPLETED\"").contains("12E45");
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (committed() < 1 && System.nanoTime() < deadline) {
            Thread.sleep(200);
        }
        assertThat(committed()).as("committed once the reply is out").isEqualTo(1);
    }

    private long committed() throws Exception {
        try (var admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(GROUP)
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            return offsets.entrySet().stream()
                    .filter(e -> e.getKey().topic().equals("downstream") && e.getValue() != null)
                    .mapToLong(e -> e.getValue().offset())
                    .sum();
        }
    }

    private String awaitReply(String taskExecutionId) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reply-reader");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        var seen = new ArrayList<String>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of("upstream"));
            var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    seen.add(record.value());
                    if (record.value().contains(taskExecutionId) && record.value().contains("COMPLETED")) {
                        return record.value();
                    }
                }
            }
        }
        throw new AssertionError("no COMPLETED reply for " + taskExecutionId + "; saw " + seen);
    }
}
