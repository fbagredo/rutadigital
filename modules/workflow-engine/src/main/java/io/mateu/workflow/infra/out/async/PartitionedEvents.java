package io.mateu.workflow.infra.out.async;

import io.mateu.workflow.ddd.DomainEvent;
import io.mateu.workflow.dtos.TraceContext;
import org.springframework.messaging.Message;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.StandardCharsets;

/**
 * Publishes an event on the partition that belongs to its process.
 *
 * <p>This is the whole mechanism behind ownership: the event's {@link DomainEvent#partitionKey()}
 * becomes the Kafka message key, so every event of a process hashes to the same partition, and a
 * consumer group hands each partition to exactly one pod. Serialization per process — and
 * ordering, which an unkeyed topic never gave — stop being something the engine arranges with
 * locks and become a property of how the events are addressed.
 *
 * <p>An event with no key is sent unkeyed and lands wherever the partitioner puts it, which is
 * what every event did before. That is the deliberate fallback for events that belong to no
 * process, and for workers that report back without echoing one.
 *
 * <p>The key goes out as bytes rather than a String so it works under the binder's default
 * {@code ByteArraySerializer}, without every application having to configure a key serializer.
 *
 * <h2>Why this throws</h2>
 *
 * <p>{@code StreamBridge.send} reports failure by returning {@code false}, and every caller here
 * used to discard it. That is what turned the transactional outbox into a lossy one: the relay
 * delivers and then marks the row Sent, which is the right order only if "delivered" means the
 * broker took it. Measured during a broker outage on a four-hour run: 71 of 642 912 messages were
 * marked Sent having never reached the topic, and each one is a process that stops forever.
 *
 * <p>So a refused send is an exception now. In the relay it leaves the row Pending for the next
 * pass; in the consumer-side publishers it fails the handler, so the offset is not committed and
 * Kafka redelivers. Both are the at-least-once behaviour the design always claimed.
 *
 * <p>This only works if the producer binding is synchronous — an asynchronous send returns
 * {@code true} as soon as the record is buffered and cannot know whether the broker will ever
 * accept it. The applications set {@code spring.cloud.stream.kafka.default.producer.sync=true}
 * for exactly this reason. Without it, the return value being checked here is not an answer to
 * the question being asked.
 */
final class PartitionedEvents {

    static void send(StreamBridge streamBridge, String binding, DomainEvent event) {
        send(streamBridge, binding, event, null);
    }

    /**
     * {@link #send(StreamBridge, String, DomainEvent)} carrying {@code trace} as the record's W3C
     * {@code traceparent}/{@code tracestate}/{@code baggage} headers, so whoever consumes it — a
     * worker, the engine's own next hop, another shard — continues the same trace. Null sends no
     * trace headers, which is exactly the message this sent before.
     */
    static void send(StreamBridge streamBridge, String binding, DomainEvent event, TraceContext trace) {
        var key = event.partitionKey();
        var keyed = key != null && !key.isBlank();
        boolean accepted;
        if (!keyed && trace == null) {
            accepted = streamBridge.send(binding, event);
        } else {
            var builder = MessageBuilder.withPayload(event);
            if (keyed) {
                builder.setHeader(KafkaHeaders.KEY, key.getBytes(StandardCharsets.UTF_8));
            }
            withTraceHeaders(builder, trace);
            accepted = streamBridge.send(binding, builder.build());
        }
        if (!accepted) {
            throw new EventPublicationRefusedException(binding, event);
        }
    }

    /**
     * Puts {@code trace} on a message as W3C headers. As bytes, deliberately: the Kafka binder's
     * header mapper writes a {@code String} header as JSON — quoted — and a {@code traceparent} in
     * quotes is one no OpenTelemetry or Micrometer propagator on the other side will read. Bytes go
     * onto the record exactly as given.
     */
    static <T> MessageBuilder<T> withTraceHeaders(MessageBuilder<T> builder, TraceContext trace) {
        if (trace != null) {
            trace.toHeaders().forEach((name, value) ->
                    builder.setHeader(name, value.getBytes(StandardCharsets.UTF_8)));
        }
        return builder;
    }

    /** {@link #withTraceHeaders(MessageBuilder, TraceContext)} for an already built message. */
    static <T> Message<T> withTraceHeaders(Message<T> message, TraceContext trace) {
        return trace == null ? message : withTraceHeaders(MessageBuilder.fromMessage(message), trace).build();
    }

    /**
     * Thrown when the broker did not accept an event. Deliberately not a checked exception and
     * deliberately not caught anywhere near here: the callers' existing failure paths — leave the
     * outbox row Pending, do not commit the offset — are already the correct response.
     */
    static final class EventPublicationRefusedException extends RuntimeException {
        EventPublicationRefusedException(String binding, DomainEvent event) {
            super("The broker did not accept " + event.getClass().getSimpleName()
                    + " on binding '" + binding + "'; it will be retried");
        }
    }

    private PartitionedEvents() {
    }
}
