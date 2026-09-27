package io.mateu.workflow.infra.out.async;

import io.mateu.workflow.dtos.events.integration.ExternalEventRequested;
import io.mateu.workflow.infra.config.EventDestinationsProperties;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;

/**
 * Kafka mode: sends a PUBLISH_EVENT's event to its destination's topic, keyed, synchronously — the
 * same contract as every relayed event (a refused send throws, the outbox row stays Pending).
 *
 * <p>Formats (CloudEvents 1.0 over Kafka):
 * <ul>
 *   <li>{@code binary} (default): the payload is the record value; the attributes are {@code ce_*}
 *       headers — what a CloudEvents-aware Kafka consumer expects;</li>
 *   <li>{@code structured}: the record value is the whole event as JSON
 *       ({@code application/cloudevents+json});</li>
 *   <li>{@code plain}: the payload alone, no envelope.</li>
 * </ul>
 */
final class ExternalEventSender {

    static final String SPEC_VERSION = "1.0";

    private ExternalEventSender() {
    }

    static void send(StreamBridge streamBridge, EventDestinationsProperties destinations, ExternalEventRequested event) {
        send(streamBridge, destinations, event, null);
    }

    /** With the W3C trace headers of {@code trace} on the record; null sends none. */
    static void send(StreamBridge streamBridge, EventDestinationsProperties destinations, ExternalEventRequested event,
                     io.mateu.workflow.dtos.TraceContext trace) {
        var destination = destinations.getDestinations().get(event.destination());
        if (destination == null || destination.getTopic() == null || destination.getTopic().isBlank()) {
            // Validated when the step ran, so only a configuration changed underneath can land here. It
            // cannot succeed until someone fixes it, which is what a poison message is.
            throw new io.mateu.workflow.application.out.PoisonEventException(
                    "PUBLISH_EVENT destination '" + event.destination() + "' has no topic configured"
                            + " (workflow.events.destinations." + event.destination() + ".topic)");
        }
        var message = PartitionedEvents.withTraceHeaders(messageFor(event, formatOf(event, destination)), trace);
        if (!streamBridge.send(destination.getTopic(), message)) {
            throw new PartitionedEvents.EventPublicationRefusedException(destination.getTopic(), event);
        }
    }

    static String formatOf(ExternalEventRequested event, EventDestinationsProperties.Destination destination) {
        if (event.format() != null && !event.format().isBlank()) {
            return event.format();
        }
        return destination.getFormat() == null || destination.getFormat().isBlank() ? "binary" : destination.getFormat();
    }

    static Message<byte[]> messageFor(ExternalEventRequested event, String format) {
        var key = event.partitionKey();
        byte[] value;
        MessageBuilder<byte[]> builder;
        switch (format) {
            case "structured" -> {
                var envelope = new LinkedHashMap<String, Object>(attributes(event));
                envelope.put("datacontenttype", "application/json");
                envelope.put("data", new RawJson(event.data()));
                value = RawJson.write(envelope).getBytes(StandardCharsets.UTF_8);
                builder = MessageBuilder.withPayload(value)
                        .setHeader("contentType", "application/cloudevents+json");
            }
            case "plain" -> {
                value = event.data() == null ? new byte[0] : event.data().getBytes(StandardCharsets.UTF_8);
                builder = MessageBuilder.withPayload(value).setHeader("contentType", "application/json");
            }
            default -> {
                value = event.data() == null ? new byte[0] : event.data().getBytes(StandardCharsets.UTF_8);
                builder = MessageBuilder.withPayload(value).setHeader("contentType", "application/json");
                attributes(event).forEach((name, attribute) -> builder.setHeader("ce_" + name, attribute));
            }
        }
        if (key != null && !key.isBlank()) {
            builder.setHeader(KafkaHeaders.KEY, key.getBytes(StandardCharsets.UTF_8));
        }
        return builder.build();
    }

    /** The CloudEvents context attributes, plus the two extensions that lead back to the process. */
    static LinkedHashMap<String, String> attributes(ExternalEventRequested event) {
        var attributes = new LinkedHashMap<String, String>();
        attributes.put("specversion", SPEC_VERSION);
        attributes.put("id", event.eventId());
        attributes.put("source", "eventconductor/" + event.workflowDefinitionId());
        attributes.put("type", event.eventType());
        if (event.businessKey() != null && !event.businessKey().isBlank()) {
            attributes.put("subject", event.businessKey());
        }
        attributes.put("time", event.time());
        attributes.put("processid", event.processId());
        attributes.put("stepid", event.stepId());
        return attributes;
    }

    /** JSON already rendered, embedded verbatim in the structured envelope. */
    record RawJson(String json) {
        static String write(java.util.Map<String, Object> envelope) {
            var out = new StringBuilder("{");
            var first = true;
            for (var entry : envelope.entrySet()) {
                if (!first) out.append(',');
                first = false;
                out.append(quote(entry.getKey())).append(':');
                var value = entry.getValue();
                if (value instanceof RawJson raw) {
                    out.append(raw.json() == null ? "null" : raw.json());
                } else {
                    out.append(value == null ? "null" : quote(String.valueOf(value)));
                }
            }
            return out.append('}').toString();
        }

        private static String quote(String text) {
            try {
                return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(text);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
