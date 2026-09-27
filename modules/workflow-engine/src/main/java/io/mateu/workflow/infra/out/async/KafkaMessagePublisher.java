package io.mateu.workflow.infra.out.async;

import io.mateu.workflow.application.out.MessagePublisher;
import io.mateu.workflow.dtos.events.integration.MessageReceived;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

/**
 * Sends a {@link MessageReceived} to the shared {@code messages} topic. Unkeyed on purpose: every
 * shard consumes all partitions of this topic (each shard is its own consumer group), so any
 * partition reaches every shard, and the owning shard is the one that correlates it.
 */
@Service
@ConditionalOnProperty(name = "workflow.mode", havingValue = "kafka")
@RequiredArgsConstructor
public class KafkaMessagePublisher implements MessagePublisher {

    private final StreamBridge streamBridge;
    private final io.mateu.workflow.application.out.WorkflowTracing workflowTracing;

    @Override
    public void publish(MessageReceived message) {
        PartitionedEvents.send(streamBridge, "messages", message, workflowTracing.currentTraceContext());
    }
}
