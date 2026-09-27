package io.mateu.workflow.infra.out.async;

import io.mateu.workflow.application.out.IngressPublisher;
import io.mateu.workflow.ddd.DomainEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

/**
 * Sends a new process's creation to the chosen shard's {@code upstream-<shardId>} topic, keyed by the
 * event's partition key (the business key) so the shard's consumer group hands it to one pod.
 */
@Service
@ConditionalOnProperty(name = "workflow.mode", havingValue = "kafka")
@RequiredArgsConstructor
public class KafkaIngressPublisher implements IngressPublisher {

    private final StreamBridge streamBridge;
    private final io.mateu.workflow.application.out.WorkflowTracing workflowTracing;

    @Override
    public void publishToShard(DomainEvent event, String shardId) {
        var binding = (shardId == null || shardId.isBlank()) ? "upstream" : "upstream-" + shardId;
        PartitionedEvents.send(streamBridge, binding, event, workflowTracing.currentTraceContext());
    }
}
