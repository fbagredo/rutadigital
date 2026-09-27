package io.mateu.workflow.infra.out.async;

import io.mateu.workflow.application.out.CommandPublisher;
import io.mateu.workflow.application.out.ProcessIndexRepository;
import io.mateu.workflow.application.readmodel.ProcessIndexRow;
import io.mateu.workflow.ddd.DomainEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

/**
 * Sends a targeted command to the owning shard's {@code upstream-<shardId>} topic. The shard is
 * resolved from the process-index by the command's process id (its {@link DomainEvent#partitionKey()}),
 * and the same key rides the message so the shard's consumer group hands it to the pod that owns the
 * process — the ownership guarantee, extended across shards.
 *
 * <p>When the shard is not known (a just-created process the index has not caught up with, or the read
 * model off) it falls back to this shard's own {@code upstream}: a best-effort that the owner-only
 * command handler still guards — it acts if the process is here and throws (redeliver / dead-letter) if
 * not, rather than silently dropping the command.
 */
@Service
@ConditionalOnProperty(name = "workflow.mode", havingValue = "kafka")
@RequiredArgsConstructor
public class KafkaCommandPublisher implements CommandPublisher {

    private final StreamBridge streamBridge;
    private final io.mateu.workflow.application.out.WorkflowTracing workflowTracing;
    private final ProcessIndexRepository processIndexRepository;

    @Override
    public void publish(DomainEvent command) {
        var processId = command.partitionKey();
        var shardId = processId == null ? null
                : processIndexRepository.findByProcessId(processId).map(ProcessIndexRow::shardId).orElse(null);
        var binding = (shardId == null || shardId.isBlank()) ? "upstream" : "upstream-" + shardId;
        PartitionedEvents.send(streamBridge, binding, command, workflowTracing.currentTraceContext());
    }
}
