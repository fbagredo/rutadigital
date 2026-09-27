package io.mateu.workflow.infra.out.async;

import io.mateu.workflow.application.out.ProcessIndexRepository;
import io.mateu.workflow.application.readmodel.ProcessIndexRow;
import io.mateu.workflow.dtos.events.integration.RetryProcessRequested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.function.StreamBridge;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The targeted command routing: resolve the owning shard from the process-index and send to its
 * {@code upstream-<shardId>}; fall back to this shard's {@code upstream} when the shard is unknown.
 */
@ExtendWith(MockitoExtension.class)
class KafkaCommandPublisherTest {

    @Mock StreamBridge streamBridge;
    @Mock io.mateu.workflow.application.out.WorkflowTracing workflowTracing;
    @Mock ProcessIndexRepository processIndexRepository;

    @InjectMocks KafkaCommandPublisher publisher;

    private ProcessIndexRow rowOnShard(String shardId) {
        return new ProcessIndexRow("p-1", "bk-1", "a process", "wd-1", 1, "ERROR", 40,
                LocalDateTime.now(), LocalDateTime.now(), null, LocalDateTime.now(), shardId);
    }

    @Test
    void routesToTheOwningShardUpstream() {
        when(streamBridge.send(anyString(), any())).thenReturn(true);
        when(processIndexRepository.findByProcessId("p-1")).thenReturn(Optional.of(rowOnShard("shard-A")));

        publisher.publish(new RetryProcessRequested("p-1"));

        verify(streamBridge).send(eq("upstream-shard-A"), any());
    }

    @Test
    void fallsBackToLocalUpstreamWhenTheShardIsUnknown() {
        when(streamBridge.send(anyString(), any())).thenReturn(true);
        when(processIndexRepository.findByProcessId("p-1")).thenReturn(Optional.empty());

        publisher.publish(new RetryProcessRequested("p-1"));

        verify(streamBridge).send(eq("upstream"), any());
    }
}
