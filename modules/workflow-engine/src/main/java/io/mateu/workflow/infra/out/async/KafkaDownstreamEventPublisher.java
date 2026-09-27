package io.mateu.workflow.infra.out.async;

import io.mateu.workflow.application.out.DownstreamEventPublisher;
import io.mateu.workflow.ddd.DomainEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

/**
 * Sends a task to the destination its step names.
 *
 * <p>A step's {@code topic} is the binding this sends on, and a step that names none goes to
 * {@code downstream} — the single destination everything used to go to, and the one the engine
 * contributes a binding for. A topic that has no binding of its own is a dynamic destination:
 * Spring Cloud Stream creates it on first use, so a definition can name a worker pool without the
 * application declaring anything, and it inherits the producer defaults (including
 * {@code sync=true}, without which the accepted/refused answer {@link PartitionedEvents} checks
 * would be meaningless).
 */
@Service
@ConditionalOnProperty(name = "workflow.mode", havingValue = "kafka")
@RequiredArgsConstructor
public class KafkaDownstreamEventPublisher implements DownstreamEventPublisher {

    private final StreamBridge streamBridge;
    private final io.mateu.workflow.application.out.WorkflowTracing workflowTracing;

    @Override
    public void publish(DomainEvent event, String topic) {
        PartitionedEvents.send(streamBridge, DownstreamEventPublisher.destinationFor(topic), event,
                workflowTracing.currentTraceContext());
    }
}
