package io.mateu.workflow.infra.out.async;

import io.mateu.workflow.dtos.events.integration.TaskCancellationRequested;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.function.StreamBridge;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Per-step routing: a task goes to the destination its step names, and to {@code downstream} when
 * it names none.
 *
 * <p>Worth asserting because getting it wrong is silent. A task sent to the wrong destination is
 * not refused by anything — the broker takes it, the send succeeds, and the only symptom is a
 * worker that never hears about a step that then sits until its timeout. The same is true of the
 * cancellation, which is why it is asserted here too: it has to land on the topic the task was
 * dispatched to, not on the default.
 */
@ExtendWith(MockitoExtension.class)
class KafkaDownstreamEventPublisherTest {

    @Mock StreamBridge streamBridge;
    @Mock io.mateu.workflow.application.out.WorkflowTracing workflowTracing;

    @InjectMocks KafkaDownstreamEventPublisher publisher;

    private static TaskExecutionRequested aTask() {
        return new TaskExecutionRequested("se-1", "p-1", "wd-1", "step-1", "", List.of());
    }

    @Test
    void sendsToTheTopicTheStepNames() {
        when(streamBridge.send(anyString(), any())).thenReturn(true);

        publisher.publish(aTask(), "order-validator");

        verify(streamBridge).send(eq("order-validator"), any());
    }

    @Test
    void fallsBackToDownstreamWhenTheStepNamesNoTopic() {
        when(streamBridge.send(anyString(), any())).thenReturn(true);

        publisher.publish(aTask(), null);

        verify(streamBridge).send(eq("downstream"), any());
    }

    @Test
    void treatsABlankTopicAsNoTopic() {
        when(streamBridge.send(anyString(), any())).thenReturn(true);

        publisher.publish(aTask(), "   ");

        verify(streamBridge).send(eq("downstream"), any());
    }

    @Test
    void aCancellationFollowsTheTaskToItsOwnTopic() {
        when(streamBridge.send(anyString(), any())).thenReturn(true);

        publisher.publish(new TaskCancellationRequested("se-1"), "order-validator");

        verify(streamBridge).send(eq("order-validator"), any());
    }
}
