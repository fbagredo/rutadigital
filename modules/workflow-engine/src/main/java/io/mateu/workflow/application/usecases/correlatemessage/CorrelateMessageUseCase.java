package io.mateu.workflow.application.usecases.correlatemessage;

import io.mateu.workflow.application.out.StepExecutionRepository;
import io.mateu.workflow.application.usecases.correlatemessage.completemessagestep.CompleteMessageStepCommand;
import io.mateu.workflow.application.usecases.correlatemessage.completemessagestep.CompleteMessageStepHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Correlates an incoming {@code MessageReceived} against WAIT_FOR_MESSAGE steps waiting for
 * it. A step matches when it is PENDING on the same messageName and its correlation key (from
 * the step's correlationExpression; businessKey only for pre-rename persisted steps) equals the
 * message's. Both are materialised on the step execution and kept current as the process
 * changes, so this is an indexed lookup rather than a walk over every step waiting anywhere in
 * the engine — with many processes parked on the same message name, that walk was the cost of
 * every single message.
 *
 * <p>Matches are completed by {@link CompleteMessageStepHandler} under the process lock, which
 * re-evaluates the correlation from the live process before acting: this query is the filter,
 * that check is the decision. A message that matches no waiting step is ignored (logged, not
 * buffered): upstream delivery is at-least-once, so the sender simply retries once the process
 * is waiting.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CorrelateMessageUseCase {

    final StepExecutionRepository stepExecutionRepository;
    final CompleteMessageStepHandler completeMessageStepHandler;
    final io.mateu.workflow.application.out.WorkflowTracing workflowTracing;
    final io.mateu.workflow.application.services.ProcessTrace processTrace;
    final io.mateu.workflow.application.services.messagerouting.MessageRoutingMetrics messageRoutingMetrics;
    final io.mateu.workflow.application.services.messagerouting.WaitingMessageFilter waitingMessageFilter;

    public void handle(CorrelateMessageCommand command) {
        // Layer 3: a broadcast reaches every shard, but the per-shard filter can say — without a
        // query — that no step here is waiting for this pair, so the shard drops it. No false
        // negatives: a pair a step here is waiting for is always in the filter (or the filter is
        // disabled/not-ready and says "maybe"), so this only ever saves the query below.
        if (!waitingMessageFilter.mightBeWaitingFor(command.messageName(), command.correlationKey())) {
            messageRoutingMetrics.correlationQuerySkipped();
            return;
        }
        // The query whose S×M cost message routing exists to reduce; counted per shard so the
        // effect is measurable (a routed message reaches only the shard(s) that can match it).
        messageRoutingMetrics.correlationQuery();
        // Correlation is the one piece of engine work that does not know whose process it is until
        // it has done it: a message arrives naming a message and a key, and which step — if any —
        // is waiting for that pair is precisely what the lookup answers. So the lookup itself runs
        // untraced, and each match is traced inside the trace of the process it turned out to
        // belong to. A span around the lookup would be a root of its own with nothing to attach it
        // to, which is what every span in the engine used to be.
        var matched = stepExecutionRepository.findWaitingForMessage(
                command.messageName(), command.correlationKey());
        if (matched.isEmpty()) {
            log.warn("Message '{}' with correlation key '{}' matched no waiting step — ignored (messages are not buffered)",
                    command.messageName(), command.correlationKey());
            return;
        }
        matched.forEach(se -> workflowTracing.continuing(
                processTrace.contextFor(se.getProcessId()),
                "eventconductor.correlate-message",
                java.util.Map.of("eventconductor.process.id", String.valueOf(se.getProcessId()),
                        "eventconductor.message.name", String.valueOf(command.messageName())),
                () -> completeMessageStepHandler.handle(new CompleteMessageStepCommand(
                        se.id(), command.messageName(), command.correlationKey(), command.variables()))));
    }

}
