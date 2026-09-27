package io.mateu.workflow.application.usecases.stepexecution.start;

import io.mateu.workflow.application.out.DownstreamEventPublisher;
import io.mateu.workflow.application.out.StepExecutionRepository;
import io.mateu.workflow.domain.aggregates.Step;
import io.mateu.workflow.domain.aggregates.StepExecution;
import io.mateu.workflow.domain.aggregates.StepExecutionStatus;
import io.mateu.workflow.domain.aggregates.StepType;
import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import static io.mateu.core.infra.JsonSerializer.pojoFromJson;

@Service
@RequiredArgsConstructor
@Slf4j
public class StartStepExecutionUseCase {

    final StepExecutionRepository stepExecutionRepository;

    final io.mateu.workflow.application.out.WorkflowTracing workflowTracing;
    final io.mateu.workflow.application.services.ProcessTrace processTrace;
    private final DownstreamEventPublisher downstreamEventPublisher;

    public void handle(StartStepExecutionCommand command) {
        // The step execution is read here, before the span, only to learn which process this
        // dispatch belongs to: the span is anchored to that process's trace so it lands in the same
        // picture as the process and its steps, rather than starting a trace of its own that
        // nothing connects to anything. Not an extra read — dispatch() used to do this one.
        var stepExecution = stepExecutionRepository.findById(command.stepExecutionId()).orElseThrow();
        workflowTracing.continuing(
                processTrace.contextFor(stepExecution.getProcessId()),
                "eventconductor.dispatch-step",
                java.util.Map.of("eventconductor.process.id", String.valueOf(stepExecution.getProcessId()),
                        "eventconductor.step.executionId", command.stepExecutionId(),
                        "eventconductor.step.id", String.valueOf(stepExecution.getStepId()),
                        "eventconductor.workflow.id", String.valueOf(stepExecution.getWorkflowDefinitionId())),
                () -> dispatch(command, stepExecution));
    }

    private void dispatch(StartStepExecutionCommand command, StepExecution stepExecution) {
        // Idempotency: only dispatch if the step is still waiting (PENDING).
        // A duplicate event arriving after the worker has already responded
        // (status RUNNING / COMPLETED / ERROR / …) is silently ignored.
        if (stepExecution.getStatus() != StepExecutionStatus.PENDING) {
            log.warn("Step execution {} is already in status {}, ignoring duplicate TaskExecutionRequested",
                    command.stepExecutionId(), stepExecution.getStatus());
            return;
        }
        // The step's clock starts here, not where the step was marked started.
        //
        // A step is marked started by the orchestrator and its dispatch is written to the outbox;
        // this runs when that dispatch has been relayed out again, which is the first moment the
        // task is genuinely on its way to a worker. Everything in between — outbox residence, the
        // relay, the broker — is queueing, and charging it to the step's timeout is what turns a
        // backlog into failures: once the dispatch backlog exceeds the timeout, steps expire
        // before any worker has seen them, the retries go with them, and their sagas roll back.
        // Measured under deliberate overload: 12,517 ERROR and 3,035 COMPENSATION_FAILED, none of
        // which was a worker doing anything wrong.
        //
        // Two published contracts already describe it this way and neither was true before:
        // `timeout` is "Maximum execution time" in the definition schema, and
        // `eventconductor.step.duration` is "from dispatch to final status" in the observability
        // reference. Both derive from startedAt, so moving it here makes both of them so.
        //
        // The engine already holds this principle elsewhere: pausing a process shifts startedAt by
        // the pause duration, precisely so that time a step could not be running does not count
        // against it. Queue time is the same thing, and was the case left out.
        stepExecution = stepExecution.withStartedAt(java.time.LocalDateTime.now());
        stepExecutionRepository.save(stepExecution);

        var taskId = "";
        var step = pojoFromJson(stepExecution.getStepJson(), Step.class);
        if (StepType.USER_TASK.equals(step.type())) {
            taskId = "complete-form";
        }
        if (StepType.RULE.equals(step.type())) {
            taskId = "evaluate-rule";
        }
        if (StepType.HTTP_CALL.equals(step.type())) {
            taskId = io.mateu.workflow.domain.services.HttpRequestRenderer.TASK_ID;
        }
        // An ACTION that references a task contract dispatches its pinned `<id>@<version>` as the
        // taskId, so a worker resolves its handler by contract rather than by stepId. Pinned at
        // import; empty for an ACTION with no contract, unchanged from before task contracts.
        if (StepType.ACTION.equals(step.type()) && step.task() != null && !step.task().isBlank()) {
            taskId = step.task();
        }
        downstreamEventPublisher.publish(new TaskExecutionRequested(
                stepExecution.id(),
                stepExecution.getProcessId(),
                stepExecution.getWorkflowDefinitionId(),
                stepExecution.getStepId(),
                taskId,
                stepExecution.getVariables().stream()
                        .map(variable -> new Variable(variable.name(), variable.value()))
                        .toList()
        ), step.topic());
    }

}
