package io.mateu.workflow.application.usecases.process.stepover;

import io.mateu.workflow.application.out.LockService;
import io.mateu.workflow.application.out.ProcessLockService;
import io.mateu.workflow.application.out.ProcessRepository;
import io.mateu.workflow.application.out.StepExecutionRepository;
import io.mateu.workflow.application.out.WorkflowMetrics;
import io.mateu.workflow.application.usecases.process.childcancel.CancelChildProcessService;
import io.mateu.workflow.application.out.DownstreamEventPublisher;
import io.mateu.workflow.application.usecases.process.parentnotify.NotifyParentStepService;
import io.mateu.workflow.application.usecases.stepexecution.update.UpdateStepExecutionCommand;
import io.mateu.workflow.application.usecases.stepexecution.update.UpdateStepExecutionUseCase;
import io.mateu.workflow.domain.aggregates.Process;
import io.mateu.workflow.domain.aggregates.ProcessStatus;
import io.mateu.workflow.domain.aggregates.Step;
import io.mateu.workflow.domain.aggregates.StepExecution;
import io.mateu.workflow.domain.aggregates.StepExecutionStatus;
import io.mateu.workflow.domain.aggregates.StepType;
import io.mateu.workflow.domain.aggregates.WorkflowDefinition;
import io.mateu.workflow.domain.services.LockKeyResolver;
import io.mateu.workflow.domain.services.WorkflowOrchestrationService;
import io.mateu.workflow.dtos.events.integration.TaskCancellationRequested;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static io.mateu.core.infra.JsonSerializer.pojoFromJson;

@Service
@RequiredArgsConstructor
@Slf4j
public class StepOverProcessUseCase {

    final ProcessRepository processRepository;
    final StepExecutionRepository stepExecutionRepository;
    final ProcessLockService processLockService;
    final WorkflowMetrics workflowMetrics;
    final WorkflowOrchestrationService workflowOrchestrationService;
    final NotifyParentStepService notifyParentStepService;
    final io.mateu.workflow.application.services.RecordProcessTraceService recordProcessTraceService;
    final CancelChildProcessService cancelChildProcessService;
    final DownstreamEventPublisher downstreamEventPublisher;
    final io.mateu.workflow.application.out.WorkflowTracing workflowTracing;
    final io.mateu.workflow.application.services.ProcessTrace processTrace;
    final LockService lockService;
    // ObjectProvider (not direct injection): waking a granted waiter runs UpdateStepExecutionUseCase,
    // which sits on the same step-update pipeline that leads back here, so a direct dependency could
    // close an injection cycle — the same reason NotifyParentStepService uses one.
    final ObjectProvider<UpdateStepExecutionUseCase> updateStepExecutionUseCase;
    // Self, via a provider: waking a process-level waiter steps IT over, and a bean cannot inject
    // itself directly.
    final ObjectProvider<StepOverProcessUseCase> self;
    // A provider: only needed to link a REPLY's span back to its synchronous caller's trace.
    final ObjectProvider<io.mateu.workflow.application.out.InvocationRepository> invocationRepository;

    /** Where PUBLISH_EVENT steps may publish; optional so hand-built instances (tests) still work. */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    io.mateu.workflow.infra.config.EventDestinationsProperties eventDestinations;

    @org.springframework.beans.factory.annotation.Value("${workflow.mode:embedded}")
    String mode = "embedded";

    /** The largest reply a REPLY step may record, in bytes of JSON; 0 or less means no limit. */
    @org.springframework.beans.factory.annotation.Value("${workflow.sync.max-reply-bytes:262144}")
    long maxReplyBytes;

    public void handle(StepOverProcessCommand command) {
        // Serialize per process: two concurrent step-overs (e.g. two parallel steps
        // completing at once, or two pods handling events for the same process) would
        // both see the next step as CREATED and dispatch it twice.
        // Named, because this is where a process actually moves and it is the span an operator
        // reading a trace is looking for — everything else in the picture is a database call.
        // Anchored to the process's own trace rather than started as a root of its own. Without
        // this, each step-over is the beginning of a fresh trace — a pile of two-millisecond spans
        // with nothing to say which process they belong to or what came before them — because the
        // context that produced the event does not survive the outbox row and the broker record in
        // between. The anchor is derived from the process id, so every pod computes the same one.
        if (!processLockService.runExclusively(command.processId(),
                () -> workflowTracing.continuing(
                        processTrace.contextFor(command.processId()),
                        "eventconductor.step-over",
                        java.util.Map.of("eventconductor.process.id", command.processId()),
                        () -> doHandle(command)))) {
            log.error("Could not acquire lock for process {}, skipping step-over (another node is working on it)",
                    command.processId());
        }
    }

    private void doHandle(StepOverProcessCommand command) {
        // Steps first, process second, and the order is load-bearing. This query auto-flushes
        // whatever writes are pending on JPA — including a version bump on this very process, left
        // by a caller that saved it just before (ResumeProcessUseCase does exactly that). Mapping
        // the process first would capture the version that flush is about to supersede, and the
        // save below would then fail optimistic locking and roll the whole handler back.
        var stepExecutions = stepExecutionRepository.findByProcessId(command.processId());
        var process = processRepository.findById(command.processId()).orElseThrow();
        workflowTracing.tagCurrentSpan("eventconductor.workflow.id", process.getWorkflowDefinitionId());
        workflowTracing.tagCurrentSpan("eventconductor.process.businessKey", process.getBusinessKey());

        // Process-level lock: a definition can serialize its whole instances by a key. Gate the
        // process here — before any step is dispatched — so an instance that cannot take the lock
        // makes no progress at all until it is admitted. Reentrant, so once this instance holds the
        // lock every later step-over sails through; the terminal-state releaseAll below frees it.
        if (!holdsProcessLockOrParks(process)) {
            return;
        }

        var result = workflowOrchestrationService.calculateNextTransitions(process, stepExecutions);

        // Resolve any LOCK/UNLOCK step that just started this pass. It runs in this same transaction,
        // which already holds the process lock, so acquire/release is serialized per process here and
        // per key in the LockService. A LOCK either completes (acquired) or parks (WAITING_ON_LOCK);
        // an UNLOCK completes and may hand the lock to a waiter, whom we wake once it is persisted.
        var grantsToWake = resolveLockSteps(process, result.getStepsToSave());

        // Resolve any REPLY step that just started: its payload is computed from the process as it
        // stands and recorded on it, in this same transaction, so the reply commits with the
        // transition that produced it.
        boolean replied = resolveReplySteps(result.getUpdatedProcess(), result.getStepsToSave());
        // And any PUBLISH_EVENT: the event rides this transaction into the outbox with the step's
        // completion — published if and only if the step completed.
        resolvePublishSteps(result.getUpdatedProcess(), result.getStepsToSave());

        if (result.getUpdatedProcess() != process || replied) {
            processRepository.save(result.getUpdatedProcess());
        }
        tagOutcome(result.getUpdatedProcess(), result.getStepsToSave());

        // Status as it was before the transition decided anything. The orchestration service is
        // pure and hands back copies (@With), so the list read above still holds the old values —
        // which is the only way to tell a step that was merely waiting its turn from one a worker
        // is running right now.
        var statusBefore = stepExecutions.stream()
                .collect(java.util.stream.Collectors.toMap(StepExecution::getId, StepExecution::getStatus));

        result.getStepsToSave().forEach(stepExecution -> {
            stepExecutionRepository.save(stepExecution);
            // A branch still running at a worker when another branch reaches END is cancelled here
            // by a plain status flip, which reaches nobody: the worker finishes and reports on a
            // process that is already over. Same reasoning — and same event — as the saga rollback
            // path and CancelProcessUseCase.
            var before = statusBefore.get(stepExecution.getId());
            if (StepExecutionStatus.CANCELLED.equals(stepExecution.getStatus())
                    && before != null && before.isInFlightAtAWorker()) {
                downstreamEventPublisher.publish(new TaskCancellationRequested(stepExecution.getId()),
                        stepExecution.topic());
            }
            // END-transition and implicit-completion cancellations (and start-time errors)
            // flow through here — a PROCESS step ending CANCELLED/ERROR must take its
            // still-running child down with it.
            cancelChildProcessService.stepReachedTerminalStatus(stepExecution);
        });

        // The UNLOCK steps above are persisted now; wake whoever they handed the lock to.
        grantsToWake.forEach(this::wakeWaiter);

        // A process that ends still holding locks (it errored inside a critical section, or simply
        // never reached its UNLOCK) must not wedge the key forever: release everything it holds and
        // wake the waiters. The lease reaper is the backstop for a pod that dies without getting here.
        if (result.isProcessErrored() || result.isProcessCompleted()) {
            lockService.releaseAll(process.getId()).forEach(this::wakeWaiter);
        }

        if (result.isProcessErrored()) {
            workflowMetrics.processErrored(
                    result.getUpdatedProcess().getWorkflowDefinitionId(),
                    WorkflowMetrics.durationOf(result.getUpdatedProcess())
            );
        } else if (result.isProcessCompleted()) {
            workflowMetrics.processCompleted(
                    result.getUpdatedProcess().getWorkflowDefinitionId(),
                    WorkflowMetrics.durationOf(result.getUpdatedProcess())
            );
        }

        // If this process is a child workflow and just reached a terminal status, complete
        // (or error) the PROCESS step of the parent that spawned it.
        if (result.isProcessErrored() || result.isProcessCompleted()) {
            notifyParentStepService.processReachedTerminalStatus(result.getUpdatedProcess());
            // The same moment, seen the other way: this is where the process's whole run is
            // finally known, so it is where it can be written out as a trace.
            recordProcessTraceService.processReachedTerminalStatus(result.getUpdatedProcess());
        }
    }

    /**
     * What this step-over decided, on its span: the process's status after it, and each step it
     * moved as {@code stepId=STATUS}. Bounded, because a fan-out can move hundreds of steps and a
     * span attribute is not the place to list them all.
     */
    private void tagOutcome(Process updated, List<StepExecution> stepsToSave) {
        if (!workflowTracing.enabled()) {
            return;
        }
        workflowTracing.tagCurrentSpan("eventconductor.process.status", String.valueOf(updated.getStatus()));
        if (!stepsToSave.isEmpty()) {
            var moved = new StringBuilder();
            for (var stepExecution : stepsToSave) {
                if (moved.length() > 480) {
                    moved.append(",…");
                    break;
                }
                if (!moved.isEmpty()) {
                    moved.append(',');
                }
                moved.append(stepExecution.getStepId()).append('=').append(stepExecution.getStatus());
            }
            workflowTracing.tagCurrentSpan("eventconductor.steps.moved", moved.toString());
        }
    }

    /**
     * Acquire/release the lock for each LOCK/UNLOCK step that {@code start()} just moved to PENDING
     * this pass, mutating the step to its resolved status. A LOCK completes (acquired) or parks
     * (WAITING_ON_LOCK); an UNLOCK completes and returns the lock, so the grant it produces is
     * collected for waking. Only freshly-started lock steps are PENDING here — a parked waiter is
     * WAITING_ON_LOCK and a re-processed one is terminal, so neither re-acquires.
     */
    private List<LockService.Grant> resolveLockSteps(Process process, List<StepExecution> stepsToSave) {
        var grants = new ArrayList<LockService.Grant>();
        for (var stepExecution : stepsToSave) {
            if (!StepExecutionStatus.PENDING.equals(stepExecution.getStatus())) {
                continue;
            }
            var step = pojoFromJson(stepExecution.getStepJson(), Step.class);
            boolean isLock = StepType.LOCK.equals(step.type());
            boolean isUnlock = StepType.UNLOCK.equals(step.type());
            if (!isLock && !isUnlock) {
                continue;
            }
            var name = lockName(step, stepExecution);
            var key = LockKeyResolver.resolve(step, process);
            if (key == null) {
                // start() validates the key with the same resolver, so a PENDING lock step always
                // has one; guard anyway rather than silently leaving it PENDING forever.
                stepExecution.updateStatus(StepExecutionStatus.ERROR);
                continue;
            }
            if (isLock) {
                var outcome = lockService.acquire(name, key, process.getId(), stepExecution.id());
                if (outcome == LockService.Outcome.ACQUIRED) {
                    stepExecution.updateStatus(StepExecutionStatus.COMPLETED);
                } else {
                    stepExecution.markWaitingOnLock();
                }
            } else {
                lockService.release(name, key, process.getId()).ifPresent(grants::add);
                stepExecution.updateStatus(StepExecutionStatus.COMPLETED);
            }
        }
        return grants;
    }

    /**
     * Completes each REPLY step that {@code start()} just moved to PENDING this pass: computes its
     * payload and records it on the process. A process replies once — a second REPLY (which
     * validation rules out, but a runtime-injected step or an operator retry could still reach)
     * completes without replacing the first answer. A payload that cannot be computed fails the
     * step, so the failure contract answers the caller instead.
     *
     * @return whether a reply was recorded on the process (which must then be saved)
     */
    private boolean resolveReplySteps(Process process, List<StepExecution> stepsToSave) {
        boolean recorded = false;
        for (var stepExecution : stepsToSave) {
            if (!StepExecutionStatus.PENDING.equals(stepExecution.getStatus())) {
                continue;
            }
            var step = pojoFromJson(stepExecution.getStepJson(), Step.class);
            if (!StepType.REPLY.equals(step.type())) {
                continue;
            }
            var payload = io.mateu.workflow.domain.services.ReplyPayloadResolver.resolve(step, process, maxReplyBytes);
            if (payload.error() != null) {
                log.warn("REPLY step {} of process {} failed: {}", step.id(), process.getId(), payload.error());
                stepExecution.logError(payload.error());
                stepExecution.updateStatus(StepExecutionStatus.ERROR);
                continue;
            }
            if (process.recordReply(io.mateu.workflow.domain.aggregates.ProcessReply.replied(step.id(), payload.json()))) {
                recorded = true;
                traceReply(process, step);
            } else {
                log.warn("Process {} already replied; REPLY step {} completes without replacing the first answer",
                        process.getId(), step.id());
            }
            stepExecution.updateStatus(StepExecutionStatus.COMPLETED);
        }
        return recorded;
    }

    /**
     * A span for the answer, in the process's trace (this step-over runs in it), linked to the trace
     * of the caller that is waiting for it — so the request can be followed to its reply even when
     * the reply is given on another pod.
     */
    private void traceReply(Process process, Step step) {
        var repository = invocationRepository.getIfAvailable();
        var caller = repository == null ? null : repository.findByProcessId(process.getId())
                .map(io.mateu.workflow.application.sync.Invocation::callerTraceParent).orElse(null);
        workflowTracing.spanLinkedTo("eventconductor.sync.reply", caller,
                java.util.Map.of("eventconductor.process.id", process.getId(),
                        "eventconductor.step.id", step.id()), () -> null);
    }

    /**
     * Renders and emits each PUBLISH_EVENT step that {@code start()} just moved to PENDING, then
     * completes it — or fails it (so retries and compensation engage) when the destination is unknown
     * or the payload cannot be rendered.
     */
    private void resolvePublishSteps(Process process, List<StepExecution> stepsToSave) {
        for (var stepExecution : stepsToSave) {
            if (!StepExecutionStatus.PENDING.equals(stepExecution.getStatus())) {
                continue;
            }
            var step = pojoFromJson(stepExecution.getStepJson(), Step.class);
            if (!StepType.PUBLISH_EVENT.equals(step.type())) {
                continue;
            }
            try {
                var event = io.mateu.workflow.domain.services.ExternalEventRenderer.render(
                        step, process, stepExecution.id(), eventDestinations, "kafka".equals(mode));
                stepExecution.publish(event);
                stepExecution.logInfo("Event '" + event.eventType() + "' published to '" + event.destination()
                        + "' with key '" + event.key() + "'.");
                stepExecution.updateStatus(StepExecutionStatus.COMPLETED);
            } catch (IllegalArgumentException | io.mateu.workflow.template.Templates.TemplateException e) {
                log.warn("PUBLISH_EVENT step {} of process {} failed: {}", step.id(), process.getId(), e.getMessage());
                stepExecution.logError(e.getMessage());
                stepExecution.updateStatus(StepExecutionStatus.ERROR);
            }
        }
    }

    /** A step-level lock defaults its domain to the definition id, so unrelated definitions do not
     *  collide; a shared lock across definitions is opted into by naming it explicitly. */
    private static String lockName(Step step, StepExecution stepExecution) {
        return step.lockName() != null && !step.lockName().isBlank()
                ? step.lockName() : stepExecution.getWorkflowDefinitionId();
    }

    /**
     * Wake whoever a release admitted. A step-level waiter is completed through the ordinary
     * step-update pipeline — which acquires that process's lock and drives its own step-over — so the
     * critical section runs next. (A process-level waiter carries no step; it is stepped over when
     * that gate lands.)
     */
    private void wakeWaiter(LockService.Grant grant) {
        if (grant.stepExecutionId() != null) {
            // Step-level waiter: complete its parked LOCK step through the ordinary update pipeline,
            // which drives that process's own step-over into the critical section.
            updateStepExecutionUseCase.getObject().handle(new UpdateStepExecutionCommand(
                    grant.stepExecutionId(), List.of(),
                    "Lock '" + grant.lockName() + "' on '" + grant.lockKey() + "' acquired",
                    StepExecutionStatus.COMPLETED));
        } else {
            // Process-level waiter: step it over so its gate re-acquires (now the holder) and runs.
            self.getObject().handle(new StepOverProcessCommand(grant.processId()));
        }
    }

    /**
     * Whether this process may proceed under its definition's process-level lock: true if it holds
     * the lock (or the definition declares none), false if it was parked in the queue and must wait.
     * Reentrant — a process already holding the lock re-acquires it — so this is safe to call on
     * every step-over.
     */
    private boolean holdsProcessLockOrParks(Process process) {
        var json = process.getWorkflowDefinitionJson();
        if (json == null || json.isBlank()) {
            return true; // no definition snapshot (e.g. test fixtures) — nothing to gate on
        }
        WorkflowDefinition definition;
        try {
            definition = pojoFromJson(json, WorkflowDefinition.class);
        } catch (Exception e) {
            return true; // an unreadable snapshot must not wedge the process
        }
        var processLock = definition.processLock();
        if (processLock == null) {
            return true;
        }
        // Only an active instance is gated; a paused/cancelled/terminal one is handled elsewhere and
        // its lock is freed by the terminal-state releaseAll.
        if (process.getStatus() != ProcessStatus.RUNNING && process.getStatus() != ProcessStatus.PENDING) {
            return true;
        }
        var name = processLock.resolvedName(process.getWorkflowDefinitionId());
        var key = LockKeyResolver.resolveExpression(processLock.key(), process,
                "processLock key '" + processLock.key() + "' for definition " + process.getWorkflowDefinitionId());
        if (key == null) {
            // A key that will not evaluate cannot serialize anything; run rather than park forever.
            log.warn("processLock key '{}' for process {} could not be evaluated; running unserialized",
                    processLock.key(), process.getId());
            return true;
        }
        return lockService.acquire(name, key, process.getId(), null) == LockService.Outcome.ACQUIRED;
    }

}
