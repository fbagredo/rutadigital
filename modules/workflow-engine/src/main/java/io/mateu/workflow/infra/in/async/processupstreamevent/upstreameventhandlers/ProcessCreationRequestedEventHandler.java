package io.mateu.workflow.infra.in.async.processupstreamevent.upstreameventhandlers;

import io.mateu.workflow.application.out.WorkflowTracing;
import io.mateu.workflow.application.usecases.process.create.CreateProcessCommand;
import io.mateu.workflow.application.usecases.process.create.CreateProcessUseCase;
import io.mateu.workflow.ddd.DomainEvent;
import io.mateu.workflow.ddd.DomainEventHandler;
import io.mateu.workflow.domain.aggregates.Variable;
import io.mateu.workflow.dtos.TraceContext;
import io.mateu.workflow.dtos.events.integration.ProcessCreationRequested;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProcessCreationRequestedEventHandler implements DomainEventHandler<ProcessCreationRequested> {

    final CreateProcessUseCase createProcessUseCase;
    final WorkflowTracing workflowTracing;

    @Override
    public Class<? extends DomainEvent> eventClass() {
        return ProcessCreationRequested.class;
    }

    @Override
    public void handle(ProcessCreationRequested e) {
        var processId = UUID.randomUUID().toString();
        // Re-read through TraceContext.of: the field arrived in an untrusted payload, and only a
        // well-formed traceparent (with bounded extras) is worth keeping for the life of a process.
        var traceContext = e.traceContext() == null ? null
                : TraceContext.of(e.traceContext().traceparent(), e.traceContext().tracestate(),
                        e.traceContext().baggage());
        var command = new CreateProcessCommand(
                processId,
                e.workflowDefinitionId(),
                e.businessKey(),
                e.variables().stream()
                        .map(variable -> new Variable(variable.name(), variable.value()))
                        .toList(),
                e.parentStepExecutionId(),
                e.caller(),
                traceContext
        );
        if (traceContext == null || !workflowTracing.enabled()) {
            createProcessUseCase.handle(command);
            return;
        }
        // The creation itself is the first span of the process in the caller's trace, and what it
        // writes to the outbox (ProcessCreated) carries that trace on to the first step-over.
        var tags = new LinkedHashMap<String, String>();
        tags.put("eventconductor.process.id", processId);
        tags.put("eventconductor.workflow.id", String.valueOf(e.workflowDefinitionId()));
        if (e.businessKey() != null) {
            tags.put("eventconductor.process.businessKey", e.businessKey());
        }
        workflowTracing.continuing(traceContext, "eventconductor.create-process", tags,
                () -> createProcessUseCase.handle(command));
    }
}
