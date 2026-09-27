package io.mateu.workflow.dtos.events.integration;

import io.mateu.workflow.ddd.DomainEvent;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.mateu.workflow.dtos.TraceContext;
import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.security.AuthorizationContext;

import java.util.List;

/**
 * @param caller who asked for this process, as the identity and granted scopes the edge that
 *               accepted the request had already validated — never the token itself, which by the
 *               time a step runs has long expired (see {@link AuthorizationContext}). It is what the
 *               definition's {@code requiredScopes}/{@code requiredRoles} are checked against.
 *               {@code null} for a producer that predates the field, and for anything the engine
 *               starts for itself, where {@link AuthorizationContext#SYSTEM} says so explicitly.
 * @param traceContext the W3C trace context the process is to join, so it reads as part of the
 *               caller's trace rather than as a trace of its own. Optional, and left off the wire
 *               when null: an engine that predates the field ignores it, and a producer that does
 *               not set it gets exactly the behaviour it had. A producer that cannot touch the
 *               payload can send the same thing as {@code traceparent}/{@code tracestate}/
 *               {@code baggage} Kafka record headers instead; the payload wins when both are there.
 */
public record ProcessCreationRequested(String workflowDefinitionId, String businessKey, List<Variable> variables,
                                       String parentStepExecutionId,
                                       AuthorizationContext caller,
                                       @JsonInclude(JsonInclude.Include.NON_NULL)
                                       TraceContext traceContext) implements DomainEvent {

    /** The shape before trace propagation: a creation that joins no caller's trace. */
    public ProcessCreationRequested(String workflowDefinitionId, String businessKey, List<Variable> variables,
                                    String parentStepExecutionId, AuthorizationContext caller) {
        this(workflowDefinitionId, businessKey, variables, parentStepExecutionId, caller, null);
    }

    /** Top-level process creation (no parent step execution). */
    public ProcessCreationRequested(String workflowDefinitionId, String businessKey, List<Variable> variables) {
        this(workflowDefinitionId, businessKey, variables, null, null, null);
    }

    /** The shape before flow authorization existed: a creation that names nobody. */
    public ProcessCreationRequested(String workflowDefinitionId, String businessKey, List<Variable> variables,
                                    String parentStepExecutionId) {
        this(workflowDefinitionId, businessKey, variables, parentStepExecutionId, null, null);
    }

    /** This same request, joining {@code traceContext}'s trace. */
    public ProcessCreationRequested withTraceContext(TraceContext traceContext) {
        return new ProcessCreationRequested(workflowDefinitionId, businessKey, variables, parentStepExecutionId,
                caller, traceContext);
    }

    @Override
    public String partitionKey() {
        return businessKey;
    }
}
