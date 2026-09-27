package io.mateu.workflow.application.usecases.process.create;

import io.mateu.workflow.domain.aggregates.Variable;
import io.mateu.workflow.dtos.TraceContext;
import io.mateu.workflow.security.AuthorizationContext;

import java.util.List;

public record CreateProcessCommand(
        String processId,
        String workflowDefinitionId,
        String businessKey,
        List<Variable> variables,
        /** Id of the parent PROCESS step execution that spawned this process; null for top-level processes. */
        String parentStepExecutionId,
        /**
         * Who asked, for the definition's {@code requiredScopes}/{@code requiredRoles} to be checked
         * against. {@link AuthorizationContext#SYSTEM} for anything the engine starts for itself —
         * cron, a PROCESS step — and {@code null} for a caller nothing could identify, which is
         * denied the moment the definition requires anything.
         */
        AuthorizationContext caller,
        /**
         * The caller's W3C trace context, for the process to join rather than start a trace of its
         * own; null when the caller sent none. See {@code ProcessTrace}.
         */
        TraceContext traceContext
) {

    /** The shape before trace propagation: a creation that joins no caller's trace. */
    public CreateProcessCommand(String processId, String workflowDefinitionId, String businessKey,
                                List<Variable> variables, String parentStepExecutionId,
                                AuthorizationContext caller) {
        this(processId, workflowDefinitionId, businessKey, variables, parentStepExecutionId, caller, null);
    }

    /** The shape before flow authorization existed: a creation that names nobody. */
    public CreateProcessCommand(String processId, String workflowDefinitionId, String businessKey,
                                List<Variable> variables, String parentStepExecutionId) {
        this(processId, workflowDefinitionId, businessKey, variables, parentStepExecutionId, null, null);
    }
}
