package io.mateu.workflow.worker.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Runs one dispatched task end to end, the same way in every transport: resolve the handler, bind
 * the variables into its input, invoke it, and answer the engine — turning outcomes into replies so
 * the adapters (Kafka, embedded) only have to deliver an inbound event and provide a
 * {@link TaskReplySink}. Plain object, no Spring, no broker.
 *
 * <p>Outcome mapping:
 * <ul>
 *   <li>handler returns → {@code completed} with the output variables;</li>
 *   <li>{@link TaskFailure} (a declared business error) → {@code failed} with its code as reason;</li>
 *   <li>a binding error, or any other exception → {@code failed} with a clear reason (the engine
 *       then retries per the step's {@code retries});</li>
 *   <li>{@link ReplyNotAcceptedException} from the sink → propagated, so the caller can have the
 *       task redelivered rather than lost.</li>
 * </ul>
 * Cancellation is claimed before starting and again before replying (decision: a cancellation that
 * overtakes or races the task still stops it); the handler can also poll it via the context.
 */
public final class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    private final TaskRegistry registry;
    private final TaskReplySink sink;
    private final Cancellations cancellations;
    private final VariableBinding binding;
    private final boolean strict;
    private final TaskTracing tracing;

    public TaskDispatcher(TaskRegistry registry, TaskReplySink sink, Cancellations cancellations,
                          ObjectMapper mapper, boolean strict) {
        this(registry, sink, cancellations, mapper, strict, TaskTracing.NOOP);
    }

    public TaskDispatcher(TaskRegistry registry, TaskReplySink sink, Cancellations cancellations,
                          ObjectMapper mapper, boolean strict, TaskTracing tracing) {
        this.registry = registry;
        this.sink = sink;
        this.cancellations = cancellations == null ? Cancellations.NONE : cancellations;
        this.binding = new VariableBinding(mapper);
        this.strict = strict;
        this.tracing = tracing == null ? TaskTracing.NOOP : tracing;
    }

    public void dispatch(TaskExecutionRequested task) {
        dispatch(task, null);
    }

    /**
     * {@link #dispatch(TaskExecutionRequested)} inside the trace the engine dispatched the task in:
     * the handler and its reply run as a child of the engine's span (see {@link TaskTracing}).
     *
     * @param traceContext the W3C context from the task's transport headers; null when there is none
     */
    public void dispatch(TaskExecutionRequested task, io.mateu.workflow.dtos.TraceContext traceContext) {
        tracing.run(task, traceContext, () -> run(task, traceContext));
    }

    private void run(TaskExecutionRequested task, io.mateu.workflow.dtos.TraceContext traceContext) {
        var registration = registry.resolve(task.taskId(), task.stepId());
        if (registration == null) {
            var which = task.taskId() != null && !task.taskId().isBlank()
                    ? "task '" + task.taskId() + "'" : "step '" + task.stepId() + "'";
            if (strict) {
                sink.failed(task, List.of(), "No task handler registered for " + which + ".");
            } else {
                log.warn("No task handler registered for {} — ignoring (set eventconductor.worker.strict"
                        + "=true to fail instead).", which);
            }
            return;
        }

        if (cancellations.claim(task.taskExecutionId())) {
            log.info("Task {} was cancelled before it started.", task.taskExecutionId());
            return;
        }

        Object input;
        try {
            input = binding.toInput(task.variables(), registration.inputType());
        } catch (VariableBinding.BindingException e) {
            sink.failed(task, List.of(), e.getMessage());
            return;
        }

        Object output;
        try {
            output = invoke(registration, input, contextFor(task, traceContext));
        } catch (TaskFailure failure) {
            sink.failed(task, List.of(), failure.reason());
            return;
        } catch (ReplyNotAcceptedException redelivery) {
            throw redelivery;
        } catch (Exception e) {
            sink.failed(task, List.of(), e.toString());
            return;
        }

        List<Variable> variables;
        try {
            variables = binding.toVariables(output);
        } catch (VariableBinding.BindingException e) {
            sink.failed(task, List.of(), e.getMessage());
            return;
        }

        if (cancellations.claim(task.taskExecutionId())) {
            log.info("Task {} was cancelled while it ran — not reporting it done.", task.taskExecutionId());
            return;
        }
        sink.completed(task, variables);
    }

    @SuppressWarnings("unchecked")
    private Object invoke(TaskRegistration<?, ?> registration, Object input, TaskContext context)
            throws Exception {
        return ((TaskHandler<Object, Object>) registration.handler()).handle(input, context);
    }

    private TaskContext contextFor(TaskExecutionRequested task, io.mateu.workflow.dtos.TraceContext traceContext) {
        return new TaskContext() {
            public io.mateu.workflow.dtos.TraceContext traceContext() {
                return traceContext;
            }

            public String taskExecutionId() {
                return task.taskExecutionId();
            }

            public String processId() {
                return task.processId();
            }

            public String workflowDefinitionId() {
                return task.workflowDefinitionId();
            }

            public String stepId() {
                return task.stepId();
            }

            public boolean isCancelled() {
                return cancellations.isCancelled(task.taskExecutionId());
            }

            public void progress(String message) {
                if (message != null) {
                    log.debug("Task {} progress: {}", task.taskExecutionId(), message);
                }
                sink.running(task);
            }
        };
    }
}
