package io.mateu.workflow.worker.api;

/**
 * What a {@link TaskHandler} is told about the task it is running and the levers it has while it
 * runs. Created per task by the runtime; never implemented by application code.
 */
public interface TaskContext {

    /** The engine's step-execution id — the reply and any cancellation are keyed by it. */
    String taskExecutionId();

    String processId();

    String workflowDefinitionId();

    /** The workflow step this task is running for. */
    String stepId();

    /**
     * Whether the engine has asked to cancel this task. Polled by a long-running handler to bail
     * out early; the runtime also checks it before starting and before replying, so a handler that
     * ignores this is still cancelled at those points. Non-consuming — safe to call repeatedly.
     */
    boolean isCancelled();

    /**
     * Reports progress: sends a {@code RUNNING} status, which resets the step's timeout clock. Call
     * it from a long task so a slow-but-healthy run is not mistaken for a lost one.
     */
    void progress(String message);

    /**
     * The W3C trace context the engine dispatched this task in, or null when it sent none. The
     * handler already runs inside it when the worker has a tracing bridge; this is for handing it
     * on by hand — to a call no instrumentation covers, or to a process this task starts.
     */
    default io.mateu.workflow.dtos.TraceContext traceContext() {
        return null;
    }
}
