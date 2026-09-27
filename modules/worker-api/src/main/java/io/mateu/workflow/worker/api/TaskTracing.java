package io.mateu.workflow.worker.api;

import io.mateu.workflow.dtos.TraceContext;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;

/**
 * Runs a task inside the trace the engine dispatched it in.
 *
 * <p>The engine puts W3C {@code traceparent}/{@code tracestate}/{@code baggage} headers on every
 * {@code TaskExecutionRequested} it publishes. A transport adapter reads them and hands them to the
 * {@link TaskDispatcher}, which runs the handler through this: with a tracing bridge on the
 * classpath the handler runs in a span that is a child of the engine's dispatch, so whatever the
 * handler calls — HTTP, JDBC, Kafka, all auto-instrumented — lands in the same trace as the process
 * that asked for it. Without one it is {@link #NOOP} and the handler runs exactly as before.
 *
 * <p>Must never fail a task: a context that cannot be read runs the work untraced.
 */
public interface TaskTracing {

    TaskTracing NOOP = (task, context, work) -> work.run();

    /**
     * @param task    the task being run, for the span's name and attributes
     * @param context the context the engine dispatched it in; null when it sent none
     * @param work    the task itself; runs exactly once, and its exceptions propagate unchanged
     */
    void run(TaskExecutionRequested task, TraceContext context, Runnable work);
}
