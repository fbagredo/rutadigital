package io.mateu.workflow.application.out;

import io.mateu.workflow.dtos.TraceContext;

import java.util.Collection;
import java.util.Optional;

/**
 * The caller's trace context a process joined when it was created, kept so that every pod, at any
 * later time, continues that trace rather than the one derived from the process id.
 *
 * <p>Only processes that were started <em>with</em> a context have a row: everything else keeps
 * the derived anchor, and costs nothing here.
 */
public interface ProcessTraceContextRepository {

    void save(String processId, TraceContext context);

    Optional<TraceContext> findByProcessId(String processId);

    void deleteAllByProcessId(Collection<String> processIds);
}
