package io.mateu.workflow.infra.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The W3C trace context a process joined at creation (see {@code ProcessTraceContextRepository}).
 * A table of its own rather than columns on the process: only processes started from inside a
 * caller's trace have a row, it is written once and never updated, and the process row — the
 * hottest one in the engine — stays exactly as it was. Declared here as well as in
 * {@code V35__process_trace_context.sql}, for the schemas {@code ddl-auto} creates.
 */
@Entity
@Table(name = "process_trace_context")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessTraceContextEntity {

    @Id
    @Column(name = "process_id")
    private String processId;

    @Column(name = "trace_parent", length = 64, nullable = false)
    private String traceParent;

    @Column(name = "trace_state", length = 512)
    private String traceState;

    @Column(name = "baggage", length = 2048)
    private String baggage;
}
