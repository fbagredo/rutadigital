package io.mateu.workflow.infra.out.persistence;

import io.mateu.workflow.ddd.DomainEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

import static io.mateu.core.infra.JsonSerializer.toJson;

/**
 * <p>The indexes are declared here as well as in the Flyway migrations, and that duplication is
 * deliberate. A schema created by {@code ddl-auto} — which is what the embedded path and every
 * test harness use — gets tables from the entities and nothing from the migrations, so without
 * this the engine runs with primary keys only. Every deadline scan, outbox claim and correlation
 * lookup then becomes a sequential scan, and the queries this engine was built around stop being
 * lookups at all. Measured on a cluster where they were missing: PostgreSQL pinned at 750m of CPU
 * and throughput down from tens of process instances a second to about one.
 */
@Entity
@Table(name = "outbox_message_entity", indexes = {
        // The relays claim pending messages oldest first, so the ordering has to be covered too.
        @Index(name = "idx_outbox_status_ts", columnList = "status, timestamp"),
        // The fast path finds the rows its process wrote, and the claim sweeper finds expired claims.
        @Index(name = "idx_outbox_partition_status", columnList = "partition_key, status"),
        @Index(name = "idx_outbox_claim_until", columnList = "status, claim_until")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OutboxMessageEntity {

    @Id
    private String id;

    private LocalDateTime timestamp;

    private String status;

    private String messageType;

    @Column(columnDefinition = "TEXT")
    private String payload;

    /**
     * The W3C {@code traceparent} of whatever produced this event, so the relay can publish it as
     * part of that trace instead of starting a new one.
     *
     * <p>This row is the engine's asynchronous boundary: written inside one transaction, published
     * by another thread later. Nothing automatic bridges that gap — the instrumentation sees a
     * database write in one trace and, some time afterwards, an unrelated Kafka send — so without
     * carrying the context here, following a process end to end gives a trace per hop. Null when
     * nothing was being traced, which is the normal case.
     */
    @Column(length = 64)
    private String traceParent;

    /**
     * The {@code tracestate} and {@code baggage} of the trace the row was produced in — present only
     * for a process that joined a caller's trace carrying them. Passed through untouched onto the
     * published record's headers, next to the relay's own {@code traceparent}.
     */
    @Column(name = "trace_state", length = 512)
    private String traceState;

    @Column(name = "baggage", length = 2048)
    private String baggage;

    /**
     * The event's partition key — the process id for every process event — so the synchronous fast
     * path can find the rows one process wrote without reading payloads. Null for unkeyed events.
     */
    @Column(name = "partition_key")
    private String partitionKey;

    /** The pod whose inline drive claimed this row ({@link OutboxMessageStatus#InlineClaimed}); null otherwise. */
    @Column(name = "claimed_by", length = 64)
    private String claimedBy;

    /** Until when that claim holds unless renewed; past it, the sweeper hands the row back to the relay. */
    @Column(name = "claim_until")
    private LocalDateTime claimUntil;

    /** The shape before rows could be claimed by an inline drive: hand-made rows keep compiling. */
    public OutboxMessageEntity(String id, LocalDateTime timestamp, String status, String messageType,
                               String payload, String traceParent) {
        this(id, timestamp, status, messageType, payload, traceParent, null, null, null, null, null);
    }

    /** An event with no trace attached — what happens when tracing is off, which is the default. */
    public OutboxMessageEntity(DomainEvent event) {
        this(event, (String) null);
    }

    public OutboxMessageEntity(DomainEvent event, String traceParent) {
        this.id = UUID.randomUUID().toString();
        this.timestamp = LocalDateTime.now();
        this.status = OutboxMessageStatus.Pending.name();
        this.messageType = event.getClass().getName();
        this.payload = toJson(event);
        this.traceParent = traceParent;
        this.partitionKey = partitionKeyOf(event);
    }

    /** An event and the whole trace context it was produced in; null when nothing was traced. */
    public OutboxMessageEntity(DomainEvent event, io.mateu.workflow.dtos.TraceContext traceContext) {
        this(event, traceContext == null ? null : traceContext.traceparent());
        if (traceContext != null) {
            this.traceState = traceContext.tracestate();
            this.baggage = traceContext.baggage();
        }
    }

    /** The row's trace context, whole, or null when it was written outside any trace. */
    public io.mateu.workflow.dtos.TraceContext traceContext() {
        return io.mateu.workflow.dtos.TraceContext.of(traceParent, traceState, baggage);
    }

    /**
     * Marks this (not yet saved) row as already claimed by the inline drive of {@code processId}.
     * An unkeyed event (a log line) takes the driven process's id as its key, so the drive finds it.
     */
    public OutboxMessageEntity claimedBy(String processId, String pod, LocalDateTime until) {
        if (this.partitionKey == null) {
            this.partitionKey = processId;
        }
        this.status = OutboxMessageStatus.InlineClaimed.name();
        this.claimedBy = pod;
        this.claimUntil = until;
        return this;
    }

    private static String partitionKeyOf(DomainEvent event) {
        try {
            var key = event.partitionKey();
            return key == null || key.length() > 255 ? null : key;
        } catch (RuntimeException e) {
            return null;
        }
    }
}
