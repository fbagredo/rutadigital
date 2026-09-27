package io.mateu.workflow.infra.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * The held half of a named per-key lock: one row per {@code (lockName, lockKey)} that is currently
 * owned. Its primary key <b>is</b> the mutex — the composite key is materialised into {@link #id}
 * ({@link LockRowId}: {@code <name length>:<lockName>:<lockKey>}) so a would-be second holder simply fails the insert.
 *
 * <p>Declared as an entity so {@code ddl-auto} creates the table for the embedded path and every
 * test harness; the same shape is created by Flyway {@code V29} for deployments that run migrations
 * instead — the two must agree. See {@code JdbcLockService} for the acquire/release protocol and
 * {@code LOCK-SERIALIZATION-PLAN.md} for the design.
 */
@Entity
@Table(name = "process_lock", indexes = {
        // releaseAll(processId) and the terminal-state cleanup list every lock a process holds.
        @Index(name = "idx_process_lock_holder", columnList = "holder_process_id"),
        // The lease reaper scans for locks whose holder has gone away.
        @Index(name = "idx_process_lock_lease", columnList = "lease_deadline_at")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessLockEntity {

    /**
     * {@link LockRowId#of}: {@code <name length>:<lockName>:<lockKey>}. 520 fits the longest pair the
     * two 255-wide columns allow (3 + 1 + 255 + 1 + 255 = 515).
     */
    @Id
    @Column(length = 520)
    private String id;

    private String lockName;

    private String lockKey;

    private String holderProcessId;

    /** The step execution holding the lock for a step-level lock, or null for a process-level one. */
    private String holderStepExecutionId;

    private LocalDateTime acquiredAt;

    /** When the hold expires if never released — the crash backstop, wired by the reaper in P4. */
    private LocalDateTime leaseDeadlineAt;
}
