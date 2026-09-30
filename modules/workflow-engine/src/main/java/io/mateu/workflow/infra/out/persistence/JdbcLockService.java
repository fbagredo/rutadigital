package io.mateu.workflow.infra.out.persistence;

import io.mateu.workflow.application.out.LockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC {@link LockService} for {@code jpa} mode. It coordinates across pods through the database:
 * the held lock is one row in {@code process_lock} keyed by {@code (lockName, lockKey)}, and both
 * acquire and release open by taking {@code SELECT … FOR UPDATE} on that row, so the two are
 * serialized per key across every pod — the same discipline {@link JdbcProcessLockService} uses for
 * the per-process row lock.
 *
 * <p>The queue lives in {@code process_lock_waiter}, ordered by {@code enqueued_at}. On release the
 * earliest waiter (if any) is promoted to holder in the same transaction, so no window exists where
 * the lock is free but a waiter is still queued behind it.
 *
 * <p>The composite key is materialised into a single {@code id} column ({@link LockRowId}:
 * {@code <name length>:<lockName>:<lockKey>}, printable so PostgreSQL accepts it) so the mutex is a
 * plain primary key and {@code FOR UPDATE} is
 * a by-id lookup. The first acquirer of a fresh key races on that INSERT; the loser catches the
 * duplicate-key and retries the {@code FOR UPDATE} path once, in a new transaction (PostgreSQL aborts
 * the one the failed INSERT ran in), by which point the winner's row is visible and it enqueues.
 *
 * <p>Portable across the supported databases: no {@code ON CONFLICT} and no {@code SKIP LOCKED}.
 * {@code LIMIT 1} on the waiter query holds on H2, PostgreSQL and MariaDB; Oracle would route it
 * through {@link DbLockDialect} in a later pass (this deployment is PostgreSQL, tested on H2).
 */
@Service
@ConditionalOnProperty(name = "workflow.persistence", havingValue = "jpa")
@RequiredArgsConstructor
@Slf4j
public class JdbcLockService implements LockService {

    final JdbcTemplate jdbcTemplate;
    final TransactionTemplate transactionTemplate;

    /** Bounds how long an acquire/release waits for the per-key row lock before giving up. */
    @Value("${workflow.lock.wait-timeout-seconds:10}")
    int waitTimeoutSeconds;

    /** How long a hold lasts before the reaper may evict it (crash backstop). Generous by design. */
    @Value("${workflow.lock.lease-ms:900000}")
    long leaseMs;

    private static String rowId(String lockName, String lockKey) {
        return LockRowId.of(lockName, lockKey);
    }

    /** Set once the legacy-id rewrite has run successfully; see {@link #migrateLegacyRowIds()}. */
    private volatile boolean legacyIdsChecked;

    /**
     * Rewrites the ids of rows written by the old NUL-joined encoding, so a lock held across the
     * upgrade is still found (and still excludes) under the new one. Only H2 can hold such rows —
     * PostgreSQL rejected every one of those inserts — and a held lock is short-lived, so this is
     * normally a no-op scan of a near-empty table.
     *
     * <p>Runs lazily, before the first lock operation of this instance, not at startup: a boot must
     * never wait on the database (DIST-08 boots a pod with PostgreSQL paused). Until it succeeds it is
     * retried on the next operation; a failure is logged, never thrown.
     */
    public void migrateLegacyRowIds() {
        if (legacyIdsChecked) {
            return;
        }
        synchronized (this) {
            if (legacyIdsChecked) {
                return;
            }
            try {
                rewriteLegacyRowIds();
                legacyIdsChecked = true;
            } catch (RuntimeException e) {
                log.warn("Could not check process_lock for legacy ids: {}", e.getMessage());
            }
        }
    }

    /** The rewrite itself, unguarded; returns how many rows it rewrote. */
    public int rewriteLegacyRowIds() {
        var rows = jdbcTemplate.query("SELECT id, lock_name, lock_key FROM process_lock",
                (rs, i) -> new String[]{rs.getString(1), rs.getString(2), rs.getString(3)});
        int rewritten = 0;
        for (String[] row : rows) {
            if (LockRowId.isLegacy(row[0])) {
                rewritten += jdbcTemplate.update("UPDATE process_lock SET id = ? WHERE id = ?",
                        rowId(row[1], row[2]), row[0]);
            }
        }
        if (rewritten > 0) {
            log.info("Rewrote {} process_lock id(s) from the NUL-separated encoding", rewritten);
        }
        return rewritten;
    }

    private Timestamp newLease() {
        return Timestamp.valueOf(LocalDateTime.now().plusNanos(leaseMs * 1_000_000));
    }

    @Override
    public Outcome acquire(String lockName, String lockKey, String processId, String stepExecutionId) {
        migrateLegacyRowIds();
        return acquireRetryingTheFirstInsert(lockName, lockKey, processId, stepExecutionId, true);
    }

    @Override
    public Outcome tryAcquire(String lockName, String lockKey, String processId) {
        migrateLegacyRowIds();
        return acquireRetryingTheFirstInsert(lockName, lockKey, processId, null, false);
    }

    /**
     * The first acquirers of a fresh key race on its INSERT: the loser's FOR UPDATE locked nothing (the
     * row did not exist yet) and its INSERT hits the winner's key. It retries once — in a NEW
     * transaction: on PostgreSQL a failed statement aborts the transaction it ran in («current
     * transaction is aborted, commands ignored until end of transaction block», 25P02), so retrying in
     * the same one fails whatever it does. In the new one the winner's row is visible, FOR UPDATE
     * blocks on it and reads the holder, and the loser enqueues.
     */
    private Outcome acquireRetryingTheFirstInsert(String lockName, String lockKey, String processId,
                                                  String stepExecutionId, boolean enqueue) {
        try {
            return transactionTemplate.execute(status ->
                    acquireInTx(lockName, lockKey, processId, stepExecutionId, enqueue));
        } catch (DuplicateKeyException raced) {
            log.debug("Lost the first acquire of {}/{} to another process; retrying", lockName, lockKey);
            return transactionTemplate.execute(status ->
                    acquireInTx(lockName, lockKey, processId, stepExecutionId, enqueue));
        }
    }

    private Outcome acquireInTx(String lockName, String lockKey, String processId,
                                String stepExecutionId, boolean enqueue) {
        String id = rowId(lockName, lockKey);
        String holder = lockRowForUpdate(id);
        if (holder == null) {
            // A DuplicateKeyException here — another acquirer inserted the fresh row first — ends this
            // transaction; the caller retries in a new one.
            jdbcTemplate.update(
                    "INSERT INTO process_lock (id, lock_name, lock_key, holder_process_id, "
                            + "holder_step_execution_id, acquired_at, lease_deadline_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    id, lockName, lockKey, processId, stepExecutionId,
                    Timestamp.valueOf(LocalDateTime.now()), newLease());
            return Outcome.ACQUIRED;
        }
        if (processId.equals(holder)) {
            return Outcome.ACQUIRED; // reentrant
        }
        if (!enqueue) {
            return Outcome.BUSY;
        }
        enqueueIfAbsent(lockName, lockKey, processId, stepExecutionId);
        return Outcome.ENQUEUED;
    }

    @Override
    public Optional<Grant> release(String lockName, String lockKey, String processId) {
        migrateLegacyRowIds();
        return transactionTemplate.execute(status -> releaseInTx(lockName, lockKey, processId));
    }

    @Override
    public List<Grant> releaseAll(String processId) {
        migrateLegacyRowIds();
        return transactionTemplate.execute(status -> {
            List<String[]> held = jdbcTemplate.query(
                    "SELECT lock_name, lock_key FROM process_lock WHERE holder_process_id = ?",
                    (rs, i) -> new String[]{rs.getString(1), rs.getString(2)}, processId);
            List<Grant> grants = new ArrayList<>();
            for (String[] lock : held) {
                releaseInTx(lock[0], lock[1], processId).ifPresent(grants::add);
            }
            // Drop this process from every queue it was waiting in.
            jdbcTemplate.update("DELETE FROM process_lock_waiter WHERE process_id = ?", processId);
            return grants;
        });
    }

    private Optional<Grant> releaseInTx(String lockName, String lockKey, String processId) {
        String id = rowId(lockName, lockKey);
        String holder = lockRowForUpdate(id);
        if (holder == null || !processId.equals(holder)) {
            return Optional.empty();
        }
        return reassignOrFree(id, lockName, lockKey);
    }

    @Override
    public List<Grant> expireLeases(LocalDateTime now) {
        migrateLegacyRowIds();
        return transactionTemplate.execute(status -> {
            var expired = jdbcTemplate.query(
                    "SELECT lock_name, lock_key FROM process_lock "
                            + "WHERE lease_deadline_at IS NOT NULL AND lease_deadline_at < ?",
                    (rs, i) -> new String[]{rs.getString(1), rs.getString(2)}, Timestamp.valueOf(now));
            List<Grant> grants = new ArrayList<>();
            for (String[] lock : expired) {
                String id = rowId(lock[0], lock[1]);
                // Re-lock and re-check the deadline: another pod may have released or renewed it
                // between the scan and here.
                if (lockRowForUpdate(id) == null || !leaseExpired(id, now)) {
                    continue;
                }
                reassignOrFree(id, lock[0], lock[1]).ifPresent(grants::add);
            }
            return grants;
        });
    }

    private boolean leaseExpired(String id, LocalDateTime now) {
        var deadline = jdbcTemplate.query(
                "SELECT lease_deadline_at FROM process_lock WHERE id = ?",
                (rs, i) -> rs.getTimestamp(1), id);
        return !deadline.isEmpty() && deadline.get(0) != null
                && deadline.get(0).toLocalDateTime().isBefore(now);
    }

    /** Hand the (already row-locked) lock to the earliest waiter, or free it if none wait. */
    private Optional<Grant> reassignOrFree(String id, String lockName, String lockKey) {
        List<String[]> next = jdbcTemplate.query(
                "SELECT id, process_id, step_execution_id FROM process_lock_waiter "
                        + "WHERE lock_name = ? AND lock_key = ? ORDER BY enqueued_at, id LIMIT 1",
                (rs, i) -> new String[]{rs.getString(1), rs.getString(2), rs.getString(3)},
                lockName, lockKey);
        if (!next.isEmpty()) {
            String waiterRowId = next.get(0)[0];
            String waiterProcess = next.get(0)[1];
            String waiterStep = next.get(0)[2];
            jdbcTemplate.update("DELETE FROM process_lock_waiter WHERE id = ?", waiterRowId);
            jdbcTemplate.update(
                    "UPDATE process_lock SET holder_process_id = ?, holder_step_execution_id = ?, "
                            + "acquired_at = ?, lease_deadline_at = ? WHERE id = ?",
                    waiterProcess, waiterStep, Timestamp.valueOf(LocalDateTime.now()), newLease(), id);
            return Optional.of(new Grant(lockName, lockKey, waiterProcess, waiterStep));
        }
        jdbcTemplate.update("DELETE FROM process_lock WHERE id = ?", id);
        return Optional.empty();
    }

    /**
     * Takes {@code SELECT … FOR UPDATE} on the lock row and returns its current holder, or null if
     * the row does not exist yet. The query timeout bounds the wait so a stuck holder never wedges
     * an acquirer forever.
     */
    private String lockRowForUpdate(String id) {
        return jdbcTemplate.execute((ConnectionCallback<String>) con -> {
            try (var ps = con.prepareStatement(
                    "SELECT holder_process_id FROM process_lock WHERE id = ? FOR UPDATE")) {
                ps.setQueryTimeout(waitTimeoutSeconds);
                ps.setString(1, id);
                try (var rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : null;
                }
            }
        });
    }

    private void enqueueIfAbsent(String lockName, String lockKey, String processId, String stepExecutionId) {
        Integer already = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM process_lock_waiter WHERE lock_name = ? AND lock_key = ? AND process_id = ?",
                Integer.class, lockName, lockKey, processId);
        if (already != null && already > 0) {
            return; // idempotent: this process is already queued for the key
        }
        jdbcTemplate.update(
                "INSERT INTO process_lock_waiter (id, lock_name, lock_key, process_id, step_execution_id, "
                        + "enqueued_at) VALUES (?, ?, ?, ?, ?, ?)",
                UUID.randomUUID().toString(), lockName, lockKey, processId, stepExecutionId,
                Timestamp.valueOf(LocalDateTime.now()));
    }
}
