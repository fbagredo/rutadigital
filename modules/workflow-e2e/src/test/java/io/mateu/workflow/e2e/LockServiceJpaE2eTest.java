package io.mateu.workflow.e2e;

import io.mateu.workflow.application.out.LockService;
import io.mateu.workflow.application.out.LockService.Grant;
import io.mateu.workflow.application.out.LockService.Outcome;
import io.mateu.workflow.e2e.support.AbstractJpaE2eTest;
import org.junit.jupiter.api.Test;
import io.mateu.workflow.infra.out.persistence.JdbcLockService;
import io.mateu.workflow.infra.out.persistence.LockRowId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the JDBC {@link LockService} — {@code SELECT … FOR UPDATE}, the insert-and-retry on a
 * fresh key, the FIFO waiter promotion — actually runs on the real schema. It exercises
 * {@code JdbcLockService} against the H2 (PostgreSQL-compatibility) database the JPA suite boots,
 * with the lock tables created by {@code ddl-auto} from the entities, so it catches anything the
 * in-heap contract test cannot: the SQL, the portable DDL, the transaction boundaries.
 *
 * <p>Sequential by design — one waiter at a time — so it asserts admission without depending on the
 * tie-break between two waiters enqueued in the same timestamp tick. Multi-pod contention is P4.
 */
class LockServiceJpaE2eTest extends AbstractJpaE2eTest {

    private static final String LOCK = "booking";
    private static final String KEY = "B-1";

    @Autowired
    LockService locks;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void the_jdbc_lock_serializes_and_admits_fifo_over_the_real_schema() {
        // First acquirer takes it; a second process on the same key is parked.
        assertThat(locks.acquire(LOCK, KEY, "p1", null)).isEqualTo(Outcome.ACQUIRED);
        assertThat(locks.acquire(LOCK, KEY, "p2", null)).isEqualTo(Outcome.ENQUEUED);

        // Reentrant: the holder re-acquiring does not perturb the queue.
        assertThat(locks.acquire(LOCK, KEY, "p1", null)).isEqualTo(Outcome.ACQUIRED);

        // Releasing hands the lock to the waiter.
        Optional<Grant> toP2 = locks.release(LOCK, KEY, "p1");
        assertThat(toP2).map(Grant::processId).contains("p2");

        // A newcomer now queues behind p2, and gets it when p2 releases.
        assertThat(locks.acquire(LOCK, KEY, "p3", null)).isEqualTo(Outcome.ENQUEUED);
        assertThat(locks.release(LOCK, KEY, "p2")).map(Grant::processId).contains("p3");

        // Last holder releases with an empty queue: the key is free again.
        assertThat(locks.release(LOCK, KEY, "p3")).isEmpty();
        assertThat(locks.acquire(LOCK, KEY, "p4", null)).isEqualTo(Outcome.ACQUIRED);
    }

    @Test
    void releaseAll_frees_every_lock_a_process_holds() {
        locks.acquire("booking", "B-1", "p1", null);
        locks.acquire("payment", "B-1", "p1", null);
        locks.acquire("booking", "B-1", "p2", null); // p2 waits behind p1 on booking

        List<Grant> grants = locks.releaseAll("p1");

        // The booking lock is handed to p2; the payment lock had no waiter and is simply freed.
        assertThat(grants).extracting(Grant::processId).containsExactly("p2");
        assertThat(locks.acquire("payment", "B-1", "p3", null)).isEqualTo(Outcome.ACQUIRED);
    }

    @Test
    void the_row_id_is_the_printable_length_prefixed_encoding() {
        assertThat(locks.acquire("reservation", "MRU01/ABC123", "p1", null)).isEqualTo(Outcome.ACQUIRED);

        assertThat(jdbc.queryForList("SELECT id FROM process_lock WHERE lock_name = ? AND lock_key = ?",
                String.class, "reservation", "MRU01/ABC123"))
                .containsExactly("11:reservation:MRU01/ABC123");
        locks.release("reservation", "MRU01/ABC123", "p1");
    }

    @Test
    void a_lock_held_under_the_old_nul_separated_id_still_excludes_after_the_upgrade() {
        // What an H2 database kept from before the fix: the holder's row keyed lockName + NUL + lockKey.
        jdbc.update("INSERT INTO process_lock (id, lock_name, lock_key, holder_process_id, acquired_at, "
                        + "lease_deadline_at) VALUES (?, ?, ?, ?, ?, ?)",
                "legacy\u0000L-1", "legacy", "L-1", "old-holder",
                Timestamp.valueOf(LocalDateTime.now()), Timestamp.valueOf(LocalDateTime.now().plusHours(1)));

        // What each instance runs once before its first lock operation.
        assertThat(((JdbcLockService) locks).rewriteLegacyRowIds()).isEqualTo(1);

        assertThat(jdbc.queryForList("SELECT id FROM process_lock WHERE lock_name = 'legacy'", String.class))
                .containsExactly(LockRowId.of("legacy", "L-1"));
        // The rewritten row is found under the new id: a newcomer queues rather than taking it twice.
        assertThat(locks.acquire("legacy", "L-1", "newcomer", null)).isEqualTo(Outcome.ENQUEUED);
        assertThat(locks.release("legacy", "L-1", "old-holder")).map(Grant::processId).contains("newcomer");
        assertThat(locks.release("legacy", "L-1", "newcomer")).isEmpty();
    }
}
