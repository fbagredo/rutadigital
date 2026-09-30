package io.mateu.workflowdist;

import io.mateu.workflow.application.usecases.directoryimport.ImportWorkflowDefinitionsFromDirectoryUseCase;
import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import io.mateu.workflowdist.support.AbstractDistTest;
import io.mateu.workflowdist.support.DistInfra;
import io.mateu.workflowdist.support.WorkerStub;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DIST-30 — the per-key serialization lock on PostgreSQL. LOCK/UNLOCK and {@code processLock} both
 * shipped tested only on H2, whose text columns take a NUL; PostgreSQL's refuse one, and the lock row
 * id joined name and key with exactly that, so the first acquire on a real deployment failed. Here
 * both run end to end on the real database, with a key shaped like the ones production uses
 * ({@code MRU01/ABC123}): a second process on the same key waits, is admitted when the first lets go,
 * and the lock table is empty afterwards.
 */
class Dist30LockOnPostgresTest extends AbstractDistTest {

    static ConfigurableApplicationContext pod;

    @BeforeAll
    static void startPods() throws Exception {
        DistInfra.ensureWorkerStarted();
        pod = DistInfra.startOrchestrator(Map.of());
        // Imported here rather than dropped under workflows/: every classpath definition costs every
        // pod's boot a database round trip, and DIST-08 times a boot with the database paused.
        var dir = Files.createTempDirectory("dist30");
        for (var name : List.of("dist-lock.json", "dist-process-lock.json")) {
            try (var in = Dist30LockOnPostgresTest.class.getResourceAsStream("/dist30/" + name)) {
                Files.copy(in, dir.resolve(name));
            }
        }
        var result = pod.getBean(ImportWorkflowDefinitionsFromDirectoryUseCase.class).handle(List.of(dir.toString()));
        assertThat(result.errors()).isEmpty();
    }

    @AfterAll
    static void stopPods() {
        pod.close();
    }

    private static Variable booking(String id) {
        return new Variable("bookingId", id);
    }

    private TaskExecutionRequested awaitTask(String businessKey, String stepId) {
        return Awaitility.await(stepId + " of " + businessKey + " reaches the worker")
                .atMost(DEFAULT_TIMEOUT).pollInterval(Duration.ofMillis(250))
                .until(() -> processStatus(businessKey).isPresent()
                                ? WorkerStub.receivedFor(processId(businessKey)).stream()
                                        .filter(r -> r.stepId().equals(stepId)).findFirst().orElse(null)
                                : null,
                        r -> r != null);
    }

    private void awaitStepStatus(String businessKey, String stepId, String status) {
        Awaitility.await(stepId + " of " + businessKey + " reaches " + status)
                .atMost(DEFAULT_TIMEOUT).pollInterval(Duration.ofMillis(250))
                .until(() -> status.equals(stepStatuses(businessKey).get(stepId)));
    }

    private List<String> lockRowIds(String lockName, String lockKey) {
        return DistInfra.jdbc().queryForList(
                "SELECT id FROM process_lock WHERE lock_name = ? AND lock_key = ?", String.class, lockName, lockKey);
    }

    @Test
    void lockAndUnlockStepsSerializeTwoProcessesOnTheSameKey() throws Exception {
        WorkerStub.on("dist-lock", "work", WorkerStub::silent);

        createProcess("dist-lock", "dist30-a", booking("MRU01/ABC123"));
        var aWork = awaitTask("dist30-a", "work");
        assertThat(lockRowIds("booking", "MRU01/ABC123")).containsExactly("7:booking:MRU01/ABC123");

        createProcess("dist-lock", "dist30-b", booking("MRU01/ABC123"));
        awaitStepStatus("dist30-b", "lock", "WAITING_ON_LOCK");
        Thread.sleep(1_000);
        assertThat(WorkerStub.receivedFor(processId("dist30-b")))
                .as("B's critical section must not run while A holds the lock").isEmpty();

        WorkerStub.complete(aWork);
        awaitProcessCompleted("dist30-a");
        var bWork = awaitTask("dist30-b", "work");
        assertThat(stepStatuses("dist30-b").get("lock")).isEqualTo("COMPLETED");

        WorkerStub.complete(bWork);
        awaitProcessCompleted("dist30-b");
        assertThat(lockRowIds("booking", "MRU01/ABC123")).isEmpty();
    }

    @Test
    void aProcessLockSerializesWholeInstancesOnTheSameKey() throws Exception {
        WorkerStub.on("dist-process-lock", "pwork", WorkerStub::silent);

        createProcess("dist-process-lock", "dist30-pa", booking("MRU01/XYZ789"));
        var aWork = awaitTask("dist30-pa", "pwork");
        assertThat(lockRowIds("reservation", "MRU01/XYZ789")).containsExactly("11:reservation:MRU01/XYZ789");

        createProcess("dist-process-lock", "dist30-pb", booking("MRU01/XYZ789"));
        Awaitility.await("dist30-pb is created").atMost(DEFAULT_TIMEOUT)
                .until(() -> processStatus("dist30-pb").isPresent());
        Thread.sleep(2_000);
        assertThat(WorkerStub.receivedFor(processId("dist30-pb")))
                .as("B must not run at all while A holds the process lock").isEmpty();
        assertThat(DistInfra.jdbc().queryForObject(
                "SELECT count(*) FROM process_lock_waiter WHERE lock_name = ? AND lock_key = ?",
                Integer.class, "reservation", "MRU01/XYZ789")).isEqualTo(1);

        WorkerStub.complete(aWork);
        awaitProcessCompleted("dist30-pa");
        var bWork = awaitTask("dist30-pb", "pwork");

        WorkerStub.complete(bWork);
        awaitProcessCompleted("dist30-pb");
        assertThat(lockRowIds("reservation", "MRU01/XYZ789")).isEmpty();
    }

    /**
     * Two processes taking a free key at the same instant (ec-demo1: a check-in and the charge of its
     * extra, started by the same desk action) race on the lock row's INSERT. The loser used to retry
     * in the transaction its failed INSERT had already aborted on PostgreSQL (25P02), and the step's
     * event was parked on the dead-letter topic, the process PENDING for good (2.23.3 retried in a
     * "new" transaction, which joined the caller's: still aborted). Here the winner's
     * INSERT is held uncommitted while the loser acquires: the loser's INSERT waits on it, fails when
     * it commits, and the loser must end up queued behind the winner.
     */
    @Test
    void theLoserOfTheFirstInsertOfAFreeKeyIsQueuedNotBroken() throws Exception {
        var locks = pod.getBean(io.mateu.workflow.infra.out.persistence.JdbcLockService.class);
        var dataSource = ((org.springframework.jdbc.datasource.DriverManagerDataSource) DistInfra.jdbc().getDataSource());
        try (var winner = dataSource.getConnection()) {
            winner.setAutoCommit(false);
            try (var insert = winner.prepareStatement("INSERT INTO process_lock (id, lock_name, lock_key, holder_process_id, "
                    + "holder_step_execution_id, acquired_at, lease_deadline_at) VALUES (?, ?, ?, ?, ?, now(), now() + interval '15 minutes')")) {
                insert.setString(1, io.mateu.workflow.infra.out.persistence.LockRowId.of("reservation", "MRU01/RACE01"));
                insert.setString(2, "reservation");
                insert.setString(3, "MRU01/RACE01");
                insert.setString(4, "winner");
                insert.setString(5, "winner-step");
                insert.executeUpdate();
            }
            // As the engine acquires it: inside the transaction of the step over the process (the
            // partition-owned process lock's), so a failed INSERT would abort that one.
            var outer = new org.springframework.transaction.support.TransactionTemplate(
                    pod.getBean(org.springframework.transaction.PlatformTransactionManager.class));
            var loser = java.util.concurrent.CompletableFuture.supplyAsync(() -> outer.execute(status -> {
                var outcome = locks.acquire("reservation", "MRU01/RACE01", "loser", "loser-step");
                // The transaction is still usable afterwards: the step goes on writing in it.
                assertThat(pod.getBean(org.springframework.jdbc.core.JdbcTemplate.class)
                        .queryForObject("SELECT count(*) FROM process_lock_waiter", Integer.class)).isNotNull();
                return outcome;
            }));
            Thread.sleep(1_000);
            assertThat(loser).as("the loser waits on the winner's uncommitted row").isNotDone();
            winner.commit();

            assertThat(loser.get(15, java.util.concurrent.TimeUnit.SECONDS))
                    .isEqualTo(io.mateu.workflow.application.out.LockService.Outcome.ENQUEUED);
        }
        assertThat(DistInfra.jdbc().queryForObject(
                "SELECT holder_process_id FROM process_lock WHERE lock_name = ? AND lock_key = ?", String.class,
                "reservation", "MRU01/RACE01")).isEqualTo("winner");
        assertThat(DistInfra.jdbc().queryForList(
                "SELECT process_id FROM process_lock_waiter WHERE lock_name = ? AND lock_key = ?", String.class,
                "reservation", "MRU01/RACE01")).containsExactly("loser");
        DistInfra.jdbc().update("DELETE FROM process_lock_waiter WHERE lock_key = 'MRU01/RACE01'");
        DistInfra.jdbc().update("DELETE FROM process_lock WHERE lock_key = 'MRU01/RACE01'");
    }
}
