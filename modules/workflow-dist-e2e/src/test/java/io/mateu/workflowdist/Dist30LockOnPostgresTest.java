package io.mateu.workflowdist;

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
    static void startPods() {
        DistInfra.ensureWorkerStarted();
        pod = DistInfra.startOrchestrator(Map.of());
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
}
