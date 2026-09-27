package io.mateu.workflow.worker.embedded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.mateu.workflow.application.out.EmbeddedTaskExecutor;
import io.mateu.workflow.application.usecases.stepexecution.update.UpdateStepExecutionCommand;
import io.mateu.workflow.application.usecases.stepexecution.update.UpdateStepExecutionUseCase;
import io.mateu.workflow.domain.aggregates.StepExecutionStatus;
import io.mateu.workflow.domain.aggregates.Variable;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import io.mateu.workflow.worker.api.TaskHandler;
import io.mateu.workflow.worker.api.TaskRegistration;
import io.mateu.workflow.worker.api.TaskRegistry;
import io.mateu.workflow.worker.api.WorkerProperties;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The whole embedded path: engine calls execute(), the handler runs, the engine is called back. */
class EmbeddedTaskExecutorTest {

    record In(int qty) {}

    record Out(int total) {}

    private final UpdateStepExecutionUseCase useCase = mock(UpdateStepExecutionUseCase.class);

    private EmbeddedTaskExecutor executor(TaskHandler<In, Out> handler) {
        var config = new WorkerEmbeddedAutoConfiguration();
        var sink = config.embeddedReplySink(useCase);
        var registry = new TaskRegistry(List.of(
                new TaskRegistration<>("place-order", 1, "t", In.class, Out.class, handler)));
        var dispatcher = config.taskDispatcher(registry, sink, config.embeddedCancellations(),
                new WorkerProperties());
        return config.dispatchingTaskExecutor(dispatcher);
    }

    @Test
    void a_dispatched_task_runs_and_completes_the_step() {
        var task = new TaskExecutionRequested("tx-9", "p-9", "wf", "step", "place-order@1",
                List.of(new io.mateu.workflow.dtos.Variable("qty", "4")));

        executor((in, ctx) -> new Out(in.qty() * 10)).execute(task);

        var captor = ArgumentCaptor.forClass(UpdateStepExecutionCommand.class);
        verify(useCase).handle(captor.capture());
        var command = captor.getValue();
        assertThat(command.stepId()).isEqualTo("tx-9");
        assertThat(command.status()).isEqualTo(StepExecutionStatus.COMPLETED);
        assertThat(command.variables()).containsExactly(new Variable("total", "40"));
    }

    @Test
    void a_business_failure_errors_the_step() {
        var task = new TaskExecutionRequested("tx-10", "p-10", "wf", "step", "place-order@1", List.of());

        executor((in, ctx) -> {
            throw new io.mateu.workflow.worker.api.TaskFailure("OUT_OF_STOCK");
        }).execute(task);

        var captor = ArgumentCaptor.forClass(UpdateStepExecutionCommand.class);
        verify(useCase).handle(captor.capture());
        assertThat(captor.getValue().status()).isEqualTo(StepExecutionStatus.ERROR);
        assertThat(captor.getValue().log()).isEqualTo("OUT_OF_STOCK");
    }
}
