package io.mateu.workflow.worker.embedded;

import io.mateu.workflow.application.out.EmbeddedTaskExecutor;
import io.mateu.workflow.application.usecases.stepexecution.update.UpdateStepExecutionUseCase;
import io.mateu.workflow.worker.api.Cancellations;
import io.mateu.workflow.worker.api.TaskDispatcher;
import io.mateu.workflow.worker.api.TaskRegistry;
import io.mateu.workflow.worker.api.TaskReplySink;
import io.mateu.workflow.worker.api.TaskTracing;
import io.mateu.workflow.worker.api.TransactionAwareReplySink;
import io.mateu.workflow.worker.api.WorkerApiAutoConfiguration;
import io.mateu.workflow.worker.api.WorkerProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * The embedded worker: it supplies the engine's {@link EmbeddedTaskExecutor} port with one that
 * hands every task to the shared {@link TaskDispatcher} and answers back through
 * {@link UpdateStepExecutionUseCase}. The same {@code TaskHandler}s a developer writes run here
 * unchanged; only the transport differs from the Kafka worker.
 *
 * <p>Active only when the engine is on the classpath and {@code workflow.mode} is embedded (its
 * default). Cancellations are never delivered to an embedded worker — the engine manages timeouts
 * and cancellation itself — so {@link Cancellations#NONE} is used.
 */
@AutoConfiguration
@AutoConfigureAfter(WorkerApiAutoConfiguration.class)
@ConditionalOnClass(UpdateStepExecutionUseCase.class)
@ConditionalOnProperty(name = "workflow.mode", havingValue = "embedded", matchIfMissing = true)
public class WorkerEmbeddedAutoConfiguration {

    @Bean("eventconductorEmbeddedReplySink")
    @ConditionalOnMissingBean(TaskReplySink.class)
    public TaskReplySink embeddedReplySink(UpdateStepExecutionUseCase updateStepExecution) {
        return new TransactionAwareReplySink(new UpdateStepExecutionSink(updateStepExecution));
    }

    @Bean("eventconductorEmbeddedCancellations")
    @ConditionalOnMissingBean(Cancellations.class)
    public Cancellations embeddedCancellations() {
        return Cancellations.NONE;
    }

    /**
     * Namespaced bean name, and backs off for any {@link TaskDispatcher} the application defines: an
     * application bean called {@code taskDispatcher} of its own type must not clash with this one.
     */
    @Bean("eventconductorTaskDispatcher")
    @ConditionalOnMissingBean(TaskDispatcher.class)
    public TaskDispatcher taskDispatcher(TaskRegistry registry, TaskReplySink sink,
                                         Cancellations cancellations, WorkerProperties properties) {
        return new TaskDispatcher(registry, sink, cancellations, properties.isStrict(), TaskTracing.NOOP);
    }

    @Bean("eventconductorEmbeddedTaskExecutor")
    @ConditionalOnMissingBean(EmbeddedTaskExecutor.class)
    public EmbeddedTaskExecutor dispatchingTaskExecutor(TaskDispatcher dispatcher) {
        return dispatcher::dispatch;
    }
}
