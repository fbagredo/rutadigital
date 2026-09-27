package io.mateu.workflow.worker.kafka;

import io.mateu.workflow.ddd.DomainEvent;
import io.mateu.workflow.dtos.TraceContext;
import io.mateu.workflow.dtos.events.integration.TaskCancellationRequested;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import io.mateu.workflow.worker.CancelledTasks;
import io.mateu.workflow.worker.api.Cancellations;
import io.mateu.workflow.worker.api.TaskDispatcher;
import io.mateu.workflow.worker.api.TaskRegistry;
import io.mateu.workflow.worker.api.TaskTracing;
import io.mateu.workflow.worker.api.TaskReplySink;
import io.mateu.workflow.worker.api.TransactionAwareReplySink;
import io.mateu.workflow.worker.api.WorkerApiAutoConfiguration;
import io.mateu.workflow.worker.api.WorkerProperties;
import java.util.function.Consumer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.function.StreamOperations;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;

/**
 * The Kafka worker: it binds the {@code consumeWorkerEvent} consumer to the task topic, hands each
 * {@link TaskExecutionRequested} to the shared {@link TaskDispatcher}, and records each
 * {@link TaskCancellationRequested} so an in-flight or not-yet-started task is stopped. Replies go
 * back over {@link WorkerReplySink}. Everything transport-specific lives here; the dispatcher and
 * the handlers below it never see Kafka.
 *
 * <p><b>Commit semantics — at least once.</b> The consumer is imperative: the handler runs and its
 * reply is published (synchronously, see {@code WorkerReply}) on the listener thread, <em>before</em>
 * the consumer returns, and the binder commits the record's offset only after it returns. A crash
 * anywhere before the reply is published leaves the offset uncommitted, so the task is redelivered
 * (handlers must be idempotent). A reply the broker refuses surfaces as an exception, which the
 * binder retries ({@code max-attempts}) and then hands to its error handling — configure a DLQ
 * ({@code enable-dlq}) if a task must survive a prolonged broker outage. For parallelism use the
 * binding's {@code consumer.concurrency} (at most one thread per partition).
 *
 * <p>Bean names are namespaced ({@code eventconductor...}) and every bean backs off when the
 * application defines one of the same type, so an application bean named e.g. {@code taskDispatcher}
 * never clashes with the SDK's.
 */
@AutoConfiguration
@AutoConfigureAfter(WorkerApiAutoConfiguration.class)
@ConditionalOnClass(StreamOperations.class)
public class WorkerKafkaAutoConfiguration {

    @Bean("eventconductorCancelledTasks")
    @ConditionalOnMissingBean(CancelledTasks.class)
    public CancelledTasks cancelledTasks() {
        return new CancelledTasks();
    }

    @Bean("eventconductorWorkerCancellations")
    @ConditionalOnMissingBean(Cancellations.class)
    public Cancellations workerCancellations(CancelledTasks cancelledTasks) {
        return new CancelledTasksCancellations(cancelledTasks);
    }

    @Bean("eventconductorWorkerReplySink")
    @ConditionalOnMissingBean(TaskReplySink.class)
    public TaskReplySink workerReplySink(StreamOperations streamBridge) {
        return new TransactionAwareReplySink(new WorkerReplySink(streamBridge));
    }

    @Bean("eventconductorTaskDispatcher")
    @ConditionalOnMissingBean(TaskDispatcher.class)
    public TaskDispatcher taskDispatcher(TaskRegistry registry, TaskReplySink sink,
                                         Cancellations cancellations, WorkerProperties properties,
                                         ObjectProvider<TaskTracing> tracing) {
        return new TaskDispatcher(registry, sink, cancellations, properties.isStrict(),
                tracing.getIfAvailable(() -> TaskTracing.NOOP));
    }

    /**
     * The bound consumer. Its name is what {@link WorkerKafkaBindingDefaults} adds to
     * {@code spring.cloud.function.definition}, so the binding {@code consumeWorkerEvent-in-0} is the
     * task topic. It runs the task to its reply on the listener thread and only then returns, so the
     * offset is committed after the reply is published; a refused reply is rethrown so the record is
     * not committed.
     *
     * <p>Messages rather than bare payloads so the record's W3C trace headers — which the engine
     * puts on every task — reach the dispatcher, and the handler runs in the engine's trace.
     */
    @Bean
    public Consumer<Message<DomainEvent>> consumeWorkerEvent(TaskDispatcher dispatcher,
                                                             CancelledTasks cancelledTasks) {
        return message -> route(dispatcher, cancelledTasks, message.getPayload(),
                TraceContext.fromHeaders(message.getHeaders()));
    }

    private void route(TaskDispatcher dispatcher, CancelledTasks cancelledTasks, DomainEvent event,
                       TraceContext traceContext) {
        if (event instanceof TaskCancellationRequested cancellation) {
            cancelledTasks.accept(cancellation);
            return;
        }
        if (event instanceof TaskExecutionRequested task) {
            dispatcher.dispatch(task, traceContext);
        }
    }
}
