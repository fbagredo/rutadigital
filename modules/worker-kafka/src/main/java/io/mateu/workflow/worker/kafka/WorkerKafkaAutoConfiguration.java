package io.mateu.workflow.worker.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.function.Function;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.function.StreamOperations;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * The Kafka worker: it binds the {@code consumeWorkerEvent} function to the task topic, hands each
 * {@link TaskExecutionRequested} to the shared {@link TaskDispatcher}, and records each
 * {@link TaskCancellationRequested} so an in-flight or not-yet-started task is stopped. Replies go
 * back over {@link WorkerReplySink}. Everything transport-specific lives here; the dispatcher and
 * the handlers below it never see Kafka.
 */
@AutoConfiguration
@AutoConfigureAfter(WorkerApiAutoConfiguration.class)
@ConditionalOnClass(StreamOperations.class)
public class WorkerKafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public CancelledTasks cancelledTasks() {
        return new CancelledTasks();
    }

    @Bean
    @ConditionalOnMissingBean(Cancellations.class)
    public Cancellations workerCancellations(CancelledTasks cancelledTasks) {
        return new CancelledTasksCancellations(cancelledTasks);
    }

    @Bean
    @ConditionalOnMissingBean(TaskReplySink.class)
    public TaskReplySink workerReplySink(StreamOperations streamBridge) {
        return new TransactionAwareReplySink(new WorkerReplySink(streamBridge));
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskDispatcher taskDispatcher(TaskRegistry registry, TaskReplySink sink,
                                         Cancellations cancellations, ObjectMapper objectMapper,
                                         WorkerProperties properties, ObjectProvider<TaskTracing> tracing) {
        return new TaskDispatcher(registry, sink, cancellations, objectMapper, properties.isStrict(),
                tracing.getIfAvailable(() -> TaskTracing.NOOP));
    }

    /**
     * The bound function. Its name is what {@link WorkerKafkaBindingDefaults} adds to
     * {@code spring.cloud.function.definition}, so the binding {@code consumeWorkerEvent-in-0} is the
     * task topic. Each task runs on a worker thread (the dispatcher is blocking); a refused reply
     * surfaces as an error so the record is not committed and is redelivered.
     *
     * <p>Messages rather than bare payloads so the record's W3C trace headers — which the engine
     * puts on every task — reach the dispatcher, and the handler runs in the engine's trace.
     */
    @Bean
    public Function<Flux<Message<DomainEvent>>, Mono<Void>> consumeWorkerEvent(TaskDispatcher dispatcher,
                                                                               CancelledTasks cancelledTasks) {
        return messages -> messages.flatMap(message -> route(dispatcher, cancelledTasks, message.getPayload(),
                TraceContext.fromHeaders(message.getHeaders()))).then();
    }

    private Mono<Void> route(TaskDispatcher dispatcher, CancelledTasks cancelledTasks, DomainEvent event,
                             TraceContext traceContext) {
        if (event instanceof TaskCancellationRequested cancellation) {
            cancelledTasks.accept(cancellation);
            return Mono.empty();
        }
        if (event instanceof TaskExecutionRequested task) {
            return Mono.<Void>fromRunnable(() -> dispatcher.dispatch(task, traceContext))
                    .subscribeOn(Schedulers.boundedElastic());
        }
        return Mono.empty();
    }
}
