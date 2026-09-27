package io.mateu.workflow.worker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.mateu.workflow.ddd.DomainEvent;
import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.dtos.events.integration.TaskCancellationRequested;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import io.mateu.workflow.dtos.events.integration.TaskStatus;
import io.mateu.workflow.dtos.events.integration.TaskStatusChanged;
import io.mateu.workflow.worker.CancelledTasks;
import io.mateu.workflow.worker.api.Cancellations;
import io.mateu.workflow.worker.api.TaskDispatcher;
import io.mateu.workflow.worker.api.TaskHandler;
import io.mateu.workflow.worker.api.TaskRegistration;
import io.mateu.workflow.worker.api.TaskRegistry;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class WorkerKafkaFunctionTest {

    record In(int qty) {}

    record Out(int total) {}

    private final RecordingStreamOperations bridge = new RecordingStreamOperations();
    private final CancelledTasks cancelledTasks = new CancelledTasks();

    private final java.util.List<io.mateu.workflow.dtos.TraceContext> tracedWith = new java.util.ArrayList<>();
    private final java.util.List<io.mateu.workflow.dtos.TraceContext> handlerSaw = new java.util.ArrayList<>();

    private static org.springframework.messaging.Message<DomainEvent> message(DomainEvent event) {
        return org.springframework.messaging.support.MessageBuilder.withPayload(event).build();
    }

    private Consumer<org.springframework.messaging.Message<DomainEvent>> function() {
        TaskHandler<In, Out> handler = (in, ctx) -> {
            handlerSaw.add(ctx.traceContext());
            return new Out(in.qty() * 10);
        };
        var registry = new TaskRegistry(List.of(
                new TaskRegistration<>("place-order", 1, "t", In.class, Out.class, handler)));
        Cancellations cancellations = new CancelledTasksCancellations(cancelledTasks);
        io.mateu.workflow.worker.api.TaskTracing tracing = (task, context, work) -> {
            tracedWith.add(context);
            work.run();
        };
        var dispatcher = new TaskDispatcher(registry, new WorkerReplySink(bridge), cancellations,
                false, tracing);
        return new WorkerKafkaAutoConfiguration().consumeWorkerEvent(dispatcher, cancelledTasks);
    }

    @Test
    void it_runs_a_task_and_replies_completed() {
        var task = new TaskExecutionRequested("tx-1", "p-1", "wf", "step", "place-order@1",
                List.of(new Variable("qty", "3")));

        function().accept(message(task));

        var completed = bridge.sent.stream()
                .map(RecordingStreamOperations.Sent::payload)
                .filter(TaskStatusChanged.class::isInstance)
                .map(TaskStatusChanged.class::cast)
                .filter(r -> r.status() == TaskStatus.COMPLETED)
                .findFirst()
                .orElseThrow();
        assertThat(completed.variables()).containsExactly(new Variable("total", "30"));
    }

    @Test
    void it_records_a_cancellation_and_does_not_treat_it_as_a_task() {
        var cancellation = new TaskCancellationRequested("other-tx");

        function().accept(message(cancellation));

        assertThat(cancelledTasks.isCancelled("other-tx")).isTrue();
        assertThat(bridge.sent).isEmpty();
    }

    @Test
    void it_runs_the_task_in_the_trace_its_record_headers_carry() {
        var traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        var task = new TaskExecutionRequested("tx-2", "p-2", "wf", "step", "place-order@1",
                List.of(new Variable("qty", "1")));
        // As the Kafka binder delivers an unmapped header: raw bytes.
        var record = org.springframework.messaging.support.MessageBuilder.<DomainEvent>withPayload(task)
                .setHeader("traceparent", traceparent.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .setHeader("baggage", "tenant=mru01".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .build();

        function().accept(record);

        assertThat(tracedWith).singleElement().satisfies(context -> {
            assertThat(context.traceparent()).isEqualTo(traceparent);
            assertThat(context.baggage()).isEqualTo("tenant=mru01");
        });
        assertThat(handlerSaw).singleElement().isEqualTo(tracedWith.get(0));
    }

    @Test
    void a_task_without_trace_headers_runs_with_no_context() {
        var task = new TaskExecutionRequested("tx-3", "p-3", "wf", "step", "place-order@1",
                List.of(new Variable("qty", "1")));

        function().accept(message(task));

        assertThat(tracedWith).containsExactly((io.mateu.workflow.dtos.TraceContext) null);
        assertThat(handlerSaw).containsExactly((io.mateu.workflow.dtos.TraceContext) null);
    }

    @Test
    void the_reply_is_published_on_the_listener_thread_before_the_consumer_returns() {
        // The binder commits a record's offset once the consumer returns. So the whole task — the
        // handler and its reply — must have happened, on this thread, by then: a crash before the
        // reply leaves the offset uncommitted and the task is redelivered instead of lost.
        var listenerThread = Thread.currentThread();
        var handlerThread = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var replyThread = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var bridge = new RecordingStreamOperations() {
            @Override
            public boolean send(String bindingName, Object data) {
                replyThread.set(Thread.currentThread());
                return super.send(bindingName, data);
            }
        };
        TaskHandler<In, Out> handler = (in, ctx) -> {
            handlerThread.set(Thread.currentThread());
            return new Out(in.qty());
        };
        var registry = new TaskRegistry(List.of(
                new TaskRegistration<>("place-order", 1, "t", In.class, Out.class, handler)));
        var dispatcher = new TaskDispatcher(registry, new WorkerReplySink(bridge),
                new CancelledTasksCancellations(cancelledTasks), false, null);
        var consumer = new WorkerKafkaAutoConfiguration().consumeWorkerEvent(dispatcher, cancelledTasks);

        consumer.accept(message(new TaskExecutionRequested("tx-4", "p-4", "wf", "step", "place-order@1",
                List.of(new Variable("qty", "2")))));

        // Returned → the reply is already out; nothing is left running elsewhere.
        assertThat(bridge.sent).extracting(RecordingStreamOperations.Sent::payload)
                .anyMatch(p -> p instanceof TaskStatusChanged r && r.status() == TaskStatus.COMPLETED);
        assertThat(handlerThread.get()).isSameAs(listenerThread);
        assertThat(replyThread.get()).isSameAs(listenerThread);
    }

    @Test
    void a_refused_reply_fails_the_consumer_so_the_offset_is_not_committed() {
        TaskHandler<In, Out> handler = (in, ctx) -> new Out(1);
        var registry = new TaskRegistry(List.of(
                new TaskRegistration<>("place-order", 1, "t", In.class, Out.class, handler)));
        var refusing = new RecordingStreamOperations();
        refusing.accept = false;
        var dispatcher = new TaskDispatcher(registry, new WorkerReplySink(refusing),
                new CancelledTasksCancellations(cancelledTasks), false, null);
        var consumer = new WorkerKafkaAutoConfiguration().consumeWorkerEvent(dispatcher, cancelledTasks);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> consumer.accept(message(
                        new TaskExecutionRequested("tx-5", "p-5", "wf", "step", "place-order@1", List.of()))))
                .isInstanceOf(io.mateu.workflow.worker.api.ReplyNotAcceptedException.class);
    }
}
