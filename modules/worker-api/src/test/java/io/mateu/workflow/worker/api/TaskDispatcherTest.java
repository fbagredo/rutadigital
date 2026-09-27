package io.mateu.workflow.worker.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.dtos.events.integration.TaskExecutionRequested;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TaskDispatcherTest {

    record OrderInput(String ref, int qty, boolean express) {}

    record OrderOutput(String status, int total) {}

    static TaskExecutionRequested request(String taskId, List<Variable> variables) {
        return new TaskExecutionRequested("tx-1", "p-1", "wf-1", "step-a", taskId, variables);
    }

    static TaskRegistration<OrderInput, OrderOutput> registration(TaskHandler<OrderInput, OrderOutput> handler) {
        return new TaskRegistration<>("place-order", 1, "place-order.v1", OrderInput.class, OrderOutput.class, handler);
    }

    /** Records what the dispatcher reported so a test can assert the outcome. */
    static final class RecordingSink implements TaskReplySink {
        final List<TaskExecutionRequested> running = new ArrayList<>();
        final List<List<Variable>> completed = new ArrayList<>();
        final List<String> failed = new ArrayList<>();

        public void running(TaskExecutionRequested task) {
            running.add(task);
        }

        public void completed(TaskExecutionRequested task, List<Variable> variables) {
            completed.add(variables);
        }

        public void failed(TaskExecutionRequested task, List<Variable> variables, String reason) {
            failed.add(reason);
        }
    }

    static final class ProgrammableCancellations implements Cancellations {
        final List<Boolean> claims = new ArrayList<>();
        boolean cancelled = false;
        private final AtomicInteger claimCall = new AtomicInteger();

        ProgrammableCancellations(Boolean... perClaim) {
            claims.addAll(List.of(perClaim));
        }

        public boolean claim(String taskExecutionId) {
            int i = claimCall.getAndIncrement();
            return i < claims.size() && claims.get(i);
        }

        public boolean isCancelled(String taskExecutionId) {
            return cancelled;
        }
    }

    private TaskDispatcher dispatcher(TaskRegistry registry, TaskReplySink sink, Cancellations cancellations,
                                      boolean strict) {
        return new TaskDispatcher(registry, sink, cancellations, strict, null);
    }

    @Test
    void completes_with_bound_input_and_output_variables() {
        var handler = (TaskHandler<OrderInput, OrderOutput>) (in, ctx) ->
                new OrderOutput(in.express() ? "express" : "standard", in.qty() * 10);
        var registry = new TaskRegistry(List.of(registration(handler)));
        var sink = new RecordingSink();

        dispatcher(registry, sink, Cancellations.NONE, false).dispatch(request("place-order@1", List.of(
                new Variable("ref", "A-100"),
                new Variable("qty", "3"),
                new Variable("express", "true"))));

        assertThat(sink.failed).isEmpty();
        assertThat(sink.completed).hasSize(1);
        var out = sink.completed.get(0).stream().collect(
                java.util.stream.Collectors.toMap(Variable::name, Variable::value));
        assertThat(out).containsEntry("status", "express").containsEntry("total", "30");
    }

    @Test
    void keeps_a_plain_string_a_string_and_reads_json_shapes() {
        record Shapes(String name, Map<String, Object> meta, List<Integer> tags) {}
        var seen = new Object() { Shapes value; };
        TaskHandler<Shapes, Void> handler = (in, ctx) -> {
            seen.value = in;
            return null;
        };
        var registry = new TaskRegistry(List.of(
                new TaskRegistration<>("shape", 1, "shape.v1", Shapes.class, Void.class, handler)));

        dispatcher(registry, new RecordingSink(), Cancellations.NONE, false).dispatch(request("shape@1", List.of(
                new Variable("name", "not-json at all"),
                new Variable("meta", "{\"a\":1}"),
                new Variable("tags", "[1,2,3]"))));

        assertThat(seen.value.name()).isEqualTo("not-json at all");
        assertThat(seen.value.meta()).containsEntry("a", 1);
        assertThat(seen.value.tags()).containsExactly(1, 2, 3);
    }

    @Test
    void business_failure_is_reported_with_its_code() {
        TaskHandler<OrderInput, OrderOutput> handler = (in, ctx) -> {
            throw new TaskFailure("OUT_OF_STOCK");
        };
        var registry = new TaskRegistry(List.of(registration(handler)));
        var sink = new RecordingSink();

        dispatcher(registry, sink, Cancellations.NONE, false).dispatch(request("place-order@1", List.of()));

        assertThat(sink.completed).isEmpty();
        assertThat(sink.failed).containsExactly("OUT_OF_STOCK");
    }

    @Test
    void unexpected_exception_is_reported_as_a_failure() {
        TaskHandler<OrderInput, OrderOutput> handler = (in, ctx) -> {
            throw new IllegalStateException("boom");
        };
        var registry = new TaskRegistry(List.of(registration(handler)));
        var sink = new RecordingSink();

        dispatcher(registry, sink, Cancellations.NONE, false).dispatch(request("place-order@1", List.of()));

        assertThat(sink.failed).hasSize(1);
        assertThat(sink.failed.get(0)).contains("boom");
    }

    @Test
    void an_input_that_cannot_be_bound_fails_the_task() {
        TaskHandler<OrderInput, OrderOutput> handler = (in, ctx) -> new OrderOutput("ok", 0);
        var registry = new TaskRegistry(List.of(registration(handler)));
        var sink = new RecordingSink();

        dispatcher(registry, sink, Cancellations.NONE, false).dispatch(request("place-order@1", List.of(
                new Variable("qty", "\"not a number\""))));

        assertThat(sink.completed).isEmpty();
        assertThat(sink.failed).hasSize(1);
        assertThat(sink.failed.get(0)).contains("OrderInput");
    }

    @Test
    void a_cancellation_before_start_stops_the_task_silently() {
        var invoked = new AtomicInteger();
        TaskHandler<OrderInput, OrderOutput> handler = (in, ctx) -> {
            invoked.incrementAndGet();
            return new OrderOutput("ok", 0);
        };
        var registry = new TaskRegistry(List.of(registration(handler)));
        var sink = new RecordingSink();

        dispatcher(registry, sink, new ProgrammableCancellations(true), false)
                .dispatch(request("place-order@1", List.of()));

        assertThat(invoked).hasValue(0);
        assertThat(sink.completed).isEmpty();
        assertThat(sink.failed).isEmpty();
    }

    @Test
    void a_cancellation_during_the_task_suppresses_the_completion() {
        TaskHandler<OrderInput, OrderOutput> handler = (in, ctx) -> new OrderOutput("ok", 1);
        var registry = new TaskRegistry(List.of(registration(handler)));
        var sink = new RecordingSink();

        // false before start, true before reply
        dispatcher(registry, sink, new ProgrammableCancellations(false, true), false)
                .dispatch(request("place-order@1", List.of()));

        assertThat(sink.completed).isEmpty();
        assertThat(sink.failed).isEmpty();
    }

    @Test
    void an_unknown_task_is_ignored_unless_strict() {
        var registry = new TaskRegistry(List.of());
        var lenient = new RecordingSink();
        dispatcher(registry, lenient, Cancellations.NONE, false).dispatch(request("ghost@1", List.of()));
        assertThat(lenient.failed).isEmpty();

        var strict = new RecordingSink();
        dispatcher(registry, strict, Cancellations.NONE, true).dispatch(request("ghost@1", List.of()));
        assertThat(strict.failed).hasSize(1);
        assertThat(strict.failed.get(0)).contains("ghost@1");
    }

    @Test
    void a_refused_reply_propagates_for_redelivery() {
        TaskHandler<OrderInput, OrderOutput> handler = (in, ctx) -> new OrderOutput("ok", 1);
        var registry = new TaskRegistry(List.of(registration(handler)));
        TaskReplySink refusing = new TaskReplySink() {
            public void running(TaskExecutionRequested task) {}

            public void completed(TaskExecutionRequested task, List<Variable> variables) {
                throw new ReplyNotAcceptedException("broker down");
            }

            public void failed(TaskExecutionRequested task, List<Variable> variables, String reason) {}
        };

        assertThatThrownBy(() -> dispatcher(registry, refusing, Cancellations.NONE, false)
                .dispatch(request("place-order@1", List.of())))
                .isInstanceOf(ReplyNotAcceptedException.class);
    }

    @Test
    void the_context_exposes_the_task_and_progress_reports_running() {
        var seen = new Object() {
            String pid;
            String step;
            boolean cancelled;
        };
        TaskHandler<OrderInput, OrderOutput> handler = (in, ctx) -> {
            seen.pid = ctx.processId();
            seen.step = ctx.stepId();
            seen.cancelled = ctx.isCancelled();
            ctx.progress("halfway");
            return new OrderOutput("ok", 1);
        };
        var registry = new TaskRegistry(List.of(registration(handler)));
        var sink = new RecordingSink();

        dispatcher(registry, sink, Cancellations.NONE, false).dispatch(request("place-order@1", List.of()));

        assertThat(seen.pid).isEqualTo("p-1");
        assertThat(seen.step).isEqualTo("step-a");
        assertThat(seen.cancelled).isFalse();
        assertThat(sink.running).hasSize(1);
        assertThat(sink.completed).hasSize(1);
    }

    @Test
    void resolution_by_contract_id_with_or_without_version_and_step_id_fallback() {
        TaskHandler<OrderInput, OrderOutput> handler = (in, ctx) -> new OrderOutput("ok", 1);
        var v1 = new TaskRegistration<>("place-order", 1, "t", OrderInput.class, OrderOutput.class, handler);
        var v2 = new TaskRegistration<>("place-order", 2, "t", OrderInput.class, OrderOutput.class, handler);
        var registry = new TaskRegistry(List.of(v2, v1));

        // 1. taskId with a version: exactly that registration.
        assertThat(registry.resolve("place-order@1", "whatever")).isSameAs(v1);
        assertThat(registry.resolve("place-order@2", "whatever")).isSameAs(v2);
        // ...and a version not served is never swapped for another one.
        assertThat(registry.resolve("place-order@3", "place-order")).isNull();
        // 2. taskId without a version: the highest registered version.
        assertThat(registry.resolve("place-order", "whatever")).isSameAs(v2);
        // 3. Blank taskId (an ACTION with no `task:`): the stepId, as a contract id or a ref.
        assertThat(registry.resolve("", "place-order")).isSameAs(v2);
        assertThat(registry.resolve(null, "place-order@1")).isSameAs(v1);
        // A taskId that is set but not served does not fall back to the stepId.
        assertThat(registry.resolve("ghost@1", "place-order")).isNull();
        assertThat(registry.resolve("", "step-x")).isNull();
        assertThat(registry.resolve("", null)).isNull();

        assertThatThrownBy(() -> new TaskRegistry(List.of(v1, v1)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_step_without_a_task_reference_is_served_by_the_handler_named_like_the_step() {
        var registry = new TaskRegistry(List.of(new TaskRegistration<>("place-order", 1, "t",
                OrderInput.class, OrderOutput.class, (in, ctx) -> new OrderOutput("by-step", in.qty()))));
        var sink = new RecordingSink();

        dispatcher(registry, sink, Cancellations.NONE, true).dispatch(new TaskExecutionRequested(
                "tx-1", "p-1", "wf-1", "place-order", "", List.of(new Variable("qty", "2"))));

        assertThat(sink.failed).isEmpty();
        assertThat(sink.completed).hasSize(1);
    }

    record Booking(String locator, String code, Long nights, java.math.BigDecimal amount, Boolean vip,
                   java.time.LocalDate arrival, java.time.LocalDateTime createdAt,
                   com.fasterxml.jackson.databind.JsonNode extra, List<String> rooms, Object anything) {}

    @Test
    void values_are_typed_by_the_declared_input_type_not_by_how_they_look() {
        var seen = new Object() { Booking value; };
        TaskHandler<Booking, Void> handler = (in, ctx) -> {
            seen.value = in;
            return null;
        };
        var registry = new TaskRegistry(List.of(
                new TaskRegistration<>("book", 1, "t", Booking.class, Void.class, handler)));
        var sink = new RecordingSink();

        dispatcher(registry, sink, Cancellations.NONE, false).dispatch(request("book@1", List.of(
                new Variable("locator", "12E45"),        // a string that looks like a number
                new Variable("code", "007"),
                new Variable("nights", "3"),
                new Variable("amount", "12.50"),
                new Variable("vip", "true"),
                new Variable("arrival", "2026-09-27"),
                new Variable("createdAt", "2026-09-27T10:15:30"),
                new Variable("extra", "{\"a\":1}"),
                new Variable("rooms", "[\"101\",\"102\"]"),
                new Variable("anything", "42"),          // no declared type: the raw string
                new Variable("notInTheContract", "x")))); // another process variable: ignored

        assertThat(sink.failed).isEmpty();
        var b = seen.value;
        assertThat(b.locator()).isEqualTo("12E45");
        assertThat(b.code()).isEqualTo("007");
        assertThat(b.nights()).isEqualTo(3L);
        assertThat(b.amount()).isEqualByComparingTo("12.50");
        assertThat(b.vip()).isTrue();
        assertThat(b.arrival()).isEqualTo(java.time.LocalDate.of(2026, 9, 27));
        assertThat(b.createdAt()).isEqualTo(java.time.LocalDateTime.of(2026, 9, 27, 10, 15, 30));
        assertThat(b.extra().get("a").asInt()).isEqualTo(1);
        assertThat(b.rooms()).containsExactly("101", "102");
        assertThat(b.anything()).isEqualTo("42");
    }

    @Test
    void a_map_input_without_declared_types_gets_raw_strings() {
        var seen = new Object() { Map<String, Object> value; };
        TaskHandler<Map, Void> handler = (in, ctx) -> {
            @SuppressWarnings("unchecked") Map<String, Object> m = in;
            seen.value = m;
            return null;
        };
        var registry = new TaskRegistry(List.of(
                new TaskRegistration<>("raw", 1, "t", Map.class, Void.class, handler)));

        dispatcher(registry, new RecordingSink(), Cancellations.NONE, false).dispatch(request("raw@1", List.of(
                new Variable("locator", "12E45"), new Variable("qty", "3"))));

        assertThat(seen.value).containsEntry("locator", "12E45").containsEntry("qty", "3");
    }

    record Dated(java.time.LocalDate day) {}

    @Test
    void java_time_output_is_written_as_iso_strings() {
        TaskHandler<Void, Dated> handler = (in, ctx) -> new Dated(java.time.LocalDate.of(2026, 1, 2));
        var registry = new TaskRegistry(List.of(
                new TaskRegistration<>("date", 1, "t", Void.class, Dated.class, handler)));
        var sink = new RecordingSink();

        dispatcher(registry, sink, Cancellations.NONE, false).dispatch(request("date@1", List.of()));

        assertThat(sink.completed).singleElement().satisfies(vars ->
                assertThat(vars).containsExactly(new Variable("day", "2026-01-02")));
    }
}
