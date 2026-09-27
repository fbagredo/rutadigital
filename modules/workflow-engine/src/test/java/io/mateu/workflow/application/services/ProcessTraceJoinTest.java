package io.mateu.workflow.application.services;

import io.mateu.workflow.application.out.ProcessTraceContextRepository;
import io.mateu.workflow.application.out.WorkflowTracing;
import io.mateu.workflow.dtos.TraceContext;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The stored half of a process's anchor: joined contexts are found on any pod, and the lookup that
 * finds them happens once per process per pod — including the "joined nothing" answer, which is the
 * answer for nearly every process and must not cost a query per step-over.
 */
class ProcessTraceJoinTest {

    private static final TraceContext CALLER =
            TraceContext.of("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");

    private final AtomicInteger lookups = new AtomicInteger();
    private final Map<String, TraceContext> rows = new HashMap<>();
    private final ProcessTraceContextRepository store = new ProcessTraceContextRepository() {
        public void save(String processId, TraceContext context) {
            rows.put(processId, context);
        }

        public Optional<TraceContext> findByProcessId(String processId) {
            lookups.incrementAndGet();
            return Optional.ofNullable(rows.get(processId));
        }

        public void deleteAllByProcessId(Collection<String> processIds) {
            processIds.forEach(rows::remove);
        }
    };

    private final WorkflowTracing on = new WorkflowTracing() {
        @Override
        public boolean enabled() {
            return true;
        }
    };

    @Test
    void anotherPodFindsTheJoinedContextAndAsksOnlyOnce() {
        new ProcessTrace(1.0, on, store, 100).join("p-1", CALLER);
        var otherPod = new ProcessTrace(1.0, on, store, 100);

        assertThat(otherPod.anchorFor("p-1")).isEqualTo(CALLER.traceparent());
        assertThat(otherPod.anchorFor("p-1")).isEqualTo(CALLER.traceparent());
        assertThat(lookups).hasValue(1);
    }

    @Test
    void joiningNothingIsAlsoRememberedSoTheQueryIsNotRepeated() {
        var trace = new ProcessTrace(1.0, on, store, 100);

        trace.anchorFor("p-2");
        trace.anchorFor("p-2");

        assertThat(trace.anchorFor("p-2")).isEqualTo(trace.derivedAnchorFor("p-2"));
        assertThat(lookups).hasValue(1);
    }

    @Test
    void withTracingOffThereIsNoLookupAtAll() {
        rows.put("p-1", CALLER);
        var trace = new ProcessTrace(1.0, WorkflowTracing.NOOP, store, 100);

        assertThat(trace.anchorFor("p-1")).isEqualTo(trace.derivedAnchorFor("p-1"));
        assertThat(lookups).hasValue(0);
    }

    @Test
    void aStoreThatFailsFallsBackToTheDerivedAnchor() {
        var failing = new ProcessTraceContextRepository() {
            public void save(String processId, TraceContext context) {
            }

            public Optional<TraceContext> findByProcessId(String processId) {
                throw new IllegalStateException("database down");
            }

            public void deleteAllByProcessId(Collection<String> processIds) {
            }
        };
        var trace = new ProcessTrace(1.0, on, failing, 100);

        assertThat(trace.anchorFor("p-1")).isEqualTo(trace.derivedAnchorFor("p-1"));
    }

    @Test
    void forgottenProcessesLoseTheirContext() {
        var trace = new ProcessTrace(1.0, on, store, 100);
        trace.join("p-1", CALLER);

        trace.forget(java.util.List.of("p-1"));

        assertThat(trace.anchorFor("p-1")).isEqualTo(trace.derivedAnchorFor("p-1"));
    }
}
