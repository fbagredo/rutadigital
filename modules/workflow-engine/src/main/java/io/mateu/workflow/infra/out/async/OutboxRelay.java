package io.mateu.workflow.infra.out.async;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import io.mateu.workflow.infra.out.persistence.DbLockDialect;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes committed outbox messages to Kafka.
 *
 * <p><b>Every pod relays.</b> Batches are claimed with row locks that skip what other pods hold
 * (see {@link OutboxDrain}), so N orchestrators drain N disjoint slices and relay throughput
 * grows with the cluster. This used to be a leader-elected singleton: one pod drained the whole
 * outbox while the rest idled, which put a ceiling on the distributed topology that adding pods
 * could not lift — every state transition in kafka mode passes through here.
 *
 * <p>The loop does not simply sleep between passes: it waits on {@link OutboxSignal}, which the
 * pod raises after committing a write of its own. The poll interval becomes a fallback for rows
 * written by other pods rather than the latency every step pays — measured on the benchmark
 * harness, that wait was about half the cost of a transition.
 *
 * <p>A pass that comes back with a full batch means more is waiting, so the loop keeps draining
 * until it does not — otherwise throughput would be capped at one batch per poll interval, and a
 * backlog would take as many intervals as it has batches. It stops early if a full batch settles
 * nothing, so messages that always fail cannot spin the loop.
 *
 * <p><b>When the broker refuses, the relay backs off.</b> Stopping the inner loop early is not
 * enough on its own: the outer loop waits on the signal, every commit raises it, and a commit does
 * not mean the broker has come back — so a pod under load with a broker down retried at the rate it
 * was writing, not at any configured interval. A pass that claims rows and settles none of them is
 * now paced by {@link RelayPace} instead, which ignores the signal while it waits. See that class
 * for what counts as a stall and why the cap is seconds.
 */
@Service
@ConditionalOnProperty(name = "workflow.mode", havingValue = "kafka")
@ConditionalOnProperty(name = "workflow.outbox.relay-enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
@Slf4j
public class OutboxRelay {

    final OutboxDrain outboxDrain;
    final OutboxSignal outboxSignal;
    final io.mateu.workflow.application.out.WorkflowMetrics workflowMetrics;
    final StreamBridge streamBridge;
    final JdbcTemplate jdbcTemplate;
    final DbLockDialect dbLockDialect;
    final io.mateu.workflow.application.services.messagerouting.MessageRouter messageRouter;
    final io.mateu.workflow.infra.config.EventDestinationsProperties eventDestinations;

    @org.springframework.beans.factory.annotation.Value("${workflow.outbox-poll-interval-ms:500}")
    long pollIntervalMs;

    @org.springframework.beans.factory.annotation.Value("${workflow.outbox.batch-size:100}")
    int batchSize;

    // When sharded, a SEND_MESSAGE step's MessageReceived (which rides this process's outbox) is relayed
    // to the shared cross-shard `messages` topic instead of this shard's `outbox`, so it can reach a
    // waiter on any shard. Off (default) → messages stay on `outbox`, single-cluster behaviour unchanged.
    @org.springframework.beans.factory.annotation.Value("${workflow.sharding.enabled:false}")
    boolean sharedMessages;

    // In remote projection mode the read model is maintained by a standalone projector against a read
    // database this shard does not own, so ProcessStatusChanged is relayed to the shared projection
    // topic instead of this shard's `outbox`. Embedded (the default) → it stays on `outbox` and the
    // in-process projector handles it, exactly as before.
    @org.springframework.beans.factory.annotation.Value("${workflow.projection.mode:embedded}")
    String projectionMode;

    // How the relay paces itself once passes stop settling anything — see RelayPace for why the
    // signal cannot be what paces them and why the cap is seconds rather than a minute.
    @org.springframework.beans.factory.annotation.Value("${workflow.outbox.relay-backoff-base-ms:100}")
    long relayBackoffBaseMs;

    @org.springframework.beans.factory.annotation.Value("${workflow.outbox.relay-backoff-max-ms:5000}")
    long relayBackoffMaxMs;

    @org.springframework.beans.factory.annotation.Value("${workflow.outbox.relay-backoff-jitter:0.2}")
    double relayBackoffJitter;

    private volatile boolean running = true;
    private Thread relayThread;

    @PostConstruct
    public void iterate() {
        var pace = new RelayPace(relayBackoffBaseMs, relayBackoffMaxMs, relayBackoffJitter);
        relayThread = new Thread(() -> {
            try {
                while (running) {
                    var cycleStartedAt = System.nanoTime();
                    boolean stalled;
                    try {
                        var lastPass = drainUntilEmpty();
                        stalled = lastPass != null && RelayPace.isStall(lastPass.claimed(), lastPass.settled());
                    } catch (Throwable e) {
                        stalled = true;
                        log.error("Error relaying outbox messages", e);
                    }
                    var drainedAt = System.nanoTime();
                    if (stalled) {
                        // Claimed rows, settled none: the broker is refusing. Wait a growing
                        // while and IGNORE the signal, because the signal is what turns this
                        // into a hot loop — every commit raises it, and a commit does not mean
                        // the broker has come back.
                        var wait = pace.stalled();
                        workflowMetrics.outboxRelayStalled();
                        log.warn("The outbox relay settled nothing on {} consecutive passes; "
                                        + "waiting {} ms before the next one",
                                pace.consecutiveStalls(), wait.toMillis());
                        Thread.sleep(wait.toMillis());
                    } else {
                        pace.progressed();
                        // Woken by this pod's own writes, and falling back to the poll for rows
                        // written by other pods, which there is no way to hear about directly.
                        outboxSignal.awaitWork(pollIntervalMs);
                    }
                    // Draining against waiting is this thread's duty cycle. It is one thread per
                    // pod and every transition in kafka mode passes through it, so a duty cycle
                    // that sits near 1 is the ceiling itself and not a symptom of one.
                    workflowMetrics.outboxRelayCycle(
                            java.time.Duration.ofNanos(drainedAt - cycleStartedAt),
                            java.time.Duration.ofNanos(System.nanoTime() - drainedAt));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "outbox-relay");
        relayThread.setDaemon(true);
        relayThread.start();
    }

    /**
     * Stops the relay when the context goes down. A daemon thread would not hold the JVM open, but
     * it would keep claiming rows and publishing while everything it depends on is being closed —
     * and in a test it would keep running for the rest of the suite.
     */
    @PreDestroy
    public void stop() {
        running = false;
        if (relayThread != null) {
            relayThread.interrupt();
        }
    }

    /**
     * Drains until a pass comes back short or settles nothing, and reports that last pass so the
     * caller can tell a relay that is keeping up from one the broker is refusing. Null when the
     * gate was not taken — the chaos tests hold it exclusively to freeze every relay at once, and
     * a relay that was not allowed to run has not failed at anything.
     */
    private OutboxDrain.Result drainUntilEmpty() {
        // The gate is held in shared mode: relays never block each other, but the chaos tests
        // can freeze every one of them by taking it exclusively.
        return jdbcTemplate.execute((org.springframework.jdbc.core.ConnectionCallback<OutboxDrain.Result>) con -> {
            if (!dbLockDialect.tryRelayGate(con)) {
                return null;
            }
            try {
                OutboxDrain.Result result;
                do {
                    result = outboxDrain.drain(batchSize, this::relay);
                } while (result.claimed() >= batchSize && result.settled() > 0);
                return result;
            } finally {
                dbLockDialect.releaseRelayGate(con);
            }
        });
    }

    /**
     * Relays one drained event. A {@code SEND_MESSAGE}'s {@link MessageReceived}, when sharding and
     * message routing are on, is handed to the {@link MessageRouter} — which sends it to the shard(s)
     * that can correlate it — instead of being broadcast to the shared {@code messages} topic.
     * Everything else (and every message with routing off) goes to its {@link RelayDestination}.
     */
    private void relay(io.mateu.workflow.ddd.DomainEvent event) {
        if (event instanceof io.mateu.workflow.dtos.events.integration.ExternalEventRequested external) {
            // A PUBLISH_EVENT: out to its destination's topic, never to the engine's own.
            ExternalEventSender.send(streamBridge, eventDestinations, external, currentTrace());
            return;
        }
        if (sharedMessages && messageRouter.isEnabled()
                && event instanceof io.mateu.workflow.dtos.events.integration.MessageReceived message) {
            messageRouter.route(message);
            return;
        }
        PartitionedEvents.send(streamBridge, bindingFor(event), event, currentTrace());
    }

    /**
     * The trace the relay is publishing in — {@link OutboxDrain} runs each delivery as a
     * continuation of the row's own context — for the record's headers. Null when nothing is traced.
     */
    private io.mateu.workflow.dtos.TraceContext currentTrace() {
        var tracing = outboxDrain.workflowTracing;
        return tracing == null ? null : tracing.currentTraceContext();
    }

    /** See {@link RelayDestination} — where a relayed event goes, and why anything leaves `outbox`. */
    private String bindingFor(io.mateu.workflow.ddd.DomainEvent event) {
        return RelayDestination.bindingFor(event, sharedMessages, "remote".equals(projectionMode));
    }
}
