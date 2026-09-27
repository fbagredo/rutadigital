package io.mateu.workflowdist.support.orchestrator;

import io.mateu.workflow.autoconfigure.WorkflowEmbeddedApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Boots a full orchestrator instance for the distributed suite. Despite the annotation's
 * name it just scans the engine's {@code io.mateu.workflow} tree (UI adapters excluded);
 * the tests run it with {@code workflow.mode=kafka} + {@code workflow.persistence=jpa},
 * which activates the Kafka consumers, the outbox relay, the JDBC advisory locks and the
 * JPA repositories — the same wiring as apps/orchestrator-standalone-app.
 *
 * <p>Lives outside {@code io.mateu.workflow} so orchestrator component scanning never
 * picks up the suite's own configuration classes — which is also why the JPA entity and
 * repository packages must be pointed at the engine explicitly.
 */
@WorkflowEmbeddedApplication
@EntityScan("io.mateu.workflow")
@EnableJpaRepositories("io.mateu.workflow")
public class DistOrchestratorApp {

    /** Stands in for the MeterRegistry the standalone app gets from Actuator. */
    @org.springframework.context.annotation.Bean
    io.micrometer.core.instrument.MeterRegistry meterRegistry() {
        return new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    }

    /**
     * A real tracer, for DIST-31 only ({@code dist.tracing.enabled=true}): OpenTelemetry's SDK with
     * W3C trace-context and baggage propagation, behind the Micrometer bridge the engine talks to.
     * Every other test runs as production runs by default — with no tracer at all.
     */
    @org.springframework.context.annotation.Configuration
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "dist.tracing.enabled", havingValue = "true")
    static class DistTracing {

        @org.springframework.context.annotation.Bean(destroyMethod = "close")
        io.opentelemetry.sdk.trace.SdkTracerProvider distTracerProvider() {
            return io.opentelemetry.sdk.trace.SdkTracerProvider.builder()
                    .addSpanProcessor(io.opentelemetry.sdk.trace.export.SimpleSpanProcessor.create(
                            io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter.create()))
                    .build();
        }

        @org.springframework.context.annotation.Bean
        io.micrometer.tracing.Tracer distTracer(io.opentelemetry.sdk.trace.SdkTracerProvider provider) {
            return new io.micrometer.tracing.otel.bridge.OtelTracer(provider.get("dist"),
                    new io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext(), event -> { });
        }

        @org.springframework.context.annotation.Bean
        io.micrometer.tracing.propagation.Propagator distPropagator(io.opentelemetry.sdk.trace.SdkTracerProvider provider) {
            return new io.micrometer.tracing.otel.bridge.OtelPropagator(
                    io.opentelemetry.context.propagation.ContextPropagators.create(
                            io.opentelemetry.context.propagation.TextMapPropagator.composite(
                                    io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.getInstance(),
                                    io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator.getInstance())),
                    provider.get("dist"));
        }
    }
}
