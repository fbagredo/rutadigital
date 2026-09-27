package io.mateu.workflow.worker.api;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * A {@link TaskTracing} over Micrometer Tracing, when the worker app has it on the classpath. The
 * tracer is looked up lazily, so an app with the classes but no configured bridge simply runs every
 * task untraced. Ordered before the transport adapters, which take whatever {@code TaskTracing}
 * exists and fall back to {@link TaskTracing#NOOP}.
 */
@AutoConfiguration(before = WorkerApiAutoConfiguration.class)
@ConditionalOnClass({Tracer.class, Propagator.class})
public class WorkerTracingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(TaskTracing.class)
    public TaskTracing workerTaskTracing(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        return new MicrometerTaskTracing(tracer::getIfAvailable, propagator::getIfAvailable);
    }
}
