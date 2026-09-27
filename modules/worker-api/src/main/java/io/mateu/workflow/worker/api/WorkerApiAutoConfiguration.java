package io.mateu.workflow.worker.api;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Wires the transport-agnostic half of the worker: it gathers the {@link TaskRegistration} beans a
 * service exposes into a {@link TaskRegistry}. A transport
 * adapter (worker-kafka, worker-embedded) contributes the {@link TaskReplySink} and
 * {@link Cancellations}, and assembles the {@link TaskDispatcher} from these pieces — so this
 * configuration stays broker-free and loads on its own.
 *
 * <p>Bean names are namespaced ({@code eventconductor...}) so they cannot collide with an
 * application's own beans, and every bean backs off when the application defines one of the same
 * type. No {@code ObjectMapper} bean is contributed: variable binding uses a mapper private to the
 * SDK (see {@code WorkerJson}), so the application's JSON configuration is left alone.
 */
@AutoConfiguration
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkerApiAutoConfiguration {

    @Bean("eventconductorTaskRegistry")
    @ConditionalOnMissingBean(TaskRegistry.class)
    @SuppressWarnings({"rawtypes", "unchecked"})
    public TaskRegistry taskRegistry(ObjectProvider<TaskRegistration> registrations) {
        return new TaskRegistry(registrations.orderedStream()
                .map(r -> (TaskRegistration<?, ?>) r)
                .toList());
    }
}
