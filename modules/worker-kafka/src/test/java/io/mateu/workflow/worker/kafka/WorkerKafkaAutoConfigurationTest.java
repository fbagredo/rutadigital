package io.mateu.workflow.worker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mateu.workflow.worker.api.Cancellations;
import io.mateu.workflow.worker.api.TaskDispatcher;
import io.mateu.workflow.worker.api.TaskRegistry;
import io.mateu.workflow.worker.api.TaskReplySink;
import io.mateu.workflow.worker.api.WorkerApiAutoConfiguration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.function.StreamOperations;

/** The worker's beans inside an application that has beans of its own. */
class WorkerKafkaAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(WorkerApiAutoConfiguration.class,
                    WorkerKafkaAutoConfiguration.class))
            .withBean(StreamOperations.class, RecordingStreamOperations::new);

    /** An application's own dispatcher, unrelated to the SDK's type. */
    static final class AppDispatcher {
    }

    @Test
    void an_application_bean_named_taskDispatcher_does_not_clash() {
        // Bean definition overriding is off by default in Boot: a same-named SDK bean would fail
        // the context with BeanDefinitionOverrideException.
        runner.withBean("taskDispatcher", AppDispatcher.class, AppDispatcher::new)
                .withBean("taskRegistry", AppDispatcher.class, AppDispatcher::new)
                .withBean("cancelledTasks", AppDispatcher.class, AppDispatcher::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean("taskDispatcher")).isInstanceOf(AppDispatcher.class);
                    assertThat(context).hasSingleBean(TaskDispatcher.class);
                    assertThat(context.getBeanNamesForType(TaskDispatcher.class))
                            .containsExactly("eventconductorTaskDispatcher");
                });
    }

    @Test
    void the_sdk_dispatcher_backs_off_for_the_applications_own() {
        var own = new TaskDispatcher(new TaskRegistry(List.of()), new WorkerReplySink(new RecordingStreamOperations()),
                Cancellations.NONE, false, null);
        runner.withBean("myDispatcher", TaskDispatcher.class, () -> own)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(TaskDispatcher.class);
                    assertThat(context.getBean(TaskDispatcher.class)).isSameAs(own);
                });
    }

    @Test
    void the_sdk_registers_no_object_mapper() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(ObjectMapper.class);
            assertThat(context).hasSingleBean(TaskReplySink.class);
        });
    }

    @Test
    void the_applications_object_mapper_stays_the_only_one() {
        var appMapper = new ObjectMapper();
        runner.withBean(ObjectMapper.class, () -> appMapper)
                .run(context -> {
                    assertThat(context).hasSingleBean(ObjectMapper.class);
                    assertThat(context.getBean(ObjectMapper.class)).isSameAs(appMapper);
                });
    }
}
