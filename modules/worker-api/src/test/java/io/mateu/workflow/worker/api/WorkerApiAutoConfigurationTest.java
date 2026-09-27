package io.mateu.workflow.worker.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class WorkerApiAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(WorkerApiAutoConfiguration.class));

    @Test
    void contributes_no_object_mapper_bean() {
        // It used to register a plain fallback ObjectMapper that became the application's mapper
        // (no java.time support) or competed with a library's ("expected single matching bean").
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(ObjectMapper.class);
            assertThat(context.getBeanNamesForType(TaskRegistry.class))
                    .containsExactly("eventconductorTaskRegistry");
        });
    }

    @Test
    void an_application_bean_named_taskRegistry_does_not_clash() {
        runner.withBean("taskRegistry", String.class, () -> "mine")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean("taskRegistry")).isEqualTo("mine");
                    assertThat(context).hasSingleBean(TaskRegistry.class);
                });
    }
}
