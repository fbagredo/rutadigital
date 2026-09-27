package io.mateu.workflow.infra.in.ui.pages.taskcontracts;

import static org.assertj.core.api.Assertions.assertThat;

import io.mateu.uidl.data.Pageable;
import io.mateu.workflow.application.out.TaskContractRepository;
import io.mateu.workflow.application.out.WorkflowDefinitionRepository;
import io.mateu.workflow.domain.aggregates.WorkflowDefinition;
import io.mateu.workflow.tasks.TaskContract;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TaskContractsCrudAdapterTest {

    private final TaskContract greet = new TaskContract("greet", 1, "greetings", "sample-greetings",
            "Greet someone", Map.of(), Map.of(), List.of());
    private final TaskContract charge = new TaskContract("charge", 2, "billing", null, null,
            Map.of(), Map.of(), List.of());

    private final TaskContractRepository contracts = repo(List.of(greet, charge));
    private final WorkflowDefinitionRepository definitions = definitions(List.of());
    private final TaskContractsCrudAdapter adapter = new TaskContractsCrudAdapter(contracts, definitions);

    private static Pageable page() {
        return new Pageable(0, 20, List.of());
    }

    @Test
    void lists_each_contract_as_a_row_with_download_urls() {
        var rows = adapter.search(null, null, page(), null).page().content();

        assertThat(rows).extracting(TaskContractRow::id).containsExactlyInAnyOrder("greet@1", "charge@2");
        var greetRow = rows.stream().filter(r -> r.id().equals("greet@1")).findFirst().orElseThrow();
        assertThat(greetRow.group()).isEqualTo("greetings");
        assertThat(greetRow.moduleZip()).isEqualTo("/eventconductor/tasks/greetings/module.zip");
        assertThat(greetRow.serviceZip()).isEqualTo("/eventconductor/tasks/greetings/service.zip");
        assertThat(greetRow.workflows()).isZero();
    }

    @Test
    void a_long_description_is_one_line_in_the_column_and_whole_in_the_row_detail() {
        var text = "Charges the guest's card for the stay, retrying a soft decline twice before it "
                + "gives up and tells the front desk.";
        var adapter = new TaskContractsCrudAdapter(repo(List.of(new TaskContract("charge", 3, "billing",
                null, text, Map.of(), Map.of(), List.of()))), definitions);

        var row = adapter.search(null, null, page(), null).page().content().get(0);

        assertThat(row.description()).endsWith("…").hasSizeLessThanOrEqualTo(71);
        assertThat(row.fullDescription()).isEqualTo(text);
        // the search looks at the whole description, not at the part the column shows
        assertThat(adapter.search("front desk", null, page(), null).page().content())
                .extracting(TaskContractRow::id).containsExactly("charge@3");
    }

    @Test
    void filters_by_group() {
        var rows = adapter.search(null, new TaskContractFilters("billing"), page(), null).page().content();
        assertThat(rows).extracting(TaskContractRow::id).containsExactly("charge@2");
    }

    @Test
    void filters_by_search_text() {
        var rows = adapter.search("greet", null, page(), null).page().content();
        assertThat(rows).extracting(TaskContractRow::id).containsExactly("greet@1");
    }

    private static TaskContractRepository repo(List<TaskContract> all) {
        return new TaskContractRepository() {
            public Optional<TaskContract> find(String id, int version) {
                return Optional.empty();
            }

            public Optional<TaskContract> findLatest(String id) {
                return Optional.empty();
            }

            public List<TaskContract> findAll() {
                return all;
            }

            public String save(TaskContract contract) {
                return contract.ref();
            }
        };
    }

    private static WorkflowDefinitionRepository definitions(List<WorkflowDefinition> all) {
        return new WorkflowDefinitionRepository() {
            public Optional<WorkflowDefinition> findById(String id) {
                return Optional.empty();
            }

            public String save(WorkflowDefinition entity) {
                return entity.id();
            }

            public List<WorkflowDefinition> findAll() {
                return all;
            }

            public void deleteAllById(List<String> ids) {
            }
        };
    }
}
