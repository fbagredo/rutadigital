package io.mateu.workflow.infra.in.ui.pages.taskcontracts;

import io.mateu.uidl.data.ListingData;
import io.mateu.uidl.data.Page;
import io.mateu.uidl.data.Pageable;
import io.mateu.uidl.interfaces.CrudStore;
import io.mateu.uidl.interfaces.HttpRequest;
import io.mateu.workflow.application.out.TaskContractRepository;
import io.mateu.workflow.application.out.WorkflowDefinitionRepository;
import io.mateu.workflow.domain.aggregates.Step;
import io.mateu.workflow.domain.aggregates.StepType;
import io.mateu.workflow.infra.in.ui.pages.OneLine;
import io.mateu.workflow.tasks.TaskContract;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.stereotype.Service;

/**
 * Feeds the Tasks view: it turns each stored task contract into a row, counting the workflow
 * definitions that reference it and attaching the download URLs for its generated project. Contracts
 * are few, so it lists and pages them in memory rather than pushing the work into a store.
 */
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@Service
@RequiredArgsConstructor
public class TaskContractsCrudAdapter {

    private final TaskContractRepository taskContractRepository;
    private final WorkflowDefinitionRepository workflowDefinitionRepository;

    public ListingData<TaskContractRow> search(String searchText, TaskContractFilters filters,
                                               Pageable pageable, HttpRequest httpRequest) {
        var group = filters == null ? null : filters.group();
        var rows = allRows().stream()
                .filter(row -> group == null || group.isBlank() || group.equals(row.group()))
                .filter(row -> matchesText(row, searchText))
                .toList();

        var from = Math.min(pageable.page() * pageable.size(), rows.size());
        var to = Math.min(from + pageable.size(), rows.size());
        var pageContent = rows.subList(from, to);
        return new ListingData<>(new Page<>(searchText, pageable.size(), pageable.page(),
                (long) rows.size(), pageContent));
    }

    public CrudStore<TaskContractRow> repository() {
        return new CrudStore<>() {
            @Override
            public Optional<TaskContractRow> findById(String id) {
                return allRows().stream().filter(r -> r.id().equals(id)).findFirst();
            }

            @Override
            public String save(TaskContractRow entity) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<TaskContractRow> findAll() {
                return allRows();
            }

            @Override
            public void deleteAllById(List<String> selectedIds) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private List<TaskContractRow> allRows() {
        var definitions = workflowDefinitionRepository.findAll();
        return taskContractRepository.findAll().stream()
                .map(contract -> toRow(contract, definitions))
                .toList();
    }

    private TaskContractRow toRow(TaskContract contract, List<?> definitions) {
        var ref = contract.ref();
        int usedBy = 0;
        for (var definition : definitions) {
            if (referencesTask((io.mateu.workflow.domain.aggregates.WorkflowDefinition) definition, contract)) {
                usedBy++;
            }
        }
        return new TaskContractRow(ref, contract.group(), contract.topic(), OneLine.of(contract.description()),
                usedBy,
                "/eventconductor/tasks/" + contract.group() + "/module.zip",
                "/eventconductor/tasks/" + contract.group() + "/service.zip",
                contract.description());
    }

    private boolean referencesTask(io.mateu.workflow.domain.aggregates.WorkflowDefinition definition,
                                   TaskContract contract) {
        for (Step step : definition.steps()) {
            if (step.type() == StepType.ACTION && step.task() != null
                    && (step.task().equals(contract.ref()) || step.task().equals(contract.id()))) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesText(TaskContractRow row, String searchText) {
        if (searchText == null || searchText.isBlank()) {
            return true;
        }
        var needle = searchText.toLowerCase();
        return row.id().toLowerCase().contains(needle)
                || row.group().toLowerCase().contains(needle)
                || (row.fullDescription() != null && row.fullDescription().toLowerCase().contains(needle));
    }
}
