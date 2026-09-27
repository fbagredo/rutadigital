package io.mateu.workflow.application.usecases.directoryimport;

import io.mateu.workflow.application.services.WorkflowDefinitionValidator;
import io.mateu.workflow.application.out.WorkflowDefinitionRepository;
import io.mateu.workflow.domain.aggregates.NodePosition;
import io.mateu.workflow.domain.aggregates.WorkflowDefinition;
import io.mateu.workflow.infra.config.DirectoryImportProperties;
import io.mateu.workflow.webhook.InMemoryImportedDefinitionsRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Importing definitions from a directory on disk — the thing a mounted volume, a checkout beside
 * the app, or anything that writes definitions needs, and which until now could only be had by
 * committing them and letting the engine clone.
 */
class ImportWorkflowDefinitionsFromDirectoryTest {

    private final WorkflowDefinitionRepository repository = mock(WorkflowDefinitionRepository.class);
    private final InMemoryImportedDefinitionsRegistry registry = new InMemoryImportedDefinitionsRegistry();
    private final ImportWorkflowDefinitionsFromDirectoryUseCase useCase =
            new ImportWorkflowDefinitionsFromDirectoryUseCase(
                    mock(DirectoryImportProperties.class), repository, registry,
                    new WorkflowDefinitionValidator(),
                    new io.mateu.workflow.application.services.TaskReferenceResolver(
                            new io.mateu.workflow.infra.out.classpath.ClasspathTaskContractRepository()));

    private void write(Path dir, String name, String id) throws IOException {
        Files.writeString(dir.resolve(name), """
                id: %s
                name: %s
                version: 1
                steps:
                  - id: start
                    type: START
                    name: Start
                  - id: end
                    type: END
                    name: End
                    preconditionStepId: start
                """.formatted(id, id));
    }

    private List<WorkflowDefinition> saved() {
        var captor = ArgumentCaptor.forClass(WorkflowDefinition.class);
        verify(repository, atLeast(0)).save(captor.capture());
        return captor.getAllValues();
    }

    @Test
    void importsEveryDefinitionUnderTheDirectory(@TempDir Path dir) throws Exception {
        write(dir, "one.yml.ec", "one");
        Files.createDirectories(dir.resolve("nested"));
        write(dir.resolve("nested"), "two.yml.ec", "two");
        Files.writeString(dir.resolve("README.md"), "not a definition");

        var result = useCase.handle(List.of(dir.toString()));

        assertThat(result.errors()).isEmpty();
        assertThat(saved()).extracting(WorkflowDefinition::id).containsExactlyInAnyOrder("one", "two");
        assertThat(result.imported()).hasSize(2);
    }

    private void writeArranged(Path dir, String name, String body) throws IOException {
        Files.writeString(dir.resolve(name), """
                name: Arranged
                version: 1
                steps:
                  - id: start
                    type: START
                    name: Start
                  - id: end
                    type: END
                    name: End
                    preconditionStepId: start
                %s
                """.formatted(body));
    }

    /**
     * The arrangement has to survive the rebuild that assigns a derived id — a file with no id of
     * its own goes through a narrower constructor, which is where a field that is not threaded
     * through is silently lost. It is also the commonest kind of file: most authors never write one.
     */
    @Test
    void aFileWithNoIdKeepsItsArrangement(@TempDir Path dir) throws Exception {
        writeArranged(dir, "arranged.ec", """
                layout:
                  start: {x: 60, y: 160}
                  end: {x: 420, y: 160}""");

        useCase.handle(List.of(dir.toString()));

        assertThat(saved()).singleElement().satisfies(def -> {
            assertThat(def.id()).isEqualTo("arranged");
            assertThat(def.layout()).containsOnlyKeys("start", "end");
            assertThat(def.layout().get("end")).isEqualTo(new NodePosition(420, 160));
        });
    }

    /** A workflow nobody arranged imports with no arrangement at all, not with an empty one. */
    @Test
    void aFileWithNoLayoutImportsWithoutOne(@TempDir Path dir) throws Exception {
        write(dir, "plain.ec", "plain");

        useCase.handle(List.of(dir.toString()));

        assertThat(saved()).singleElement()
                .satisfies(def -> assertThat(def.layout()).isNull());
    }

    /**
     * A coordinate for a step that has since been deleted is a stale coordinate, not a broken
     * workflow. It imports, and the graph simply has nothing to draw at it — refusing the file
     * would make deleting a step from a YAML by hand a way to break the import.
     */
    @Test
    void aLayoutEntryForAStepThatIsGoneDoesNotRefuseTheFile(@TempDir Path dir) throws Exception {
        writeArranged(dir, "stale.ec", """
                layout:
                  start: {x: 60, y: 160}
                  removed-long-ago: {x: 999, y: 999}""");

        var result = useCase.handle(List.of(dir.toString()));

        assertThat(result.errors()).isEmpty();
        assertThat(saved()).singleElement().satisfies(def ->
                assertThat(def.layout()).containsKey("removed-long-ago"));
    }

    @Test
    void aDirectoryThatIsNotThereIsAnErrorRatherThanASilence(@TempDir Path dir) {
        // The failure mode this exists to avoid: a typo in the mount path, an engine that starts
        // clean and a definition list that is empty for no stated reason.
        var result = useCase.handle(List.of(dir.resolve("nope").toString()));

        assertThat(result.imported()).isEmpty();
        assertThat(result.errors()).singleElement().satisfies(error -> {
            assertThat(error).startsWith("Directory ");
            assertThat(error).contains("not a directory");
        });
    }

    @Test
    void aDefinitionThatLeavesTheDirectoryIsPrunedOnTheNextImport(@TempDir Path dir) throws Exception {
        write(dir, "one.yml.ec", "one");
        write(dir, "two.yml.ec", "two");
        useCase.handle(List.of(dir.toString()));
        assertThat(registry.idsFor("workflow", dir.toAbsolutePath().normalize().toString())).containsExactlyInAnyOrder("one", "two");

        Files.delete(dir.resolve("two.yml.ec"));
        var gone = new WorkflowDefinition("two", "two", 1, null, false, 0, false, null, 0, List.of());
        when(repository.findById("two")).thenReturn(Optional.of(gone));

        var result = useCase.handle(List.of(dir.toString()));

        assertThat(result.pruned()).singleElement().satisfies(p -> assertThat(p).contains("two"));
        verify(repository).save(argThat(d -> "two".equals(d.id()) && d.archived()));
    }

    @Test
    void oneBadFileCostsThatFileAndNothingElse(@TempDir Path dir) throws Exception {
        write(dir, "good.yml.ec", "good");
        Files.writeString(dir.resolve("broken.ec"), "{ this is not json");

        var result = useCase.handle(List.of(dir.toString()));

        assertThat(result.errors()).singleElement().satisfies(e -> assertThat(e).contains("broken.ec"));
        assertThat(saved()).extracting(WorkflowDefinition::id).containsExactly("good");
    }

    private void writeWithoutId(Path dir, String name, String workflowName) throws IOException {
        Files.writeString(dir.resolve(name), """
                name: %s
                version: 1
                steps:
                  - id: start
                    type: START
                    name: Start
                  - id: end
                    type: END
                    name: End
                    preconditionStepId: start
                """.formatted(workflowName));
    }

    @Test
    void aFileWithNoIdKeepsTheSameIdOnEveryImport(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir.resolve("sagas"));
        writeWithoutId(dir.resolve("sagas"), "onboarding.yml.ec", "Onboarding");

        useCase.handle(List.of(dir.toString()));
        useCase.handle(List.of(dir.toString()));

        // It used to be a fresh UUID per import, so the second import could not find what the first
        // one had created from this very file and inserted another copy. With a git webhook wired
        // up, every push added one.
        assertThat(saved()).extracting(WorkflowDefinition::id)
                .containsExactly("sagas.onboarding", "sagas.onboarding");
    }

    @Test
    void aFileWithNoIdIsPrunedWhenItGoesAway(@TempDir Path dir) throws Exception {
        writeWithoutId(dir, "nameless.ec", "Nameless");
        useCase.handle(List.of(dir.toString()));

        // Pruning is what an unreconcilable id cost: the import used to track explicit ids only,
        // so a file with none could never be recognised as gone.
        assertThat(registry.idsFor("workflow", dir.toAbsolutePath().normalize().toString())).containsExactly("nameless");

        Files.delete(dir.resolve("nameless.ec"));
        when(repository.findById("nameless")).thenReturn(Optional.of(
                new WorkflowDefinition("nameless", "Nameless", 1, null, false, 0, false, null, 0, List.of())));

        var result = useCase.handle(List.of(dir.toString()));

        assertThat(result.pruned()).hasSize(1);
    }

    @Test
    void aPathDerivedIdNeverTakesOneAnotherFileDeclares(@TempDir Path dir) throws Exception {
        // Declared in a file the walk may well reach second, which is why the ids are collected
        // before anything is imported: by then the derived one would already have been saved over.
        write(dir, "elsewhere.ec", "sagas.onboarding");
        Files.createDirectories(dir.resolve("sagas"));
        writeWithoutId(dir.resolve("sagas"), "onboarding.ec", "Onboarding");

        var result = useCase.handle(List.of(dir.toString()));

        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().getFirst()).contains("sagas/onboarding.ec", "sagas.onboarding");
        // The declared one is untouched: an explicit id is a promise its author made, a derived one
        // is a default, and silently overwriting the first with the second is worse than the
        // duplication this whole change is about.
        assertThat(saved()).extracting(WorkflowDefinition::id).containsExactly("sagas.onboarding");
    }
}
