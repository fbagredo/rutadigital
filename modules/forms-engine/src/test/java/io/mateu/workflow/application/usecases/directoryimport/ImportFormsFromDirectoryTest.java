package io.mateu.workflow.application.usecases.directoryimport;

import io.mateu.workflow.application.out.FormRepository;
import io.mateu.workflow.application.out.FormsMetrics;
import io.mateu.workflow.domain.Form;
import io.mateu.workflow.infra.config.DirectoryImportProperties;
import io.mateu.workflow.webhook.InMemoryImportedDefinitionsRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/** Importing forms from a directory on disk, without a commit standing between the two. */
class ImportFormsFromDirectoryTest {

    private final FormRepository repository = mock(FormRepository.class);
    private final InMemoryImportedDefinitionsRegistry registry = new InMemoryImportedDefinitionsRegistry();
    private final ImportFormsFromDirectoryUseCase useCase = new ImportFormsFromDirectoryUseCase(
            mock(DirectoryImportProperties.class), repository, mock(FormsMetrics.class), registry);

    private void write(Path dir, String name, String id) throws IOException {
        Files.writeString(dir.resolve(name), """
                id: %s
                name: %s
                fields:
                  - id: comment
                    label: Comment
                    dataType: string
                """.formatted(id, id));
    }

    private List<Form> saved() {
        var captor = ArgumentCaptor.forClass(Form.class);
        verify(repository, atLeast(0)).save(captor.capture());
        return captor.getAllValues();
    }

    @Test
    void importsEveryFormUnderTheDirectory(@TempDir Path dir) throws Exception {
        write(dir, "one.yml", "one");
        write(dir, "two.ecform", "two");
        Files.writeString(dir.resolve("README.md"), "not a form");

        var result = useCase.handle(List.of(dir.toString()));

        assertThat(result.errors()).isEmpty();
        assertThat(saved()).extracting(Form::id).containsExactlyInAnyOrder("one", "two");
    }

    @Test
    void aDirectoryThatIsNotThereIsAnErrorRatherThanASilence(@TempDir Path dir) {
        var result = useCase.handle(List.of(dir.resolve("nope").toString()));

        assertThat(result.imported()).isEmpty();
        assertThat(result.errors()).singleElement().satisfies(error ->
                assertThat(error).startsWith("Directory ").contains("not a directory"));
    }

    @Test
    void aFormThatLeavesTheDirectoryIsPrunedOnTheNextImport(@TempDir Path dir) throws Exception {
        write(dir, "one.yml", "one");
        write(dir, "two.yml", "two");
        useCase.handle(List.of(dir.toString()));

        Files.delete(dir.resolve("two.yml"));
        when(repository.findById("two")).thenReturn(Optional.of(new Form("two", "two", null, List.of())));

        var result = useCase.handle(List.of(dir.toString()));

        assertThat(result.pruned()).singleElement().satisfies(p -> assertThat(p).contains("two"));
        verify(repository).deleteAllById(List.of("two"));
    }

    private void writeWithoutId(Path dir, String name, String formName) throws IOException {
        Files.writeString(dir.resolve(name), """
                name: %s
                fields:
                  - id: comment
                    label: Comment
                    dataType: string
                """.formatted(formName));
    }

    @Test
    void aFormWithNoIdKeepsTheSameIdOnEveryImport(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir.resolve("checkin"));
        writeWithoutId(dir.resolve("checkin"), "walk.ecform", "Walk");

        useCase.handle(List.of(dir.toString()));
        useCase.handle(List.of(dir.toString()));

        // A fresh UUID per import made the file unreconcilable with the form it had produced, so
        // every import added another copy of it.
        assertThat(saved()).extracting(Form::id).containsExactly("checkin.walk", "checkin.walk");
        // And it can be pruned now, which is the other half of what an unstable id cost.
        assertThat(registry.idsFor("form", dir.toAbsolutePath().normalize().toString())).containsExactly("checkin.walk");
    }

    @Test
    void aPathDerivedIdNeverTakesOneAnotherFormDeclares(@TempDir Path dir) throws Exception {
        write(dir, "elsewhere.ecform", "checkin.walk");
        Files.createDirectories(dir.resolve("checkin"));
        writeWithoutId(dir.resolve("checkin"), "walk.ecform", "Walk");

        var result = useCase.handle(List.of(dir.toString()));

        assertThat(result.errors()).hasSize(1);
        assertThat(saved()).extracting(Form::id).containsExactly("checkin.walk");
    }
}
