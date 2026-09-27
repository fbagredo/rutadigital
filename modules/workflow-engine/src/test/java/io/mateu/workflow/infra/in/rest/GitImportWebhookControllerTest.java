package io.mateu.workflow.infra.in.rest;

import io.mateu.workflow.application.usecases.gitimport.ImportWorkflowDefinitionsFromGitUseCase;
import io.mateu.workflow.application.usecases.gitimport.ImportWorkflowDefinitionsFromGitUseCase.ImportWorkflowDefinitionsResult;
import io.mateu.workflow.application.usecases.taskcontractimport.ImportTasksFromDirectoryUseCase.ImportTasksResult;
import io.mateu.workflow.application.usecases.taskcontractimport.ImportTasksFromGitUseCase;
import io.mateu.workflow.infra.config.GitImportProperties;
import io.mateu.workflow.infra.config.TaskContractGitImportProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GitImportWebhookControllerTest {

    @Mock GitImportProperties properties;
    @Mock ImportWorkflowDefinitionsFromGitUseCase importUseCase;
    @Mock TaskContractGitImportProperties taskProperties;
    @Mock ImportTasksFromGitUseCase taskImportUseCase;

    GitImportWebhookController controller;

    private static TaskContractGitImportProperties.GitRepository taskRepo(String url, String branch) {
        var r = new TaskContractGitImportProperties.GitRepository();
        r.setUrl(url);
        r.setBranch(branch);
        return r;
    }

    private final TaskContractGitImportProperties.GitRepository tasksMaster =
            taskRepo("https://github.com/org/defs.git", "master");

    private static GitImportProperties.GitRepository repo(String url, String branch) {
        var r = new GitImportProperties.GitRepository();
        r.setUrl(url);
        r.setBranch(branch);
        return r;
    }

    private final GitImportProperties.GitRepository master =
            repo("https://github.com/org/defs.git", "master");

    @BeforeEach
    void setUp() {
        controller = new GitImportWebhookController(properties, importUseCase, taskProperties, taskImportUseCase);
        lenient().when(properties.getWebhookSecret()).thenReturn(null);
        lenient().when(taskProperties.getRepositories()).thenReturn(List.of());
        lenient().when(taskImportUseCase.handle(anyList()))
                .thenReturn(new ImportTasksResult(List.of(), List.of()));
        lenient().when(properties.getRepositories()).thenReturn(List.of(master));
        lenient().when(importUseCase.handle(anyList()))
                .thenReturn(new ImportWorkflowDefinitionsResult(List.of(), List.of(), List.of()));
    }

    private byte[] githubPush(String branch, String cloneUrl) {
        return ("{\"ref\":\"refs/heads/" + branch + "\",\"repository\":{\"clone_url\":\""
                + cloneUrl + "\"}}").getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @SuppressWarnings("unchecked")
    void reloadsOnlyTheMatchingRepositoryOnAPushToTheConfiguredBranch() {
        var response = controller.webhook("github", new HttpHeaders(),
                githubPush("master", "https://github.com/org/defs.git"));

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        ArgumentCaptor<List<GitImportProperties.GitRepository>> captor = ArgumentCaptor.forClass(List.class);
        verify(importUseCase, timeout(2000)).handle(captor.capture());
        assertThat(captor.getValue()).containsExactly(master);
    }

    @Test
    void ignoresPushesToOtherBranches() {
        var response = controller.webhook("github", new HttpHeaders(),
                githubPush("feature-x", "https://github.com/org/defs.git"));

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(response.getBody()).contains("ignored");
        verify(importUseCase, after(400).never()).handle(any());
    }

    @Test
    void ignoresPushesToOtherRepositories() {
        var response = controller.webhook("github", new HttpHeaders(),
                githubPush("master", "https://github.com/org/something-else.git"));

        assertThat(response.getBody()).contains("ignored");
        verify(importUseCase, after(400).never()).handle(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void fallsBackToReloadingEverythingWhenThePayloadCannotBeUnderstood() {
        controller.webhook("github", new HttpHeaders(), "not a json body".getBytes(StandardCharsets.UTF_8));

        ArgumentCaptor<List<GitImportProperties.GitRepository>> captor = ArgumentCaptor.forClass(List.class);
        verify(importUseCase, timeout(2000)).handle(captor.capture());
        assertThat(captor.getValue()).containsExactly(master);
    }

    @Test
    void rejectsAnInvalidSignatureWhenASecretIsConfigured() {
        when(properties.getWebhookSecret()).thenReturn("s3cret");
        var headers = new HttpHeaders();
        headers.add("X-Hub-Signature-256", "sha256=deadbeef");

        assertThatThrownBy(() -> controller.webhook("github", headers,
                githubPush("master", "https://github.com/org/defs.git")))
                .isInstanceOf(ResponseStatusException.class)
                .hasFieldOrPropertyWithValue("statusCode", HttpStatus.UNAUTHORIZED);
        verify(importUseCase, after(300).never()).handle(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void reloads_the_task_contracts_too_and_before_the_workflows() {
        when(taskProperties.getRepositories()).thenReturn(List.of(tasksMaster,
                taskRepo("https://github.com/org/other-tasks.git", "master")));

        controller.webhook("github", new HttpHeaders(), githubPush("master", "https://github.com/org/defs.git"));

        var order = inOrder(taskImportUseCase, importUseCase);
        ArgumentCaptor<List<TaskContractGitImportProperties.GitRepository>> tasks = ArgumentCaptor.forClass(List.class);
        order.verify(taskImportUseCase, timeout(2000)).handle(tasks.capture());
        order.verify(importUseCase, timeout(2000)).handle(anyList());
        assertThat(tasks.getValue()).containsExactly(tasksMaster);
    }

    @Test
    @SuppressWarnings("unchecked")
    void a_push_to_a_task_contracts_only_repository_reloads_just_the_contracts() {
        var tasksOnly = taskRepo("https://github.com/org/task-defs.git", "main");
        when(taskProperties.getRepositories()).thenReturn(List.of(tasksOnly));

        var response = controller.webhook("github", new HttpHeaders(),
                githubPush("main", "https://github.com/org/task-defs.git"));

        assertThat(response.getBody()).doesNotContain("ignored");
        ArgumentCaptor<List<TaskContractGitImportProperties.GitRepository>> tasks = ArgumentCaptor.forClass(List.class);
        verify(taskImportUseCase, timeout(2000)).handle(tasks.capture());
        assertThat(tasks.getValue()).containsExactly(tasksOnly);
        verify(importUseCase, after(400).never()).handle(any());
    }

    @Test
    void with_only_task_contract_repositories_configured_it_still_reloads_them() {
        when(properties.getRepositories()).thenReturn(List.of());
        when(taskProperties.getRepositories()).thenReturn(List.of(tasksMaster));

        controller.webhook("github", new HttpHeaders(), "not a json body".getBytes(StandardCharsets.UTF_8));

        verify(taskImportUseCase, timeout(2000)).handle(List.of(tasksMaster));
        verify(importUseCase, after(400).never()).handle(any());
    }
}
