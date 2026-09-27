package io.mateu.workflow.infra.in.rest;

import io.mateu.workflow.application.usecases.gitimport.ImportWorkflowDefinitionsFromGitUseCase;
import io.mateu.workflow.application.usecases.taskcontractimport.ImportTasksFromGitUseCase;
import io.mateu.workflow.infra.config.GitImportProperties;
import io.mateu.workflow.infra.config.TaskContractGitImportProperties;
import io.mateu.workflow.webhook.GitPushPayload;
import io.mateu.workflow.webhook.GitPushPayloadParser;
import io.mateu.workflow.webhook.RepositoryUrlMatcher;
import io.mateu.workflow.webhook.WebhookProvider;
import io.mateu.workflow.webhook.WebhookSignatureVerifier;
import io.mateu.workflow.webhook.WebhookSignatureVerifier.WebhookVerificationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Webhook endpoint triggered by a git provider after a push/merge.
 * {@code POST /workflow/webhooks/{provider}} — re-imports the matching configured repositories.
 *
 * <p>{@code provider} is one of {@code github}, {@code gitlab}, {@code bitbucket} or
 * {@code generic} (unknown values are treated as generic). {@code /github} keeps the original
 * behaviour. The payload is parsed to reload only the repository and branch that changed;
 * a push to a repository/branch that no configuration cares about is acknowledged and ignored.
 *
 * <p>Both kinds of definition a push can change are reloaded: the task contracts configured under
 * {@code tasks.git-import} and the workflows under {@code workflow.git-import}, matched against the
 * push the same way. <b>Contracts first</b>, then workflows, in the same background run — a
 * workflow step's {@code task:} reference is resolved (and pinned to a version) at import, so the
 * contracts it references must already be there. The signature is checked with
 * {@code workflow.git-import.webhook-secret}, or {@code tasks.git-import.webhook-secret} when only
 * that one is set.
 *
 * <pre>
 *   workflow:
 *     git-import:
 *       webhook-secret: mysecret          # GitHub/Bitbucket HMAC, GitLab/generic token
 *       repositories:
 *         - url: https://github.com/org/workflow-defs.git
 *           branch: master
 * </pre>
 */
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@RestController
@RequestMapping("/workflow/webhooks")
@Slf4j
public class GitImportWebhookController {

    final GitImportProperties gitImportProperties;
    final ImportWorkflowDefinitionsFromGitUseCase importUseCase;
    final TaskContractGitImportProperties taskGitImportProperties;
    final ImportTasksFromGitUseCase taskImportUseCase;

    @org.springframework.beans.factory.annotation.Autowired
    public GitImportWebhookController(GitImportProperties gitImportProperties,
                                      ImportWorkflowDefinitionsFromGitUseCase importUseCase,
                                      TaskContractGitImportProperties taskGitImportProperties,
                                      ImportTasksFromGitUseCase taskImportUseCase) {
        this.gitImportProperties = gitImportProperties;
        this.importUseCase = importUseCase;
        this.taskGitImportProperties = taskGitImportProperties;
        this.taskImportUseCase = taskImportUseCase;
    }

    /** Workflows only — no task-contract repositories. */
    public GitImportWebhookController(GitImportProperties gitImportProperties,
                                      ImportWorkflowDefinitionsFromGitUseCase importUseCase) {
        this(gitImportProperties, importUseCase, null, null);
    }

    /**
     * Provider-agnostic webhook receiver. Responds 202 immediately and runs the import in the
     * background so a provider's short delivery timeout is never hit, even for large repos.
     */
    @PostMapping("/{provider}")
    public ResponseEntity<String> webhook(
            @PathVariable String provider,
            @RequestHeader HttpHeaders headers,
            @RequestBody(required = false) byte[] body) {

        var webhookProvider = WebhookProvider.fromPath(provider);
        var payloadBytes = body == null ? new byte[0] : body;

        try {
            WebhookSignatureVerifier.verify(webhookProvider, webhookSecret(),
                    headers::getFirst, payloadBytes);
        } catch (WebhookVerificationException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, e.getMessage());
        }

        var repositories = gitImportProperties.getRepositories();
        var taskRepositories = taskRepositories();
        if (repositories.isEmpty() && taskRepositories.isEmpty()) {
            log.info("Webhook received but no Git repositories configured — nothing to import.");
            return ResponseEntity.accepted().body("no repositories configured");
        }

        var payload = GitPushPayloadParser.parse(webhookProvider, payloadBytes);
        var selected = select(repositories, payload,
                GitImportProperties.GitRepository::getUrl, GitImportProperties.GitRepository::getBranch);
        var selectedTasks = select(taskRepositories, payload,
                TaskContractGitImportProperties.GitRepository::getUrl,
                TaskContractGitImportProperties.GitRepository::getBranch);

        if (selected.isEmpty() && selectedTasks.isEmpty() && !payload.isEmpty()) {
            // We understood the payload but nothing configured matches this repo/branch push.
            log.info("Webhook ignored: no configured repository matches the push (branch={}, repos={}).",
                    payload.branch(), payload.repositoryUrls());
            return ResponseEntity.accepted().body("ignored: no configured repository matches this push");
        }

        // Fallback: an unparseable payload reloads everything (unchanged legacy behaviour).
        var toImport = payload.isEmpty() ? repositories : selected;
        var tasksToImport = payload.isEmpty() ? taskRepositories : selectedTasks;

        CompletableFuture.runAsync(() -> {
            // Task contracts before workflows: a step's `task:` reference resolves at import.
            if (!tasksToImport.isEmpty()) {
                importTaskContracts(webhookProvider, tasksToImport);
            }
            if (!toImport.isEmpty()) {
                importWorkflows(webhookProvider, toImport);
            }
        });

        return ResponseEntity.accepted().body("import scheduled for " + (toImport.size() + tasksToImport.size())
                + " repository/ies");
    }

    private void importTaskContracts(WebhookProvider provider,
                                     List<TaskContractGitImportProperties.GitRepository> repositories) {
        log.info("Webhook ({}) triggered: importing task contracts from {} repository/ies…",
                provider, repositories.size());
        try {
            var result = taskImportUseCase.handle(repositories);
            if (!result.imported().isEmpty()) {
                log.info("Webhook import: {} task contract(s) imported: {}", result.imported().size(), result.imported());
            }
            if (!result.errors().isEmpty()) {
                log.warn("Webhook import: {} task-contract error(s): {}", result.errors().size(), result.errors());
            }
        } catch (Exception e) {
            log.error("Webhook task-contract import failed: {}", e.getMessage(), e);
        }
    }

    private void importWorkflows(WebhookProvider provider, List<GitImportProperties.GitRepository> repositories) {
        log.info("Webhook ({}) triggered: importing {} repository/ies…", provider, repositories.size());
        try {
            var result = importUseCase.handle(repositories);
            if (!result.imported().isEmpty()) {
                log.info("Webhook import: {} definition(s) imported: {}", result.imported().size(), result.imported());
            }
            if (!result.pruned().isEmpty()) {
                log.info("Webhook import: {} definition(s) pruned (archived): {}", result.pruned().size(), result.pruned());
            }
            if (!result.errors().isEmpty()) {
                log.warn("Webhook import: {} error(s): {}", result.errors().size(), result.errors());
            }
        } catch (Exception e) {
            log.error("Webhook import failed: {}", e.getMessage(), e);
        }
    }

    private String webhookSecret() {
        var secret = gitImportProperties.getWebhookSecret();
        if ((secret == null || secret.isBlank()) && taskGitImportProperties != null) {
            return taskGitImportProperties.getWebhookSecret();
        }
        return secret;
    }

    private List<TaskContractGitImportProperties.GitRepository> taskRepositories() {
        if (taskGitImportProperties == null || taskImportUseCase == null
                || taskGitImportProperties.getRepositories() == null) {
            return List.of();
        }
        return taskGitImportProperties.getRepositories();
    }

    /**
     * The configured repositories the push applies to: URL matches (when the payload names a
     * repo) AND branch matches (when the payload names a branch). An empty result with a
     * non-empty payload means "understood but irrelevant"; an empty result with an empty
     * payload means "could not tell" and the caller reloads everything.
     */
    private static <R> List<R> select(List<R> repositories, GitPushPayload payload,
                                      Function<R, String> url, Function<R, String> branch) {
        if (payload.isEmpty() || repositories == null) {
            return List.of();
        }
        Predicate<R> repoMatches = repo -> matchesRepository(url.apply(repo), payload);
        Predicate<R> branchMatches = repo -> matchesBranch(branch.apply(repo), payload);
        return repositories.stream().filter(repoMatches).filter(branchMatches).toList();
    }

    private static boolean matchesRepository(String repoUrl, GitPushPayload payload) {
        if (payload.repositoryUrls() == null || payload.repositoryUrls().isEmpty()) {
            return true; // payload did not name a repo → don't exclude on that basis
        }
        return payload.repositoryUrls().stream()
                .anyMatch(url -> RepositoryUrlMatcher.sameRepository(url, repoUrl));
    }

    private static boolean matchesBranch(String repoBranch, GitPushPayload payload) {
        if (payload.branch() == null || payload.branch().isBlank()) {
            return true; // payload did not name a branch → don't exclude on that basis
        }
        return payload.branch().equals(repoBranch);
    }
}
