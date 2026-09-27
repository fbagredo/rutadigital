package io.mateu.workflow.infra.out.persistence;

import io.mateu.workflow.application.out.AnalyticsAggregates;
import io.mateu.workflow.application.out.StepExecutionAnalyticsRow;
import io.mateu.workflow.paging.ServedPage;
import io.mateu.workflow.application.out.StepExecutionRepository;
import io.mateu.workflow.application.out.StepExecutionSummary;
import io.mateu.workflow.application.out.StepExecutionSummaryPage;
import io.mateu.workflow.domain.aggregates.*;
import io.mateu.workflow.domain.aggregates.Process;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static io.mateu.workflow.domain.aggregates.StepExecutionStatus.AWAITING_RETRY;
import static io.mateu.workflow.domain.aggregates.StepExecutionStatus.PENDING;
import static io.mateu.workflow.domain.aggregates.StepExecutionStatus.RUNNING;

import static io.mateu.core.infra.JsonSerializer.listFromJson;
import static io.mateu.core.infra.JsonSerializer.toJson;

@Service
@ConditionalOnProperty(name = "workflow.persistence", havingValue = "jpa")
@RequiredArgsConstructor
public class StepExecutionDBRepository implements StepExecutionRepository {

    final StepExecutionEntityRepository stepExecutionEntityRepository;
    final OutboxMessageEntityRepository outboxMessageEntityRepository;
    final io.mateu.workflow.application.out.WorkflowTracing workflowTracing;
    final io.mateu.workflow.infra.out.async.OutboxSignal outboxSignal;
    // Present only when the read model is switched on (workflow.analytics.rollup=true); empty
    // otherwise, and then aggregateSteps falls through to the GROUP BY below unchanged.
    final org.springframework.beans.factory.ObjectProvider<
            io.mateu.workflow.application.out.RollupAnalyticsPort> rollupAnalytics;

    @Override
    public Optional<StepExecution> findById(String id) {
        return stepExecutionEntityRepository.findById(id).map(this::map);
    }

    private StepExecution map(StepExecutionEntity entity) {
        return new StepExecution(
                entity.getId(),
                entity.getProcessId(),
                entity.getWorkflowDefinitionId(),
                entity.getStepId(),
                entity.getStepJson(),
                listFromJson(entity.getVariables(), Variable.class),
                StepExecutionStatus.valueOf(entity.getStatus()),
                entity.getWorkerId(),
                entity.getOrder(),
                entity.getStartedAt(),
                entity.getFinishedAt(),
                entity.getAttemptCount(),
                entity.getDeadlineAt(),
                entity.getAwaitingMessageName(),
                entity.getAwaitingCorrelationKey(),
                entity.getVersion(),
                entity.getInjectedByStepExecutionId()
        );
    }

    @Override
    public String save(StepExecution stepExecution) {
        stepExecutionEntityRepository.save(new StepExecutionEntity(
            stepExecution.id(),
                stepExecution.getProcessId(),
                stepExecution.getWorkflowDefinitionId(),
                stepExecution.getStepId(),
                stepExecution.getStepJson(),
                stepExecution.stepTypeName(),
                toJson(stepExecution.getVariables()),
                stepExecution.getStatus().name(),
                stepExecution.getWorkerId(),
                stepExecution.getOrder(),
                stepExecution.getStartedAt(),
                stepExecution.getFinishedAt(),
                stepExecution.getAttemptCount(),
                stepExecution.getDeadlineAt(),
                stepExecution.getAwaitingMessageName(),
                stepExecution.getAwaitingCorrelationKey(),
                stepExecution.getVersion(),
                stepExecution.getInjectedByStepExecutionId()
        ));

        // Captured here, at the one moment the event and the context that produced it are both
        // in hand: the relay publishes this row later, from a thread that has neither.
        var traceContext = workflowTracing.currentTraceContext();
        var outbox = stepExecution.popEvents().stream()
                .map(event -> {
                    var row = new OutboxMessageEntity(event, traceContext);
                    // Driving this process inline (the synchronous fast path): its own rows are
                    // written already claimed by this pod, so no relay races the driver for them.
                    var claim = io.mateu.workflow.infra.out.async.InlineDrive.claimFor(event);
                    return claim == null ? row : row.claimedBy(claim.processId(), claim.pod(), claim.claimUntil());
                }).toList();
        outboxMessageEntityRepository.saveAll(outbox);
        if (outbox.stream().anyMatch(row -> OutboxMessageStatus.Pending.name().equals(row.getStatus()))) {
            // Wake this pod's relay once the transaction commits, rather than leaving the row to
            // be found on the next poll — which is latency added to every step.
            outboxSignal.raise();
        }

        return stepExecution.id();
    }

    @Override
    public List<StepExecution> findAll() {
        return stepExecutionEntityRepository.findAll().stream().map(this::map).toList();
    }

    @Override
    public void deleteAllById(List<String> selectedIds) {
        stepExecutionEntityRepository.deleteAllById(selectedIds);
    }

    @Override
    public List<StepExecution> findByProcessId(String processId) {
        return stepExecutionEntityRepository.findAllByProcessIdOrderByOrder(processId).stream()
                .map(this::map).toList();
    }

    @Override
    public List<StepExecution> findPendingOrRunning() {
        return stepExecutionEntityRepository
                .findAllByStatusIn(List.of(PENDING.name(), RUNNING.name()))
                .stream().map(this::map).toList();
    }

    @Override
    public List<StepExecution> findPendingOrRunningByProcessId(String processId) {
        return stepExecutionEntityRepository
                .findAllByProcessIdAndStatusIn(processId, List.of(PENDING.name(), RUNNING.name()))
                .stream().map(this::map).toList();
    }

    @Override
    public List<StepExecution> findDue(LocalDateTime now) {
        // AWAITING_RETRY joins the scan so a step waiting out its backoff is woken the same way a
        // TIMER is; classification (timeout vs timer vs retry) happens by status/type downstream.
        return stepExecutionEntityRepository
                .findAllByStatusInAndDeadlineAtLessThanEqual(
                        List.of(PENDING.name(), RUNNING.name(), AWAITING_RETRY.name()), now)
                .stream().map(this::map).toList();
    }

    @Override
    public List<StepExecution> findDueByProcessId(String processId, LocalDateTime now) {
        // Intentionally PENDING/RUNNING only: this feeds the timeout check, which must never see an
        // AWAITING_RETRY step (its startedAt is the failed attempt's and would read as expired).
        return stepExecutionEntityRepository
                .findAllByProcessIdAndStatusInAndDeadlineAtLessThanEqual(
                        processId, List.of(PENDING.name(), RUNNING.name()), now)
                .stream().map(this::map).toList();
    }

    @Override
    public List<StepExecution> findDueRetriesByProcessId(String processId, LocalDateTime now) {
        return stepExecutionEntityRepository
                .findAllByProcessIdAndStatusInAndDeadlineAtLessThanEqual(
                        processId, List.of(AWAITING_RETRY.name()), now)
                .stream().map(this::map).toList();
    }

    @Override
    public List<StepExecution> findWaitingForMessage(String messageName, String correlationKey) {
        if (correlationKey == null) {
            // SQL equality never matches null; short-circuit rather than issue a query that
            // cannot return anything.
            return List.of();
        }
        return stepExecutionEntityRepository
                .findAllByStatusAndAwaitingMessageNameAndAwaitingCorrelationKey(
                        PENDING.name(), messageName, correlationKey)
                .stream().map(this::map).toList();
    }

    /**
     * The step types whose waiting is machine work: a request went out to a worker and an answer
     * is owed. The same two the fallback deadline is applied to (see {@code StepTimeoutDefaults}),
     * because it is the same question — everything else waits without a deadline on purpose.
     */
    private static final List<String> AWAITING_A_WORKER =
            List.of(StepType.ACTION.name(), StepType.RULE.name(), StepType.HTTP_CALL.name());

    @Override
    public long countStalled(LocalDateTime startedBefore) {
        return stepExecutionEntityRepository.countStalled(
                List.of(PENDING.name(), RUNNING.name()), startedBefore, AWAITING_A_WORKER);
    }

    /**
     * Two {@code GROUP BY}s over a join, instead of the engine's largest table.
     *
     * <p>345 564 step executions were loaded to produce a grid of a few rows, once per workflow
     * definition on the page. The join is only for the window — the grouping is by the step
     * execution's own definition id.
     */
    @Override
    public AnalyticsAggregates.StepAggregates aggregateSteps(LocalDateTime from, LocalDateTime to) {
        var rollup = rollupAnalytics.getIfAvailable();
        if (rollup != null) {
            return rollup.aggregateSteps(from, to);
        }
        // No window means no reason to join: the join reaches p.created and nothing else, so with
        // both bounds open it filters nothing and costs a third of the query. Measured on the
        // reference deployment, 4 521 ms against 2 993 ms over 2 714 697 rows.
        //
        // This is the common case, not an edge one — the page's default window is open, and on a
        // deployment whose processes were all created on one day a narrower default would return
        // exactly the same rows anyway.
        if (from == null && to == null) {
            return aggregateStepsAllTime();
        }
        var counts = stepExecutionEntityRepository.aggregateStepCounts(from, to).stream()
                .map(v -> new AnalyticsAggregates.DefinitionStepCount(
                        v.getDefinitionId(), v.getStepId(),
                        StepExecutionStatus.valueOf(v.getStatus()), v.getCount(), v.getFirstOrder()))
                .toList();
        var durations = stepExecutionEntityRepository.aggregateStepDurations(from, to).stream()
                .map(v -> new AnalyticsAggregates.DefinitionStepDuration(
                        v.getDefinitionId(), v.getStepId(),
                        new AnalyticsAggregates.DurationAggregate(
                                v.getSamples(), v.getTotalNanos(), v.getP95Nanos())))
                .toList();
        return new AnalyticsAggregates.StepAggregates(counts, durations);
    }

    /** The same report with the window open, off the join-free queries. */
    private AnalyticsAggregates.StepAggregates aggregateStepsAllTime() {
        var counts = stepExecutionEntityRepository.aggregateStepCountsAllTime().stream()
                .map(v -> new AnalyticsAggregates.DefinitionStepCount(
                        v.getDefinitionId(), v.getStepId(),
                        StepExecutionStatus.valueOf(v.getStatus()), v.getCount(), v.getFirstOrder()))
                .toList();
        var durations = stepExecutionEntityRepository.aggregateStepDurationsAllTime().stream()
                .map(v -> new AnalyticsAggregates.DefinitionStepDuration(
                        v.getDefinitionId(), v.getStepId(),
                        new AnalyticsAggregates.DurationAggregate(
                                v.getSamples(), v.getTotalNanos(), v.getP95Nanos())))
                .toList();
        return new AnalyticsAggregates.StepAggregates(counts, durations);
    }

    /**
     * Joined to the process so the window narrows the scan, and projected down to six columns. The
     * default would have loaded the whole table — and analytics called it once per definition.
     */
    @Override
    public List<StepExecutionAnalyticsRow> findAnalyticsRows(LocalDateTime processCreatedFrom,
                                                             LocalDateTime processCreatedTo) {
        return stepExecutionEntityRepository.findAnalyticsRows(processCreatedFrom, processCreatedTo).stream()
                .map(view -> new StepExecutionAnalyticsRow(
                        view.getProcessId(),
                        view.getDefinitionId(),
                        view.getStepId(),
                        StepExecutionStatus.valueOf(view.getStatus()),
                        view.getOrder(),
                        view.getStartedAt(),
                        view.getFinishedAt()))
                .toList();
    }

    /**
     * Pushed all the way down to SQL, unlike the in-memory default: this is the largest table the
     * engine writes, and each row carries the step's JSON and variables that the listing never
     * shows.
     */
    @Override
    public StepExecutionSummaryPage searchSummaries(String searchText, boolean onlyErrors, int page, int size) {
        var pattern = (searchText == null || searchText.isBlank())
                ? null : "%" + searchText.toLowerCase() + "%";
        // Counted first: which page can be served depends on how many there are — see ServedPage.
        var total = stepExecutionEntityRepository.countSummaries(onlyErrors, pattern);
        var served = ServedPage.of(page, size, total);
        var content = stepExecutionEntityRepository
                .searchSummaries(onlyErrors, pattern, PageRequest.of(served.number(), served.size()))
                .stream()
                .map(view -> new StepExecutionSummary(
                        view.getId(),
                        view.getProcessId(),
                        view.getStepId(),
                        StepExecutionStatus.valueOf(view.getStatus()),
                        view.getStartedAt(),
                        view.getAttemptCount()))
                .toList();
        return new StepExecutionSummaryPage(content, total, served.number(), served.size());
    }

    @Override
    public java.util.Map<String, Integer> countStoppedByStep(String workflowDefinitionId) {
        return stepExecutionEntityRepository.countStoppedByStep(workflowDefinitionId).stream()
                .collect(java.util.stream.Collectors.toMap(
                        StepExecutionEntityRepository.StoppedStepCountView::getStepId,
                        view -> (int) view.getCount()));
    }

}
