package io.mateu.workflow.infra.out.persistence;

import io.mateu.workflow.application.out.ProcessTraceContextRepository;
import io.mateu.workflow.dtos.TraceContext;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Optional;

@Service
@ConditionalOnProperty(name = "workflow.persistence", havingValue = "jpa")
@RequiredArgsConstructor
public class JpaProcessTraceContextRepository implements ProcessTraceContextRepository {

    final ProcessTraceContextEntityRepository repository;

    @Override
    public void save(String processId, TraceContext context) {
        if (processId == null || context == null) {
            return;
        }
        repository.save(new ProcessTraceContextEntity(processId, context.traceparent(), context.tracestate(),
                context.baggage()));
    }

    @Override
    public Optional<TraceContext> findByProcessId(String processId) {
        if (processId == null) {
            return Optional.empty();
        }
        // Through TraceContext.of, so a row edited by hand into something malformed reads as absent.
        return repository.findById(processId)
                .map(row -> TraceContext.of(row.getTraceParent(), row.getTraceState(), row.getBaggage()));
    }

    @Override
    public void deleteAllByProcessId(Collection<String> processIds) {
        if (processIds != null && !processIds.isEmpty()) {
            repository.deleteAllByIdInBatch(processIds);
        }
    }
}
