package io.mateu.workflow.infra.out.memory;

import io.mateu.workflow.application.out.ProcessTraceContextRepository;
import io.mateu.workflow.dtos.TraceContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
@ConditionalOnProperty(name = "workflow.persistence", havingValue = "memory", matchIfMissing = true)
public class InMemoryProcessTraceContextRepository implements ProcessTraceContextRepository {

    private final Map<String, TraceContext> contexts = new ConcurrentHashMap<>();

    @Override
    public void save(String processId, TraceContext context) {
        if (processId != null && context != null) {
            contexts.put(processId, context);
        }
    }

    @Override
    public Optional<TraceContext> findByProcessId(String processId) {
        return processId == null ? Optional.empty() : Optional.ofNullable(contexts.get(processId));
    }

    @Override
    public void deleteAllByProcessId(Collection<String> processIds) {
        processIds.forEach(contexts::remove);
    }
}
