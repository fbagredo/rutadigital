package io.mateu.workflow.infra.in.async.processupstreamevent.upstreameventhandlers;

import io.mateu.workflow.application.usecases.process.create.CreateProcessUseCase;
import io.mateu.workflow.dtos.Variable;
import io.mateu.workflow.dtos.events.integration.ProcessCreationRequested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ProcessCreationRequestedEventHandlerTest {

    @Mock CreateProcessUseCase createProcessUseCase;
    @Mock io.mateu.workflow.application.out.WorkflowTracing workflowTracing;

    @InjectMocks ProcessCreationRequestedEventHandler handler;

    @Test
    void delegatesToCreateProcessUseCase() {
        handler.handle(new ProcessCreationRequested("wd-1", "BK-1", List.of(new Variable("k", "v"))));
        verify(createProcessUseCase).handle(any());
    }

    @Test
    void theCallersTraceContextReachesTheCreationSanitised() {
        var traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        handler.handle(new ProcessCreationRequested("wd-1", "BK-1", List.of(), null, null,
                new io.mateu.workflow.dtos.TraceContext(" " + traceparent + " ", null, "k=v")));

        var command = org.mockito.ArgumentCaptor.forClass(
                io.mateu.workflow.application.usecases.process.create.CreateProcessCommand.class);
        verify(createProcessUseCase).handle(command.capture());
        org.assertj.core.api.Assertions.assertThat(command.getValue().traceContext())
                .isEqualTo(new io.mateu.workflow.dtos.TraceContext(traceparent, null, "k=v"));
    }

    @Test
    void aMalformedTraceContextIsDroppedNotKept() {
        handler.handle(new ProcessCreationRequested("wd-1", "BK-1", List.of(), null, null,
                new io.mateu.workflow.dtos.TraceContext("not-a-traceparent", null, null)));

        var command = org.mockito.ArgumentCaptor.forClass(
                io.mateu.workflow.application.usecases.process.create.CreateProcessCommand.class);
        verify(createProcessUseCase).handle(command.capture());
        org.assertj.core.api.Assertions.assertThat(command.getValue().traceContext()).isNull();
    }
}
