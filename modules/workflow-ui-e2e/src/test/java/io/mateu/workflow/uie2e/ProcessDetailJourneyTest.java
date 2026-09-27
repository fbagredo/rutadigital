package io.mateu.workflow.uie2e;

import io.mateu.workflow.uie2e.support.AbstractUiE2eTest;
import org.junit.jupiter.api.Test;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;

/**
 * One process, opened the way an operator opens it: from the list, by clicking the row.
 *
 * <p>This is the screen someone reaches when something has gone wrong, so what it has to get right
 * is the correspondence between what the database holds and what the page says — a graph whose
 * nodes are the steps that ran, an Errors tab with the reason in it, and a status badge that
 * matches the outcome.
 */
class ProcessDetailJourneyTest extends AbstractUiE2eTest {

    @Test
    void openingAProcessShowsItsIdentityAndOutcome() {
        var id = startProcess("ui-greeting", "greeting-detail");

        var detail = ui.goToProcesses().open(id);

        assertThat(detail.statusBadge()).containsText("Completed");
        assertThat(detail.name()).hasValue("UI Greeting");
    }

    @Test
    void theDetailOffersEveryTabTheEngineDeclares() {
        var id = startProcess("ui-greeting", "greeting-tabs");

        var detail = ui.goToProcesses().open(id);

        for (var name : java.util.List.of("Diagram", "Steps", "Messages", "Errors", "Resources", "Variables")) {
            assertThat(detail.tab(name)).isVisible();
        }
    }

    /**
     * The graph is the one part of this UI with no equivalent anywhere else — the API can tell you
     * a step failed, only this says where in the flow it sat.
     */
    @Test
    void theDiagramDrawsTheStepsOfTheDefinition() {
        var id = startProcess("ui-greeting", "greeting-diagram");

        var detail = ui.goToProcesses().open(id);
        detail.openTab("Diagram");

        assertThat(detail.node("Start")).isVisible();
        assertThat(detail.node("Greet the user")).isVisible();
        assertThat(detail.node("Done")).isVisible();
    }

    @Test
    void theStepsTabListsWhatActuallyRan() {
        var id = startProcess("ui-greeting", "greeting-steps");

        var detail = ui.goToProcesses().open(id);
        detail.openTab("Steps");

        // The tab lists steps by their definition id, not by the name the diagram draws.
        assertThat(detail.tabShows("greet")).isVisible();
    }

    /**
     * The saga's charge step fails, so the process rolls back. Three things have to line up: the
     * badge says COMPENSATED rather than ERROR, the compensation step appears among the steps, and
     * the Errors tab carries the reason the worker gave.
     */
    @Test
    void aRolledBackSagaShowsItsCompensationAndTheReasonItFailed() {
        var id = startProcess("ui-saga", "saga-detail");

        var detail = ui.goToProcesses().open(id);

        assertThat(detail.statusBadge()).containsText("Compensated");

        detail.openTab("Steps");
        assertThat(detail.tabShows("cancelReservation")).isVisible();

        detail.openTab("Errors");
        assertThat(detail.tabShows("The card was declined")).isVisible();
    }

    @Test
    void backToListReturnsToTheProcessList() {
        var id = startProcess("ui-greeting", "greeting-back");

        var processes = ui.goToProcesses().open(id).backToList();

        // Asserted on the id rather than on the workflow name: the detail view leaves its own
        // grid (the Steps tab) in the DOM, so "the first vaadin-grid" is not reliably the list's
        // once you have been somewhere else. The id appears in one place only (as its abbreviated
        // cell, which carries the whole id in data-uuid).
        assertThat(processes.itemCount()).containsText("1 item");
        assertThat(processes.idCell(id)).isVisible();
    }
}
