package io.mateu.workflow.infra.in.ui.pages;

import static org.assertj.core.api.Assertions.assertThat;

import io.mateu.core.domain.out.componentmapper.PageListingBuilder;
import io.mateu.workflow.domain.aggregates.WorkflowDefinition;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkflowDefinitionRowTest {

    private static final String LONG = "Receives a reservation from the channel manager, maps its room and rate "
            + "codes to the PMS ones, books it and tells the guest; on a failure it compensates.";

    private static WorkflowDefinition definition(String description) {
        return new WorkflowDefinition("wf-1", "Book", 1, description, false, 0, false, null, 0, List.of());
    }

    @Test
    void the_description_column_is_one_line_and_the_whole_of_it_is_the_row_detail() {
        var row = WorkflowDefinitionRow.of(definition(LONG));

        assertThat(row.description()).isEqualTo(OneLine.of(LONG)).endsWith("…");
        assertThat(row.fullDescription()).isEqualTo(LONG);
    }

    @Test
    void a_short_description_is_shown_whole() {
        var row = WorkflowDefinitionRow.of(definition("Books a room"));

        assertThat(row.description()).isEqualTo("Books a room");
        assertThat(row.fullDescription()).isEqualTo("Books a room");
    }

    @Test
    void no_description_is_no_description() {
        var row = WorkflowDefinitionRow.of(definition(null));

        assertThat(row.description()).isNull();
        assertThat(row.fullDescription()).isNull();
    }

    @Test
    void the_full_description_is_the_listing_row_detail_not_a_column() {
        assertThat(PageListingBuilder.getDetailPath(WorkflowDefinitionRow.class)).isEqualTo("fullDescription");
    }
}
