package io.mateu.workflow.infra.in.ui.pages.taskcontracts;

import io.mateu.uidl.annotations.Details;
import io.mateu.uidl.interfaces.Identifiable;

/**
 * A row in the Tasks view: one task contract version, how many workflow definitions reference it,
 * and the URLs a browser downloads the generated worker project from (served by
 * {@code TaskProjectController}).
 *
 * <p>The description column is cut to one line ({@code OneLine}); the whole of it is the row's
 * {@link Details}, opened under the row by a click, so a long contract description does not make
 * every row several lines tall.
 */
public record TaskContractRow(
        String id,           // the contract ref, <id>@<version>
        String group,
        String topic,
        String description,  // on one line, cut at a word with "…"
        int workflows,       // how many workflow definitions use this version
        String moduleZip,    // download URL for the task-module project
        String serviceZip,   // download URL for the standalone service project
        @Details
        String fullDescription) // the description as written: the row detail
        implements Identifiable {
}
