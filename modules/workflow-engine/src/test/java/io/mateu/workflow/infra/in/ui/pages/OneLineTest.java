package io.mateu.workflow.infra.in.ui.pages;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OneLineTest {

    @Test
    void a_short_text_is_left_as_it_is() {
        assertThat(OneLine.of("Books a room")).isEqualTo("Books a room");
    }

    @Test
    void null_stays_null() {
        assertThat(OneLine.of(null)).isNull();
    }

    @Test
    void a_long_text_is_cut_at_a_word_with_an_ellipsis() {
        var text = "Receives a reservation from the channel manager, maps its codes to the PMS ones and books it";

        var line = OneLine.of(text);

        assertThat(line).endsWith("…").hasSizeLessThanOrEqualTo(OneLine.LENGTH + 1);
        assertThat(text).startsWith(line.substring(0, line.length() - 1));
        // cut between words, never inside one
        assertThat(text.charAt(line.length() - 1)).isEqualTo(' ');
    }

    @Test
    void line_breaks_of_a_folded_block_become_spaces() {
        assertThat(OneLine.of("First line\n  second line\tthird")).isEqualTo("First line second line third");
    }

    @Test
    void a_single_word_longer_than_the_budget_is_cut_where_the_budget_ends() {
        var word = "x".repeat(100);
        assertThat(OneLine.of(word)).isEqualTo("x".repeat(OneLine.LENGTH) + "…");
    }

    @Test
    void a_text_exactly_as_long_as_the_budget_is_not_cut() {
        var text = "y".repeat(OneLine.LENGTH);
        assertThat(OneLine.of(text)).isEqualTo(text);
    }
}
