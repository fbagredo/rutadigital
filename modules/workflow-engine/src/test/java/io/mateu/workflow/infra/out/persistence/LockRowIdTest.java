package io.mateu.workflow.infra.out.persistence;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LockRowIdTest {

    @Test
    void a_business_key_with_slashes_encodes_readably() {
        assertThat(LockRowId.of("booking", "MRU01/ABC123")).isEqualTo("7:booking:MRU01/ABC123");
    }

    @Test
    void the_id_carries_no_nul_so_postgresql_accepts_it() {
        assertThat(LockRowId.of("proyectar-reserva", "MRU01/ABC123")).doesNotContain("\u0000");
        assertThat(LockRowId.isLegacy(LockRowId.of("a", "b"))).isFalse();
        assertThat(LockRowId.isLegacy("a\u0000b")).isTrue();
    }

    @Test
    void the_separator_inside_a_name_or_key_cannot_make_two_pairs_collide() {
        assertThat(LockRowId.of("a:b", "c")).isNotEqualTo(LockRowId.of("a", "b:c"));
        assertThat(LockRowId.of("a", "1:b")).isNotEqualTo(LockRowId.of("a:1", "b"));
        assertThat(LockRowId.of("1", "")).isNotEqualTo(LockRowId.of("", "1"));
        // Digits in the name do not confuse the length prefix either.
        assertThat(LockRowId.of("12", "x")).isNotEqualTo(LockRowId.of("1", "2:x"));
    }

    @Test
    void empty_and_unicode_parts_are_measured_in_characters() {
        assertThat(LockRowId.of("", "")).isEqualTo("0::");
        assertThat(LockRowId.of("reserva-ñ", "Ü/1")).isEqualTo("9:reserva-ñ:Ü/1");
    }

    @Test
    void the_longest_pair_the_columns_allow_fits_the_id_column() {
        String name = "n".repeat(255);
        String key = "k".repeat(255);
        assertThat(LockRowId.of(name, key)).hasSizeLessThanOrEqualTo(520);
    }
}
