package io.mateu.workflow.infra.out.persistence;

/**
 * The single-column primary key of a {@code process_lock} row, built from {@code (lockName, lockKey)}.
 *
 * <p>Format: {@code <length of lockName>:<lockName>:<lockKey>} — e.g. {@code booking/MRU01/ABC123}
 * under the name {@code booking} becomes {@code 7:booking:MRU01/ABC123}. The length prefix says
 * exactly where the name ends, so no character inside either part needs escaping and no two distinct
 * pairs can collide: {@code ("a:b", "c")} is {@code 3:a:b:c} and {@code ("a", "b:c")} is
 * {@code 1:a:b:c}. Every character is printable, which is the point: the previous encoding joined the
 * two with a NUL, and PostgreSQL refuses NUL in any text value, so no lock could ever be taken there.
 *
 * <p>The id is only the mutex (a second INSERT of the same pair fails on it); every read that needs
 * the name or the key reads the {@code lock_name}/{@code lock_key} columns, never this.
 */
public final class LockRowId {

    /** The encoding that preceded this one ({@code lockName + '\0' + lockKey}); only ever written on H2. */
    static final char LEGACY_SEPARATOR = '\u0000';

    private LockRowId() {
    }

    public static String of(String lockName, String lockKey) {
        return lockName.length() + ":" + lockName + ":" + lockKey;
    }

    /** Whether {@code id} was written by the NUL-joined encoding and has to be rewritten. */
    static boolean isLegacy(String id) {
        return id != null && id.indexOf(LEGACY_SEPARATOR) >= 0;
    }
}
