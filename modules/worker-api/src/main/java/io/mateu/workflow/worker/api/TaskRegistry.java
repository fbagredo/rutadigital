package io.mateu.workflow.worker.api;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The task registrations this service serves, keyed by {@code <id>@<version>}.
 *
 * <p>Lookup precedence for a dispatched task (see {@link #resolve(String, String)}):
 * <ol>
 *   <li><b>taskId with a version</b> ({@code booking@2}) — the exact registration. What the engine
 *       sends for a step that declares {@code task: booking} (pinned to a version at import) or
 *       {@code task: booking@2}. A version this service does not serve resolves to nothing: it is
 *       never swapped for another version, whose input or output may differ.</li>
 *   <li><b>taskId without a version</b> ({@code booking}) — the highest version registered for that
 *       contract id.</li>
 *   <li><b>stepId</b>, only when the taskId is blank — the engine sends a blank taskId for an ACTION
 *       with no {@code task:} reference. The stepId is matched like a taskId: {@code <id>@<version>}
 *       exactly, otherwise as a contract id (highest version). So a step {@code id: booking} with no
 *       {@code task:} is served by the {@code booking} handler.</li>
 * </ol>
 * A taskId that is set but not served never falls back to the stepId.
 */
public final class TaskRegistry {

    private final Map<String, TaskRegistration<?, ?>> byRef = new LinkedHashMap<>();
    private final Map<String, TaskRegistration<?, ?>> latestById = new LinkedHashMap<>();

    public TaskRegistry(List<? extends TaskRegistration<?, ?>> registrations) {
        for (var registration : registrations) {
            var previous = byRef.put(registration.ref(), registration);
            if (previous != null) {
                throw new IllegalStateException("Two task handlers are registered for '"
                        + registration.ref() + "'.");
            }
            latestById.merge(registration.id(), registration, (a, b) ->
                    Comparator.<TaskRegistration<?, ?>>comparingInt(TaskRegistration::version)
                            .compare(a, b) >= 0 ? a : b);
        }
    }

    /** The handler for a dispatched task, or null when none serves it. See the class doc for precedence. */
    public TaskRegistration<?, ?> resolve(String taskId, String stepId) {
        if (taskId != null && !taskId.isBlank()) {
            return lookup(taskId.trim());
        }
        return stepId == null || stepId.isBlank() ? null : lookup(stepId.trim());
    }

    /**
     * A registration by contract reference: {@code <id>@<version>} exactly, or a bare {@code <id>}
     * as its highest registered version. Null when not served.
     */
    public TaskRegistration<?, ?> lookup(String ref) {
        if (ref == null || ref.isBlank()) {
            return null;
        }
        var at = ref.lastIndexOf('@');
        if (at > 0 && isVersion(ref.substring(at + 1))) {
            return byRef.get(ref);
        }
        return latestById.get(ref);
    }

    private static boolean isVersion(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (var i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public List<String> refs() {
        return List.copyOf(byRef.keySet());
    }

    public boolean isEmpty() {
        return byRef.isEmpty();
    }
}
