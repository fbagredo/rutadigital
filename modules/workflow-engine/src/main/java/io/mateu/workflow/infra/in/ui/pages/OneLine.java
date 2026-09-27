package io.mateu.workflow.infra.in.ui.pages;

/**
 * Cuts a long text down to what fits a listing cell on one line.
 *
 * <p>Descriptions are written to be read, not scanned: several sentences, sometimes a folded YAML
 * block with line breaks in it. As a column they stretched the row to three or four lines and
 * pushed the columns that identify it off the screen. The listing shows this cut instead, and the
 * whole text opens under the row ({@code @Details} on the row), so nothing is lost — only moved.
 *
 * <p>The cut is at a word boundary when there is one in the second half of the budget, so a word is
 * not broken in two; a single word longer than that is cut where the budget ends. Whitespace runs
 * (the line breaks of a folded block included) collapse to one space first, because a cell shows
 * one line whatever the text says.
 */
public final class OneLine {

    /** About what a listing column shows on one line at a desktop width. */
    public static final int LENGTH = 70;

    private OneLine() {
    }

    /** The text on one line, cut to {@link #LENGTH} with "…" when longer; null stays null. */
    public static String of(String text) {
        return of(text, LENGTH);
    }

    static String of(String text, int length) {
        if (text == null) {
            return null;
        }
        var line = text.strip().replaceAll("\\s+", " ");
        if (line.length() <= length) {
            return line;
        }
        var cut = line.lastIndexOf(' ', length);
        return line.substring(0, cut > length / 2 ? cut : length).strip() + "…";
    }
}
