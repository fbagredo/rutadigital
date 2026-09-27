package io.mateu.workflow.uie2e.pages;

import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;

/** The process list: the grid an operator scans, its search box and its toolbar. */
public class ProcessesPage {

    private final Page page;

    public ProcessesPage(Page page) {
        this.page = page;
    }

    /**
     * Waits for the list to be the page on screen.
     *
     * <p>Keyed on the heading, not on the word: the menu carries a hidden item that also reads
     * "Processes", so matching text anywhere resolves to the closed submenu and waits twenty
     * seconds for something that is never going to be visible.
     */
    public void awaitLoaded() {
        assertThat(page.getByRole(com.microsoft.playwright.options.AriaRole.HEADING,
                new Page.GetByRoleOptions().setName("Processes"))).isVisible();
        assertThat(grid()).isVisible();
    }

    public Locator grid() {
        return page.locator("vaadin-grid").first();
    }

    /** Every rendered cell of the grid, as text. Vaadin virtualises rows, so this is what is on screen. */
    public Locator cells() {
        return grid().locator("vaadin-grid-cell-content");
    }

    /**
     * The cell carrying the given text — a process id, a workflow name, a status.
     *
     * <p>Deliberately not filtered to visible cells. {@code vaadin-grid} slots its cell contents
     * into the shadow DOM, and a slotted element does not report the bounding box Playwright's
     * visibility check wants, so {@code :visible} matches nothing here even when the row is
     * plainly on screen. Use {@link #itemCount()} to assert what the grid is <em>not</em> showing;
     * this one answers "is it there".
     */
    public Locator rowContaining(String text) {
        return cells().filter(new Locator.FilterOptions().setHasText(text)).first();
    }

    /** Types into the search box, which filters the grid server-side. */
    public ProcessesPage search(String term) {
        var search = page.getByPlaceholder("Search").first();
        search.fill(term);
        search.press("Enter");
        return this;
    }

    /**
     * The id cell of a process, wherever it is on the page.
     *
     * <p>Keyed on {@code data-uuid}, not on the text: Mateu (since 3.0-alpha.365) draws a UUID cell
     * as its last block — "…-3be632fdb9fe" — and keeps the whole UUID in the span's
     * {@code data-uuid} and tooltip. The full id is therefore not text on the page any more.
     */
    public Locator idCell(String processId) {
        return page.locator("[data-uuid=\"" + processId + "\"]").first();
    }

    /**
     * Opens a process by its id, which is what the grid renders as the link into the detail. The
     * other cells are plain text and clicking them does nothing — the same thing an operator finds
     * out by trying.
     */
    public ProcessDetailPage open(String processId) {
        grid().locator("[data-uuid=\"" + processId + "\"]").first().click();
        var detail = new ProcessDetailPage(page);
        detail.awaitLoaded();
        return detail;
    }

    /** The column headings — the shape of the list an operator relies on. */
    public Locator column(String heading) {
        return cells().filter(new Locator.FilterOptions().setHasText(heading)).first();
    }

    /** The "N items" footer the grid renders under itself. */
    public Locator itemCount() {
        return page.getByText(java.util.regex.Pattern.compile("\\d+ items?"));
    }
}
