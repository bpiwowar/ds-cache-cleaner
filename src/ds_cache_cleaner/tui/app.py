"""Textual TUI application for ds-cache-cleaner."""

from datetime import datetime
from enum import Enum

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, DataTable, Footer, Header, Label, Static

from ds_cache_cleaner.caches import CacheEntry, CacheHandler, get_all_handlers
from ds_cache_cleaner.utils import format_size


class SortColumn(Enum):
    """Columns that can be sorted."""

    NAME = "name"
    SIZE = "size"
    LAST_ACCESS = "last_access"


class ConfirmScreen(ModalScreen[bool]):
    """A modal screen to confirm deletion."""

    CSS = """
    ConfirmScreen {
        align: center middle;
    }

    #dialog {
        width: 60;
        height: auto;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }

    #dialog-title {
        text-style: bold;
        margin-bottom: 1;
    }

    #dialog-message {
        margin-bottom: 1;
    }

    #dialog-buttons {
        width: 100%;
        height: auto;
        align: center middle;
    }

    #dialog-buttons Button {
        margin: 0 1;
    }
    """

    def __init__(self, message: str, size_str: str) -> None:
        super().__init__()
        self.message = message
        self.size_str = size_str

    def compose(self) -> ComposeResult:
        with Container(id="dialog"):
            yield Label("Confirm Deletion", id="dialog-title")
            yield Label(self.message, id="dialog-message")
            yield Label(f"Total size: [bold]{self.size_str}[/bold]")
            with Horizontal(id="dialog-buttons"):
                yield Button("Delete", variant="error", id="confirm")
                yield Button("Cancel", variant="primary", id="cancel")

    @on(Button.Pressed, "#confirm")
    def confirm(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#cancel")
    def cancel(self) -> None:
        self.dismiss(False)


class EntriesScreen(Screen[None]):
    """Screen showing cache entries for a specific handler."""

    CSS = """
    #main-container {
        height: 100%;
    }

    #entries-table {
        height: 1fr;
        border: solid $primary;
    }

    #button-bar {
        height: 3;
        align: center middle;
        dock: bottom;
    }

    #button-bar Button {
        margin: 0 1;
    }

    #status-bar {
        height: 1;
        dock: bottom;
        background: $surface;
        padding: 0 1;
    }

    #sort-bar {
        height: 1;
        dock: top;
        background: $surface;
        padding: 0 1;
    }

    #title-bar {
        height: 1;
        dock: top;
        background: $primary;
        color: $text;
        padding: 0 1;
        text-style: bold;
    }
    """

    BINDINGS = [
        ("escape", "go_back", "Back"),
        ("q", "go_back", "Back"),
        ("r", "refresh", "Refresh"),
        ("space", "toggle_select", "Toggle Select"),
        ("a", "select_all", "Select All"),
        ("n", "select_none", "Select None"),
        ("d", "delete", "Delete"),
        ("1", "sort_name", "Sort by Name"),
        ("2", "sort_size", "Sort by Size"),
        ("3", "sort_date", "Sort by Date"),
    ]

    def __init__(self, handler: CacheHandler) -> None:
        super().__init__()
        self.handler = handler
        self.entries: list[CacheEntry] = []
        self.selected_entries: set[int] = set()
        self.sort_column = SortColumn.SIZE
        self.sort_reverse = True

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="main-container"):
            yield Static(
                f"[bold]{self.handler.name}[/bold] - {self.handler.formatted_size}",
                id="title-bar",
            )
            yield Static(
                "Sort: [1]Name [2]Size [3]Date | "
                "Select: [Space]Toggle [a]All [n]None | [d]Delete [r]Refresh [q]Back",
                id="sort-bar",
            )
            yield DataTable(id="entries-table")
            yield Static("", id="status-bar")
            with Horizontal(id="button-bar"):
                yield Button("Back", id="back")
                yield Button("Refresh", id="refresh")
                yield Button("Select All", id="select-all")
                yield Button("Select None", id="select-none")
                yield Button("Delete Selected", variant="error", id="delete")
        yield Footer()

    def on_mount(self) -> None:
        """Initialize the table on mount."""
        table = self.query_one(DataTable)
        table.cursor_type = "row"
        table.add_column("", key="selected", width=3)
        table.add_column("Name", key="name")
        table.add_column("Size", key="size", width=12)
        table.add_column("Last Access", key="last_access", width=18)
        self.load_entries()

    def load_entries(self) -> None:
        """Load entries from the handler."""
        self.entries = self.handler.get_entries()
        self.selected_entries.clear()
        self.sort_entries()
        self.refresh_table()

    def sort_entries(self) -> None:
        """Sort entries by the current sort column."""
        if self.sort_column == SortColumn.NAME:
            self.entries.sort(key=lambda e: e.name.lower(), reverse=self.sort_reverse)
        elif self.sort_column == SortColumn.SIZE:
            self.entries.sort(key=lambda e: e.size, reverse=self.sort_reverse)
        elif self.sort_column == SortColumn.LAST_ACCESS:

            def date_key(e: CacheEntry) -> datetime:
                if e.last_access is None:
                    return datetime.min if self.sort_reverse else datetime.max
                return e.last_access

            self.entries.sort(key=date_key, reverse=self.sort_reverse)

    def refresh_table(self) -> None:
        """Refresh the table display."""
        table = self.query_one(DataTable)
        table.clear()

        for idx, entry in enumerate(self.entries):
            selected = "[X]" if idx in self.selected_entries else "[ ]"
            table.add_row(
                selected,
                entry.name,
                entry.formatted_size,
                entry.formatted_last_access,
                key=str(idx),
            )

        self.update_status()

    def update_status(self) -> None:
        """Update the status bar."""
        count = len(self.selected_entries)
        if count == 0:
            self.query_one("#status-bar", Static).update(
                "No entries selected. Press Space to select, Enter to toggle."
            )
        else:
            total_size = sum(self.entries[i].size for i in self.selected_entries)
            self.query_one("#status-bar", Static).update(
                f"Selected: {count} entries ({format_size(total_size)})"
            )

    def toggle_selection(self, row_idx: int) -> None:
        """Toggle selection of a row."""
        if row_idx in self.selected_entries:
            self.selected_entries.remove(row_idx)
        else:
            self.selected_entries.add(row_idx)
        self.refresh_table()

    @on(DataTable.RowSelected)
    def on_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle row selection."""
        if event.row_key is not None and event.row_key.value is not None:
            row_idx = int(event.row_key.value)
            self.toggle_selection(row_idx)

    def action_toggle_select(self) -> None:
        """Toggle selection of current row."""
        table = self.query_one(DataTable)
        if table.cursor_row is not None and table.cursor_row < len(self.entries):
            self.toggle_selection(table.cursor_row)

    @on(Button.Pressed, "#back")
    def action_go_back(self) -> None:
        """Go back to library screen."""
        self.app.pop_screen()

    @on(Button.Pressed, "#refresh")
    def action_refresh(self) -> None:
        """Refresh the entries."""
        self.load_entries()
        self.app.notify("Refreshed entries")

    @on(Button.Pressed, "#select-all")
    def action_select_all(self) -> None:
        """Select all entries."""
        self.selected_entries = set(range(len(self.entries)))
        self.refresh_table()

    @on(Button.Pressed, "#select-none")
    def action_select_none(self) -> None:
        """Deselect all entries."""
        self.selected_entries.clear()
        self.refresh_table()

    def action_sort_name(self) -> None:
        """Sort by name."""
        if self.sort_column == SortColumn.NAME:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = SortColumn.NAME
            self.sort_reverse = False
        self.sort_entries()
        self.refresh_table()

    def action_sort_size(self) -> None:
        """Sort by size."""
        if self.sort_column == SortColumn.SIZE:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = SortColumn.SIZE
            self.sort_reverse = True
        self.sort_entries()
        self.refresh_table()

    def action_sort_date(self) -> None:
        """Sort by date."""
        if self.sort_column == SortColumn.LAST_ACCESS:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = SortColumn.LAST_ACCESS
            self.sort_reverse = True
        self.sort_entries()
        self.refresh_table()

    @on(Button.Pressed, "#delete")
    def action_delete(self) -> None:
        """Delete selected entries."""
        if not self.selected_entries:
            self.app.notify("No entries selected", severity="warning")
            return

        total_size = sum(self.entries[i].size for i in self.selected_entries)
        message = f"Delete {len(self.selected_entries)} selected entries?"

        def do_delete(confirmed: bool | None) -> None:
            if not confirmed:
                return

            deleted = 0
            failed = 0
            for idx in sorted(self.selected_entries, reverse=True):
                entry = self.entries[idx]
                if entry.delete():
                    deleted += 1
                else:
                    failed += 1

            self.load_entries()

            if failed:
                self.app.notify(
                    f"Deleted {deleted}, failed {failed}", severity="warning"
                )
            else:
                self.app.notify(f"Deleted {deleted} entries", severity="information")

        self.app.push_screen(ConfirmScreen(message, format_size(total_size)), do_delete)


class LibraryScreen(Screen[None]):
    """Main screen showing all cache libraries."""

    CSS = """
    #main-container {
        height: 100%;
    }

    #library-table {
        height: 1fr;
        border: solid $primary;
    }

    #button-bar {
        height: 3;
        align: center middle;
        dock: bottom;
    }

    #button-bar Button {
        margin: 0 1;
    }

    #status-bar {
        height: 1;
        dock: bottom;
        background: $surface;
        padding: 0 1;
    }

    #help-bar {
        height: 1;
        dock: top;
        background: $surface;
        padding: 0 1;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("enter", "open_library", "Open"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.handlers: list[CacheHandler] = []

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="main-container"):
            yield Static(
                "Press [Enter] to view entries | [r] Refresh | [q] Quit",
                id="help-bar",
            )
            yield DataTable(id="library-table")
            yield Static("", id="status-bar")
            with Horizontal(id="button-bar"):
                yield Button("Open", id="open")
                yield Button("Refresh", id="refresh")
                yield Button("Quit", id="quit")
        yield Footer()

    def on_mount(self) -> None:
        """Initialize the table on mount."""
        table = self.query_one(DataTable)
        table.cursor_type = "row"
        table.add_column("Library", key="name")
        table.add_column("Path", key="path")
        table.add_column("Size", key="size", width=12)
        table.add_column("Entries", key="entries", width=8)
        self.load_handlers()

    def load_handlers(self) -> None:
        """Load all cache handlers."""
        self.handlers = [h for h in get_all_handlers() if h.exists]
        self.refresh_table()

    def refresh_table(self) -> None:
        """Refresh the table display."""
        table = self.query_one(DataTable)
        table.clear()

        total_size = 0
        total_entries = 0

        for idx, handler in enumerate(self.handlers):
            entries = handler.get_entries()
            size = handler.total_size
            total_size += size
            total_entries += len(entries)

            table.add_row(
                handler.name,
                str(handler.cache_path),
                handler.formatted_size,
                str(len(entries)),
                key=str(idx),
            )

        self.query_one("#status-bar", Static).update(
            f"Total: {len(self.handlers)} libraries, "
            f"{total_entries} entries, {format_size(total_size)}"
        )

    @on(DataTable.RowSelected)
    def on_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle row selection - open the library."""
        if event.row_key is not None and event.row_key.value is not None:
            row_idx = int(event.row_key.value)
            if row_idx < len(self.handlers):
                handler = self.handlers[row_idx]
                self.app.push_screen(EntriesScreen(handler))

    @on(Button.Pressed, "#open")
    def action_open_library(self) -> None:
        """Open the selected library."""
        table = self.query_one(DataTable)
        if table.cursor_row is not None and table.cursor_row < len(self.handlers):
            handler = self.handlers[table.cursor_row]
            self.app.push_screen(EntriesScreen(handler))

    @on(Button.Pressed, "#refresh")
    def action_refresh(self) -> None:
        """Refresh the library list."""
        self.load_handlers()
        self.app.notify("Refreshed library list")

    @on(Button.Pressed, "#quit")
    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()


class CacheCleanerApp(App[None]):
    """TUI application for cleaning ML caches."""

    TITLE = "DS Cache Cleaner"

    def on_mount(self) -> None:
        """Push the main screen on mount."""
        self.push_screen(LibraryScreen())


if __name__ == "__main__":
    app = CacheCleanerApp()
    app.run()
