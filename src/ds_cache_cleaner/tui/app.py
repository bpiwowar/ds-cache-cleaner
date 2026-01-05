"""Textual TUI application for ds-cache-cleaner."""

from datetime import datetime
from enum import Enum
from pathlib import Path

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.message import Message
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, DataTable, Footer, Header, Label, Static

from ds_cache_cleaner.caches import CacheEntry, CacheHandler, get_all_handlers
from ds_cache_cleaner.utils import (
    SizeMessage,
    SizeState,
    ThreadSizeComputer,
    format_size,
)


class EntryUpdate(Message):
    """Message posted when an entry size computation updates."""

    def __init__(self, library: str, entry: CacheEntry) -> None:
        super().__init__()
        self.library = library
        self.entry = entry


class LibraryUpdate(Message):
    """Message posted when a library total size updates."""

    def __init__(self, library: str) -> None:
        super().__init__()
        self.library = library


class SortColumn(Enum):
    """Columns that can be sorted in entries view."""

    NAME = "name"
    SIZE = "size"
    LAST_ACCESS = "last_access"


class LibrarySortColumn(Enum):
    """Columns that can be sorted in library view."""

    NAME = "name"
    SIZE = "size"
    ENTRIES = "entries"


class ConfirmScreen(ModalScreen[bool]):
    """A modal screen to confirm deletion."""

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
    ]

    CSS = """
    ConfirmScreen {
        align: center middle;
    }

    #dialog {
        width: 80;
        max-height: 80%;
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

    #paths-container {
        height: auto;
        max-height: 15;
        border: solid $primary;
        margin-bottom: 1;
        overflow-y: auto;
    }

    #paths-list {
        padding: 0 1;
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

    def __init__(self, message: str, size_str: str, paths: list[Path]) -> None:
        super().__init__()
        self.message = message
        self.size_str = size_str
        self.paths = paths

    def compose(self) -> ComposeResult:
        from textual.containers import VerticalScroll

        with Container(id="dialog"):
            yield Label("Confirm Deletion", id="dialog-title")
            yield Label(self.message, id="dialog-message")
            with VerticalScroll(id="paths-container"):
                paths_text = "\n".join(str(p) for p in self.paths)
                yield Static(paths_text, id="paths-list")
            yield Label(f"Total size: [bold]{self.size_str}[/bold]")
            with Horizontal(id="dialog-buttons"):
                yield Button("Cancel", variant="primary", id="cancel")
                yield Button("Delete", variant="error", id="confirm")

    def on_mount(self) -> None:
        """Focus the Cancel button by default."""
        self.query_one("#cancel", Button).focus()

    @on(Button.Pressed, "#confirm")
    def confirm(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#cancel")
    def action_cancel(self) -> None:
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
        self.selected_entries: set[Path] = set()  # Track by path for stability
        self.sort_column = SortColumn.SIZE
        self.sort_reverse = True

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="main-container"):
            yield Static(
                f"[bold]{self.handler.name}[/bold]",
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
        table.add_column("Size", key="size", width=14)
        table.add_column("Last Access", key="last_access", width=18)
        # Defer loading to allow event loop to fully start
        self.set_timer(0.1, self.load_entries)

    def _get_column_label(self, column: SortColumn, base_label: str) -> str:
        """Get column label with sort indicator if applicable."""
        if self.sort_column != column:
            return base_label
        indicator = "▼" if self.sort_reverse else "▲"
        return f"{base_label} {indicator}"

    def _update_column_labels(self) -> None:
        """Update column labels to show current sort."""
        table = self.query_one(DataTable)
        table.columns["name"].label = self._get_column_label(SortColumn.NAME, "Name")  # type: ignore[index,assignment]
        table.columns["size"].label = self._get_column_label(SortColumn.SIZE, "Size")  # type: ignore[index,assignment]
        table.columns["last_access"].label = self._get_column_label(  # type: ignore[index,assignment]
            SortColumn.LAST_ACCESS, "Last Access"
        )

    def load_entries(self) -> None:
        """Load entries from the handler."""
        app: CacheCleanerApp = self.app  # type: ignore

        # Get entries from app cache (triggers computation if needed)
        self.entries = app.get_library_entries(self.handler.name)
        self.selected_entries.clear()

        self.sort_entries()
        self.refresh_table()

    def on_entry_update(self, event: EntryUpdate) -> None:
        """Handle entry size update messages."""
        # Only handle updates for entries in this handler
        if event.library != self.handler.name:
            return

        entry = event.entry
        # Find the entry in our list (by path)
        entry_path = entry.path.resolve()
        idx = None
        for i, e in enumerate(self.entries):
            if e.path.resolve() == entry_path:
                idx = i
                break

        if idx is None:
            return

        # Entry was updated in place by ThreadSizeComputer
        # Re-sort if sorting by size to maintain order
        if self.sort_column == SortColumn.SIZE:
            self._do_sort()
        else:
            # Just update the cell
            table = self.query_one(DataTable)
            try:
                table.update_cell(str(idx), "size", entry.formatted_size)
            except Exception:
                pass  # Row might not exist anymore

        self.update_status()

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

        self._update_column_labels()

        for idx, entry in enumerate(self.entries):
            selected = "[X]" if entry.path in self.selected_entries else "[ ]"
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
            total_size = sum(
                e.size for e in self.entries if e.path in self.selected_entries
            )
            self.query_one("#status-bar", Static).update(
                f"Selected: {count} entries ({format_size(total_size)})"
            )

    def toggle_selection(self, row_idx: int) -> None:
        """Toggle selection of a row."""
        if row_idx >= len(self.entries):
            return

        entry = self.entries[row_idx]
        if entry.path in self.selected_entries:
            self.selected_entries.remove(entry.path)
            selected = "[ ]"
        else:
            self.selected_entries.add(entry.path)
            selected = "[X]"

        # Update just the selection cell, not the entire table
        table = self.query_one(DataTable)
        try:
            table.update_cell(str(row_idx), "selected", selected)
        except Exception:
            pass

        self.update_status()

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
        self.selected_entries = {e.path for e in self.entries}
        table = self.query_one(DataTable)
        for idx in range(len(self.entries)):
            try:
                table.update_cell(str(idx), "selected", "[X]")
            except Exception:
                pass
        self.update_status()

    @on(Button.Pressed, "#select-none")
    def action_select_none(self) -> None:
        """Deselect all entries."""
        table = self.query_one(DataTable)
        for idx in range(len(self.entries)):
            try:
                table.update_cell(str(idx), "selected", "[ ]")
            except Exception:
                pass
        self.selected_entries.clear()
        self.update_status()

    def _do_sort(self) -> None:
        """Sort entries, preserving selection (tracked by path)."""
        self.sort_entries()
        self.refresh_table()

    def action_sort_name(self) -> None:
        """Sort by name."""
        if self.sort_column == SortColumn.NAME:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = SortColumn.NAME
            self.sort_reverse = False
        self._do_sort()

    def action_sort_size(self) -> None:
        """Sort by size."""
        if self.sort_column == SortColumn.SIZE:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = SortColumn.SIZE
            self.sort_reverse = True
        self._do_sort()

    def action_sort_date(self) -> None:
        """Sort by date."""
        if self.sort_column == SortColumn.LAST_ACCESS:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = SortColumn.LAST_ACCESS
            self.sort_reverse = True
        self._do_sort()

    @on(Button.Pressed, "#delete")
    def action_delete(self) -> None:
        """Delete selected entries."""
        if not self.selected_entries:
            self.app.notify("No entries selected", severity="warning")
            return

        selected_entries = [e for e in self.entries if e.path in self.selected_entries]
        total_size = sum(e.size for e in selected_entries)
        paths = [e.path for e in selected_entries]
        message = f"Delete {len(selected_entries)} selected entries?"

        def do_delete(confirmed: bool | None) -> None:
            if not confirmed:
                return

            deleted = 0
            failed = 0
            for entry in selected_entries:
                if entry.delete():
                    deleted += 1
                else:
                    failed += 1

            # Reload entries (invalidate cache first)
            app: CacheCleanerApp = self.app  # type: ignore
            app.invalidate_library(self.handler.name)
            self.load_entries()

            if failed:
                self.app.notify(
                    f"Deleted {deleted}, failed {failed}", severity="warning"
                )
            else:
                self.app.notify(f"Deleted {deleted} entries", severity="information")

        self.app.push_screen(
            ConfirmScreen(message, format_size(total_size), paths), do_delete
        )


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
        ("1", "sort_name", "Sort by Name"),
        ("2", "sort_size", "Sort by Size"),
        ("3", "sort_entries", "Sort by Entries"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.handlers: list[CacheHandler] = []
        self.sort_column = LibrarySortColumn.SIZE
        self.sort_reverse = True

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="main-container"):
            yield Static(
                "Sort: [1]Name [2]Size [3]Entries | [Enter] Open | [r] Refresh | [q] Quit",
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
        table.add_column("Size", key="size", width=15)
        table.add_column("Entries", key="entries", width=8)
        # Defer loading to allow event loop to fully start
        self.set_timer(0.1, self.load_handlers)

    def _get_column_label(self, column: LibrarySortColumn, base_label: str) -> str:
        """Get column label with sort indicator if applicable."""
        if self.sort_column != column:
            return base_label
        indicator = "▼" if self.sort_reverse else "▲"
        return f"{base_label} {indicator}"

    def _update_column_labels(self) -> None:
        """Update column labels to show current sort."""
        table = self.query_one(DataTable)
        table.columns["name"].label = self._get_column_label(  # type: ignore[index,assignment]
            LibrarySortColumn.NAME, "Library"
        )
        table.columns["size"].label = self._get_column_label(  # type: ignore[index,assignment]
            LibrarySortColumn.SIZE, "Size"
        )
        table.columns["entries"].label = self._get_column_label(  # type: ignore[index,assignment]
            LibrarySortColumn.ENTRIES, "Entries"
        )

    def sort_handlers(self) -> None:
        """Sort handlers by the current sort column."""
        app: CacheCleanerApp = self.app  # type: ignore

        if self.sort_column == LibrarySortColumn.NAME:
            self.handlers.sort(key=lambda h: h.name.lower(), reverse=self.sort_reverse)
        elif self.sort_column == LibrarySortColumn.SIZE:
            self.handlers.sort(
                key=lambda h: app.get_library_size(h.name)[1],
                reverse=self.sort_reverse,
            )
        elif self.sort_column == LibrarySortColumn.ENTRIES:
            self.handlers.sort(
                key=lambda h: len(app.get_library_entries(h.name)),
                reverse=self.sort_reverse,
            )

    def load_handlers(self) -> None:
        """Load all cache handlers."""
        self.log("Loading cache handlers")
        app: CacheCleanerApp = self.app  # type: ignore
        self.handlers = [h for h in get_all_handlers() if h.exists]

        # Pre-load entries for all handlers (triggers size computation)
        for handler in self.handlers:
            app.get_library_entries(handler.name)

        self.sort_handlers()
        self.refresh_table()

    def on_library_update(self, event: LibraryUpdate) -> None:
        """Handle library total size updates."""
        app: CacheCleanerApp = self.app  # type: ignore

        # Find handler index
        idx = None
        for i, h in enumerate(self.handlers):
            if h.name == event.library:
                idx = i
                break

        if idx is None:
            return

        # Re-sort if sorting by size to maintain order
        if self.sort_column == LibrarySortColumn.SIZE:
            self.sort_handlers()
            self.refresh_table()
        else:
            # Just update the cell
            state, total = app.get_library_size(event.library)
            table = self.query_one(DataTable)
            if state == SizeState.COMPUTED:
                display = format_size(total)
            elif state == SizeState.COMPUTING:
                display = f"⚙️  {format_size(total)}"
            else:
                display = "⌛"

            try:
                table.update_cell(str(idx), "size", display)
            except Exception:
                pass

            self._update_status_bar()

    def refresh_table(self) -> None:
        """Refresh the table display."""
        table = self.query_one(DataTable)
        table.clear()
        app: CacheCleanerApp = self.app  # type: ignore

        self._update_column_labels()

        for idx, handler in enumerate(self.handlers):
            entries = app.get_library_entries(handler.name)

            # Get current size state from app
            state, total = app.get_library_size(handler.name)
            if state == SizeState.COMPUTED:
                size_display = format_size(total)
            elif state == SizeState.COMPUTING:
                size_display = f"⚙️  {format_size(total)}"
            else:
                size_display = "⌛"

            table.add_row(
                handler.name,
                str(handler.cache_path),
                size_display,
                str(len(entries)),
                key=str(idx),
            )

        self._update_status_bar()

    def _update_status_bar(self) -> None:
        """Update the status bar with current totals."""
        app: CacheCleanerApp = self.app  # type: ignore
        total_entries = sum(len(app.get_library_entries(h.name)) for h in self.handlers)

        total_size = 0
        computed_count = 0
        for handler in self.handlers:
            state, size = app.get_library_size(handler.name)
            if state == SizeState.COMPUTED:
                computed_count += 1
            total_size += size

        status = f"Total: {len(self.handlers)} libraries, {total_entries} entries"
        if computed_count == len(self.handlers):
            status += f", {format_size(total_size)}"
        else:
            status += f", computing... ({computed_count}/{len(self.handlers)})"

        self.query_one("#status-bar", Static).update(status)

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
        app: CacheCleanerApp = self.app  # type: ignore
        app.invalidate_all()
        self.load_handlers()
        self.app.notify("Refreshed library list")

    @on(Button.Pressed, "#quit")
    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()

    def action_sort_name(self) -> None:
        """Sort by name."""
        if self.sort_column == LibrarySortColumn.NAME:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = LibrarySortColumn.NAME
            self.sort_reverse = False
        self.sort_handlers()
        self.refresh_table()

    def action_sort_size(self) -> None:
        """Sort by size."""
        if self.sort_column == LibrarySortColumn.SIZE:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = LibrarySortColumn.SIZE
            self.sort_reverse = True
        self.sort_handlers()
        self.refresh_table()

    def action_sort_entries(self) -> None:
        """Sort by entries count."""
        if self.sort_column == LibrarySortColumn.ENTRIES:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = LibrarySortColumn.ENTRIES
            self.sort_reverse = True
        self.sort_handlers()
        self.refresh_table()


class CacheCleanerApp(App[None]):
    """TUI application for cleaning ML caches."""

    TITLE = "DS Cache Cleaner"

    def __init__(self) -> None:
        super().__init__()
        # Cached entries per library (CacheEntry is the source of truth)
        self._library_entries: dict[str, list[CacheEntry]] = {}
        # Handlers by name (for loading entries)
        self._handlers: dict[str, CacheHandler] = {}

    def get_library_entries(self, library: str) -> list[CacheEntry]:
        """Get entries for a library, loading and computing sizes if needed."""
        if library in self._library_entries:
            return self._library_entries[library]

        # Load entries from handler
        if library not in self._handlers:
            for h in get_all_handlers():
                self._handlers[h.name] = h

        handler = self._handlers.get(library)
        if handler is None:
            return []

        entries = handler.get_entries()
        self._library_entries[library] = entries

        # Request size computation for entries with PENDING state
        computer = ThreadSizeComputer.get_instance()
        for entry in entries:
            if entry.size_state == SizeState.PENDING:
                computer.request_size(library, entry)

        return entries

    def get_library_size(self, library: str) -> tuple[SizeState, int]:
        """Get current total size and state for a library."""
        entries = self.get_library_entries(library)
        if not entries:
            return SizeState.COMPUTED, 0

        total = 0
        computed_count = 0
        for entry in entries:
            if entry.size_state == SizeState.COMPUTED:
                total += entry.size
                computed_count += 1

        if computed_count == len(entries):
            return SizeState.COMPUTED, total
        elif computed_count > 0:
            return SizeState.COMPUTING, total
        else:
            return SizeState.PENDING, 0

    def invalidate_library(self, library: str) -> None:
        """Invalidate cached data for a library."""
        if library in self._library_entries:
            computer = ThreadSizeComputer.get_instance()
            for entry in self._library_entries[library]:
                computer.invalidate(entry)
            del self._library_entries[library]

    def invalidate_all(self) -> None:
        """Invalidate all cached data."""
        self._library_entries.clear()
        ThreadSizeComputer.get_instance().invalidate_all()

    def on_mount(self) -> None:
        """Push the main screen on mount and set up size computation listener."""
        computer = ThreadSizeComputer.get_instance()
        computer.set_listener(self._on_size_message)
        self.push_screen(LibraryScreen())

    def on_unmount(self) -> None:
        """Clean up when app is unmounted."""
        computer = ThreadSizeComputer.get_instance()
        computer.set_listener(None)
        computer.shutdown()

    def _on_size_message(self, msg: SizeMessage) -> None:
        """Handle size message from worker thread."""
        self.call_from_thread(self._process_size_update, msg)

    def _process_size_update(self, msg: SizeMessage) -> None:
        """Process size update and dispatch to screens.

        CacheEntry is already updated by ThreadSizeComputer, just dispatch to screens.
        """
        from ds_cache_cleaner.caches.base import CacheEntry

        entry: CacheEntry = msg.entry  # type: ignore

        # Dispatch to current screen based on type
        screen = self.screen
        if isinstance(screen, EntriesScreen):
            screen.on_entry_update(EntryUpdate(msg.library, entry))  # type: ignore[attr-defined]
        elif isinstance(screen, LibraryScreen):
            screen.on_library_update(LibraryUpdate(msg.library))  # type: ignore[attr-defined]


if __name__ == "__main__":
    app = CacheCleanerApp()
    app.run()
