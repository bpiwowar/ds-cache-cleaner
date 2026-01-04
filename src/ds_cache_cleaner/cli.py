"""CLI interface for ds-cache-cleaner."""

import click
from rich.console import Console
from rich.table import Table

from ds_cache_cleaner import __version__
from ds_cache_cleaner.caches import CacheHandler, get_all_handlers
from ds_cache_cleaner.utils import format_size

console = Console()


def get_handler_by_name(name: str) -> CacheHandler | None:
    """Get a cache handler by name (case-insensitive)."""
    for handler in get_all_handlers():
        if handler.name.lower() == name.lower():
            return handler
    return None


@click.group()
@click.version_option(version=__version__, prog_name="ds-cache-cleaner")
def main() -> None:
    """Clean up cached data from ML/data science libraries."""
    pass


@main.command()
@click.option(
    "--all", "show_all", is_flag=True, help="Show all caches, even empty ones"
)
def list(show_all: bool) -> None:
    """List all detected caches and their sizes."""
    handlers = get_all_handlers()

    table = Table(title="Detected Caches")
    table.add_column("Cache", style="cyan")
    table.add_column("Path", style="dim")
    table.add_column("Size", justify="right", style="green")
    table.add_column("Entries", justify="right")

    total_size = 0
    for handler in handlers:
        if handler.exists or show_all:
            size = handler.total_size
            total_size += size
            entries = handler.get_entries()
            table.add_row(
                handler.name,
                str(handler.cache_path),
                handler.formatted_size if handler.exists else "-",
                str(len(entries)) if handler.exists else "-",
            )

    console.print(table)
    console.print(f"\n[bold]Total size:[/bold] {format_size(total_size)}")


@main.command()
@click.option("--cache", "-c", "cache_name", help="Clean only this cache")
@click.option(
    "--all", "clean_all", is_flag=True, help="Clean all caches without prompting"
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be deleted without deleting"
)
def clean(cache_name: str | None, clean_all: bool, dry_run: bool) -> None:
    """Clean cache entries."""
    if cache_name:
        handler = get_handler_by_name(cache_name)
        if not handler:
            console.print(f"[red]Unknown cache: {cache_name}[/red]")
            available = ", ".join(h.name for h in get_all_handlers())
            console.print(f"Available caches: {available}")
            raise SystemExit(1)
        handlers = [handler]
    else:
        handlers = [h for h in get_all_handlers() if h.exists]

    if not handlers:
        console.print("[yellow]No caches found.[/yellow]")
        return

    # Show what will be cleaned
    table = Table(title="Caches to clean" if not dry_run else "Caches (dry run)")
    table.add_column("Cache", style="cyan")
    table.add_column("Size", justify="right", style="green")
    table.add_column("Entries", justify="right")

    total_size = 0
    for handler in handlers:
        if handler.exists:
            size = handler.total_size
            total_size += size
            entries = handler.get_entries()
            table.add_row(handler.name, handler.formatted_size, str(len(entries)))

    console.print(table)
    console.print(f"\n[bold]Total to clean:[/bold] {format_size(total_size)}")

    if dry_run:
        console.print("\n[yellow]Dry run - no files deleted.[/yellow]")
        return

    if not clean_all:
        if not click.confirm("\nProceed with cleanup?"):
            console.print("[yellow]Cancelled.[/yellow]")
            return

    # Perform cleanup
    for handler in handlers:
        if handler.exists:
            console.print(f"Cleaning {handler.name}...", end=" ")
            deleted, failed = handler.clean_all()
            if failed:
                console.print(f"[yellow]{deleted} deleted, {failed} failed[/yellow]")
            else:
                console.print(f"[green]{deleted} entries deleted[/green]")

    console.print("\n[green]Cleanup complete![/green]")


@main.command()
@click.option("--cache", "-c", "cache_name", help="Show entries for this cache only")
def show(cache_name: str | None) -> None:
    """Show detailed cache entries."""
    if cache_name:
        handler = get_handler_by_name(cache_name)
        if not handler:
            console.print(f"[red]Unknown cache: {cache_name}[/red]")
            available = ", ".join(h.name for h in get_all_handlers())
            console.print(f"Available caches: {available}")
            raise SystemExit(1)
        handlers = [handler]
    else:
        handlers = [h for h in get_all_handlers() if h.exists]

    for handler in handlers:
        if not handler.exists:
            continue

        entries = handler.get_entries()
        if not entries:
            continue

        table = Table(title=f"{handler.name} ({handler.formatted_size})")
        table.add_column("Name", style="cyan")
        table.add_column("Size", justify="right", style="green")
        table.add_column("Last Access", justify="right", style="dim")

        for entry in entries:
            table.add_row(entry.name, entry.formatted_size, entry.formatted_last_access)

        console.print(table)
        console.print()


@main.command()
def tui() -> None:
    """Launch the interactive TUI."""
    from ds_cache_cleaner.tui import CacheCleanerApp

    app = CacheCleanerApp()
    app.run()


if __name__ == "__main__":
    main()
