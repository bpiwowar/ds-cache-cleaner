"""Utility functions for ds-cache-cleaner."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import NamedTuple


def format_size(size_bytes: int) -> str:
    """Format a size in bytes to a human-readable string."""
    size: float = size_bytes
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def get_directory_size(path: Path) -> int:
    """Calculate the total size of a directory in bytes."""
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except (OSError, PermissionError):
                pass
    return total


def get_last_access_time(path: Path) -> datetime | None:
    """Get the most recent access time for a file or directory.

    For directories, returns the most recent access time among all files.
    Uses mtime (modification time) as it's more reliable than atime.
    """
    if not path.exists():
        return None

    try:
        if path.is_file():
            return datetime.fromtimestamp(path.stat().st_mtime)

        # For directories, find the most recent mtime among all files
        latest = None
        for item in path.rglob("*"):
            if item.is_file():
                try:
                    mtime = datetime.fromtimestamp(item.stat().st_mtime)
                    if latest is None or mtime > latest:
                        latest = mtime
                except (OSError, PermissionError):
                    pass
        return latest
    except (OSError, PermissionError):
        return None


class SizeState(Enum):
    """State of an async size computation."""

    PENDING = "pending"  # Waiting in queue
    COMPUTING = "computing"  # Currently being computed
    COMPUTED = "computed"  # Computation complete
    ERROR = "error"  # Computation failed


@dataclass
class SizeResult:
    """Result of an async size computation."""

    path: Path
    state: SizeState = SizeState.PENDING
    size: int = 0
    error: str | None = None

    def format(self) -> str:
        """Format the size result for display."""
        if self.state == SizeState.PENDING:
            return "⌛"
        elif self.state == SizeState.COMPUTING:
            return "⚙️"
        elif self.state == SizeState.ERROR:
            return "⚠"
        else:
            return format_size(self.size)


class SizeMessage(NamedTuple):
    """Message sent when a size computation updates.

    Attributes:
        library: The library/handler name.
        entry: The CacheEntry being computed (with updated size).
    """

    library: str
    entry: object  # CacheEntry - using object to avoid circular import


# Type alias for callback function
SizeMessageCallback = Callable[[SizeMessage], None]


class ThreadSizeComputer:
    """Thread-based directory size computer with message callbacks.

    This class manages background computation of directory sizes,
    updating CacheEntry objects directly and notifying listeners.
    """

    _instance: "ThreadSizeComputer | None" = None
    _lock: Lock = Lock()

    def __init__(self, max_workers: int = 1) -> None:
        # Use single worker so only one computation runs at a time
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="size_computer"
        )
        self._pending: dict[Path, object] = {}  # path -> CacheEntry
        self._pending_lock = Lock()
        self._listener: SizeMessageCallback | None = None
        self._shutdown = False

    @classmethod
    def get_instance(cls) -> "ThreadSizeComputer":
        """Get the singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def set_listener(self, listener: SizeMessageCallback | None) -> None:
        """Set a listener callback for size computation updates."""
        self._listener = listener

    def request_size(self, library: str, entry: object) -> None:
        """Request size computation for a CacheEntry.

        Args:
            library: The library/handler name.
            entry: The CacheEntry to compute size for.
        """
        # Import here to avoid circular import
        from ds_cache_cleaner.caches.base import CacheEntry

        entry_typed: CacheEntry = entry  # type: ignore
        path = entry_typed.path.resolve()

        with self._pending_lock:
            # Skip if already pending/computing or computed
            if path in self._pending:
                return
            if entry_typed.size_state == SizeState.COMPUTED:
                return

            # Mark as pending
            entry_typed.size_state = SizeState.PENDING
            self._pending[path] = entry

        # Submit computation task
        self._executor.submit(self._compute_size, library, entry)

    def _compute_size(self, library: str, entry: object) -> None:
        """Compute size in background thread."""
        from ds_cache_cleaner.caches.base import CacheEntry

        if self._shutdown:
            return

        entry_typed: CacheEntry = entry  # type: ignore
        path = entry_typed.path.resolve()

        # Update state to computing
        entry_typed.size_state = SizeState.COMPUTING
        if self._listener:
            self._listener(SizeMessage(library, entry))

        if self._shutdown:
            return

        try:
            size = get_directory_size(path)
            entry_typed.size = size
            entry_typed.size_state = SizeState.COMPUTED
        except Exception:
            entry_typed.size_state = SizeState.ERROR

        if self._shutdown:
            return

        # Remove from pending
        with self._pending_lock:
            self._pending.pop(path, None)

        # Notify listener
        if self._listener:
            self._listener(SizeMessage(library, entry))

    def invalidate(self, entry: object) -> None:
        """Invalidate a CacheEntry for recomputation."""
        from ds_cache_cleaner.caches.base import CacheEntry

        entry_typed: CacheEntry = entry  # type: ignore
        path = entry_typed.path.resolve()
        with self._pending_lock:
            self._pending.pop(path, None)
        entry_typed.size_state = SizeState.PENDING
        entry_typed.size = 0

    def invalidate_all(self) -> None:
        """Clear all pending computations."""
        with self._pending_lock:
            self._pending.clear()

    def shutdown(self) -> None:
        """Shutdown the executor."""
        self._shutdown = True
        self._listener = None
        self._executor.shutdown(wait=False, cancel_futures=True)
