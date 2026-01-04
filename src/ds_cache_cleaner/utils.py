"""Utility functions for ds-cache-cleaner."""

from datetime import datetime
from pathlib import Path


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
