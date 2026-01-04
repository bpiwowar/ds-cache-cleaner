"""Base class for cache handlers."""

import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from ds_cache_cleaner.metadata import (
    MetadataManager,
    PartInfo,
)
from ds_cache_cleaner.utils import (
    format_size,
    get_directory_size,
    get_last_access_time,
)


@dataclass
class CacheEntry:
    """Represents a single cache entry (file or directory)."""

    name: str
    path: Path
    size: int
    handler_name: str
    last_access: datetime | None = None
    description: str = ""
    part_name: str = ""
    created: datetime | None = None
    metadata: dict = field(default_factory=dict)
    # Whether this entry came from metadata (vs filesystem scan)
    from_metadata: bool = False

    @property
    def formatted_size(self) -> str:
        """Return human-readable size."""
        return format_size(self.size)

    @property
    def formatted_last_access(self) -> str:
        """Return human-readable last access time."""
        if self.last_access is None:
            return "Unknown"
        return self.last_access.strftime("%Y-%m-%d %H:%M")

    def delete(self) -> bool:
        """Delete this cache entry. Returns True if successful."""
        try:
            if self.path.is_dir():
                shutil.rmtree(self.path)
            else:
                self.path.unlink()
            return True
        except (OSError, PermissionError):
            return False


class CacheHandler(ABC):
    """Abstract base class for cache handlers."""

    def __init__(self) -> None:
        self._metadata_manager: MetadataManager | None = None

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this cache type."""
        ...

    @property
    @abstractmethod
    def cache_path(self) -> Path:
        """Path to the cache directory."""
        ...

    @property
    def exists(self) -> bool:
        """Check if the cache directory exists."""
        return self.cache_path.exists()

    @property
    def metadata_manager(self) -> MetadataManager:
        """Get the metadata manager for this cache."""
        if self._metadata_manager is None:
            self._metadata_manager = MetadataManager(self.cache_path)
        return self._metadata_manager

    @property
    def has_metadata(self) -> bool:
        """Check if metadata exists for this cache."""
        return self.metadata_manager.exists

    @property
    def total_size(self) -> int:
        """Get total size of the cache in bytes."""
        return get_directory_size(self.cache_path)

    @property
    def formatted_size(self) -> str:
        """Get human-readable total size."""
        return format_size(self.total_size)

    def get_parts(self) -> list[PartInfo]:
        """Get the parts defined in metadata, or a default list."""
        if self.has_metadata:
            info = self.metadata_manager.read_info()
            if info and info.parts:
                return info.parts
        # Default: single part with empty name
        return [PartInfo(name="", description=self.name)]

    def _entries_from_metadata(self) -> list[CacheEntry] | None:
        """Get entries from metadata if available.

        Returns:
            List of CacheEntry if metadata exists, None otherwise.
        """
        if not self.has_metadata:
            return None

        entries: list[CacheEntry] = []
        all_parts = self.metadata_manager.get_all_parts()

        for part_name, part_data in all_parts.items():
            for entry_meta in part_data.entries:
                entry_path = self.cache_path / entry_meta.path
                # Use metadata size or calculate if not available
                if entry_meta.size is not None:
                    size = entry_meta.size
                elif entry_path.exists():
                    size = (
                        get_directory_size(entry_path)
                        if entry_path.is_dir()
                        else entry_path.stat().st_size
                    )
                else:
                    size = 0

                entries.append(
                    CacheEntry(
                        name=entry_meta.description or entry_meta.path,
                        path=entry_path,
                        size=size,
                        handler_name=self.name,
                        last_access=entry_meta.last_access,
                        description=entry_meta.description,
                        part_name=part_name,
                        created=entry_meta.created,
                        metadata=entry_meta.metadata,
                        from_metadata=True,
                    )
                )

        return entries if entries else None

    def _entries_from_filesystem(self) -> list[CacheEntry]:
        """Get entries by scanning the filesystem (fallback)."""
        if not self.exists:
            return []

        entries = []
        for item in self.cache_path.iterdir():
            # Skip the metadata directory
            if item.name == "ds-cache-cleaner":
                continue

            size = get_directory_size(item) if item.is_dir() else item.stat().st_size
            last_access = get_last_access_time(item)
            entries.append(
                CacheEntry(
                    name=item.name,
                    path=item,
                    size=size,
                    handler_name=self.name,
                    last_access=last_access,
                    from_metadata=False,
                )
            )
        return entries

    def get_entries(self) -> list[CacheEntry]:
        """Get all cache entries.

        First tries to get entries from metadata. If no metadata exists,
        falls back to scanning the filesystem.
        """
        # Try metadata first
        entries = self._entries_from_metadata()
        if entries is not None:
            return sorted(entries, key=lambda e: e.size, reverse=True)

        # Fallback to filesystem scan
        entries = self._entries_from_filesystem()
        return sorted(entries, key=lambda e: e.size, reverse=True)

    def delete_entry(self, entry: CacheEntry) -> bool:
        """Delete a cache entry and update metadata if applicable.

        Args:
            entry: The entry to delete.

        Returns:
            True if successful, False otherwise.
        """
        # Delete the actual files
        if not entry.delete():
            return False

        # Update metadata if this entry came from metadata
        if entry.from_metadata and entry.part_name:
            rel_path = str(entry.path.relative_to(self.cache_path))
            self.metadata_manager.remove_entry(entry.part_name, rel_path)

        return True

    def clean_all(self) -> tuple[int, int]:
        """Delete all cache entries. Returns (deleted_count, failed_count)."""
        deleted = 0
        failed = 0
        for entry in self.get_entries():
            if self.delete_entry(entry):
                deleted += 1
            else:
                failed += 1
        return deleted, failed
