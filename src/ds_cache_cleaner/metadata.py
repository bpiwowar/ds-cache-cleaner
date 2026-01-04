"""Metadata management for cache entries.

This module provides a standardized format for cache metadata that libraries
can use to provide richer information about their cached data.

Structure inside each cache directory:
    ds-cache-cleaner/
    ├── lock                    # Lock file for concurrent access
    ├── information.json        # Basic info about the cache and its parts
    └── part_<name>.json        # Entries for each part (e.g., part_models.json)

Schema for information.json:
{
    "version": 1,
    "library": "huggingface-hub",
    "description": "HuggingFace Hub cache",
    "parts": [
        {"name": "models", "description": "Downloaded model files"},
        {"name": "datasets", "description": "Downloaded dataset files"}
    ]
}

Schema for part_<name>.json:
{
    "version": 1,
    "entries": [
        {
            "path": "models--bert-base-uncased",
            "description": "BERT base uncased model",
            "created": "2024-01-15T10:30:00Z",
            "last_access": "2024-03-20T14:22:00Z",
            "size": 438123456,
            "metadata": {}  # Optional library-specific metadata
        }
    ]
}
"""

import fcntl
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

METADATA_DIR = "ds-cache-cleaner"
LOCK_FILE = "lock"
INFO_FILE = "information.json"
PART_PREFIX = "part_"
CURRENT_VERSION = 1


@dataclass
class PartInfo:
    """Information about a cache part."""

    name: str
    description: str = ""


@dataclass
class CacheInfo:
    """Information about a cache."""

    version: int = CURRENT_VERSION
    library: str = ""
    description: str = ""
    parts: list[PartInfo] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "library": self.library,
            "description": self.description,
            "parts": [
                {"name": p.name, "description": p.description} for p in self.parts
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CacheInfo":
        """Create from dictionary."""
        parts = [
            PartInfo(name=p["name"], description=p.get("description", ""))
            for p in data.get("parts", [])
        ]
        return cls(
            version=data.get("version", CURRENT_VERSION),
            library=data.get("library", ""),
            description=data.get("description", ""),
            parts=parts,
        )


@dataclass
class EntryMetadata:
    """Metadata for a single cache entry."""

    path: str
    description: str = ""
    created: datetime | None = None
    last_access: datetime | None = None
    size: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {"path": self.path}
        if self.description:
            result["description"] = self.description
        if self.created:
            result["created"] = self.created.isoformat()
        if self.last_access:
            result["last_access"] = self.last_access.isoformat()
        if self.size is not None:
            result["size"] = self.size
        if self.metadata:
            result["metadata"] = self.metadata
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EntryMetadata":
        """Create from dictionary."""
        created = None
        if "created" in data:
            created = datetime.fromisoformat(data["created"])

        last_access = None
        if "last_access" in data:
            last_access = datetime.fromisoformat(data["last_access"])

        return cls(
            path=data["path"],
            description=data.get("description", ""),
            created=created,
            last_access=last_access,
            size=data.get("size"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class PartData:
    """Data for a cache part."""

    version: int = CURRENT_VERSION
    entries: list[EntryMetadata] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "entries": [e.to_dict() for e in self.entries],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PartData":
        """Create from dictionary."""
        entries = [EntryMetadata.from_dict(e) for e in data.get("entries", [])]
        return cls(
            version=data.get("version", CURRENT_VERSION),
            entries=entries,
        )


class MetadataManager:
    """Manages cache metadata for a specific cache directory."""

    def __init__(self, cache_path: Path) -> None:
        """Initialize the metadata manager.

        Args:
            cache_path: Path to the cache directory
        """
        self.cache_path = cache_path
        self.metadata_dir = cache_path / METADATA_DIR
        self.lock_path = self.metadata_dir / LOCK_FILE
        self.info_path = self.metadata_dir / INFO_FILE

    @property
    def exists(self) -> bool:
        """Check if metadata exists for this cache."""
        return self.info_path.exists()

    def _ensure_dir(self) -> None:
        """Ensure the metadata directory exists."""
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def lock(self, exclusive: bool = False) -> Iterator[None]:
        """Acquire a lock on the metadata directory.

        Args:
            exclusive: If True, acquire an exclusive (write) lock.
                      If False, acquire a shared (read) lock.
        """
        self._ensure_dir()
        lock_file = open(self.lock_path, "w")
        try:
            if exclusive:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_SH)
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()

    def _read_info_unlocked(self) -> CacheInfo | None:
        """Read the cache information file without acquiring lock (internal use)."""
        if not self.info_path.exists():
            return None
        data = json.loads(self.info_path.read_text())
        return CacheInfo.from_dict(data)

    def _write_info_unlocked(self, info: CacheInfo) -> None:
        """Write the cache information file without acquiring lock (internal use)."""
        self._ensure_dir()
        self.info_path.write_text(json.dumps(info.to_dict(), indent=2))

    def read_info(self) -> CacheInfo | None:
        """Read the cache information file.

        Returns:
            CacheInfo if the file exists, None otherwise.
        """
        with self.lock(exclusive=False):
            return self._read_info_unlocked()

    def write_info(self, info: CacheInfo) -> None:
        """Write the cache information file.

        Args:
            info: The cache information to write.
        """
        with self.lock(exclusive=True):
            self._write_info_unlocked(info)

    def _part_path(self, part_name: str) -> Path:
        """Get the path to a part file."""
        return self.metadata_dir / f"{PART_PREFIX}{part_name}.json"

    def _read_part_unlocked(self, part_name: str) -> PartData | None:
        """Read a part data file without acquiring lock (internal use)."""
        part_path = self._part_path(part_name)
        if not part_path.exists():
            return None
        data = json.loads(part_path.read_text())
        return PartData.from_dict(data)

    def _write_part_unlocked(self, part_name: str, part_data: PartData) -> None:
        """Write a part data file without acquiring lock (internal use)."""
        self._ensure_dir()
        part_path = self._part_path(part_name)
        part_path.write_text(json.dumps(part_data.to_dict(), indent=2))

    def read_part(self, part_name: str) -> PartData | None:
        """Read a part data file.

        Args:
            part_name: Name of the part (e.g., "models", "datasets").

        Returns:
            PartData if the file exists, None otherwise.
        """
        with self.lock(exclusive=False):
            return self._read_part_unlocked(part_name)

    def write_part(self, part_name: str, part_data: PartData) -> None:
        """Write a part data file.

        Args:
            part_name: Name of the part.
            part_data: The part data to write.
        """
        with self.lock(exclusive=True):
            self._write_part_unlocked(part_name, part_data)

    def get_all_parts(self) -> dict[str, PartData]:
        """Read all part files.

        Returns:
            Dictionary mapping part names to their data.
        """
        result: dict[str, PartData] = {}
        if not self.metadata_dir.exists():
            return result

        with self.lock(exclusive=False):
            for path in self.metadata_dir.glob(f"{PART_PREFIX}*.json"):
                part_name = path.stem[len(PART_PREFIX) :]
                data = json.loads(path.read_text())
                result[part_name] = PartData.from_dict(data)

        return result

    def update_entry_access(self, part_name: str, entry_path: str) -> None:
        """Update the last access time for an entry.

        Args:
            part_name: Name of the part containing the entry.
            entry_path: Relative path of the entry.
        """
        with self.lock(exclusive=True):
            part_data = self._read_part_unlocked(part_name)
            if part_data is None:
                return

            for entry in part_data.entries:
                if entry.path == entry_path:
                    entry.last_access = datetime.now()
                    break

            self._write_part_unlocked(part_name, part_data)

    def remove_entry(self, part_name: str, entry_path: str) -> bool:
        """Remove an entry from the metadata.

        Args:
            part_name: Name of the part containing the entry.
            entry_path: Relative path of the entry.

        Returns:
            True if the entry was removed, False if not found.
        """
        with self.lock(exclusive=True):
            part_data = self._read_part_unlocked(part_name)
            if part_data is None:
                return False

            original_count = len(part_data.entries)
            part_data.entries = [e for e in part_data.entries if e.path != entry_path]

            if len(part_data.entries) < original_count:
                self._write_part_unlocked(part_name, part_data)
                return True

            return False

    def add_entry(
        self,
        part_name: str,
        entry: EntryMetadata,
        update_if_exists: bool = True,
    ) -> None:
        """Add or update an entry in the metadata.

        Args:
            part_name: Name of the part.
            entry: The entry metadata.
            update_if_exists: If True, update existing entry; if False, skip.
        """
        with self.lock(exclusive=True):
            part_data = self._read_part_unlocked(part_name)
            if part_data is None:
                part_data = PartData()

            # Check if entry exists
            for i, existing in enumerate(part_data.entries):
                if existing.path == entry.path:
                    if update_if_exists:
                        part_data.entries[i] = entry
                    self._write_part_unlocked(part_name, part_data)
                    return

            # Add new entry
            part_data.entries.append(entry)
            self._write_part_unlocked(part_name, part_data)


class CacheRegistry:
    """High-level API for libraries to register and update their cache entries.

    This provides a simple interface for ML libraries to integrate with
    ds-cache-cleaner.

    Example usage by a library:
        ```python
        from ds_cache_cleaner.metadata import CacheRegistry

        # Initialize once for your library
        registry = CacheRegistry(
            cache_path=Path("~/.cache/mylib").expanduser(),
            library="mylib",
            description="My ML Library cache",
        )

        # Register a part (e.g., models, datasets)
        registry.register_part("models", "Downloaded model weights")

        # When downloading/accessing a model
        registry.register_entry(
            part="models",
            path="bert-base",
            description="BERT base model",
            size=438_000_000,
        )

        # When accessing an existing entry
        registry.touch("models", "bert-base")

        # When deleting an entry
        registry.remove("models", "bert-base")
        ```
    """

    def __init__(
        self,
        cache_path: Path | str,
        library: str,
        description: str = "",
    ) -> None:
        """Initialize the cache registry.

        Args:
            cache_path: Path to the cache directory.
            library: Name of the library (e.g., "huggingface-hub").
            description: Human-readable description of the cache.
        """
        if isinstance(cache_path, str):
            cache_path = Path(cache_path)
        self.cache_path = cache_path.expanduser()
        self.library = library
        self.description = description
        self._manager = MetadataManager(self.cache_path)
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """Ensure the cache info is initialized."""
        if self._initialized:
            return

        with self._manager.lock(exclusive=True):
            info = self._manager._read_info_unlocked()
            if info is None:
                info = CacheInfo(
                    library=self.library,
                    description=self.description,
                    parts=[],
                )
                self._manager._write_info_unlocked(info)
        self._initialized = True

    def register_part(self, name: str, description: str = "") -> None:
        """Register a new part in the cache.

        Args:
            name: Name of the part (e.g., "models", "datasets").
            description: Human-readable description.
        """
        self._ensure_initialized()

        with self._manager.lock(exclusive=True):
            info = self._manager._read_info_unlocked()
            if info is None:
                info = CacheInfo(library=self.library, description=self.description)

            # Check if part already exists
            for part in info.parts:
                if part.name == name:
                    part.description = description
                    self._manager._write_info_unlocked(info)
                    return

            info.parts.append(PartInfo(name=name, description=description))
            self._manager._write_info_unlocked(info)

            # Initialize empty part file if it doesn't exist
            if self._manager._read_part_unlocked(name) is None:
                self._manager._write_part_unlocked(name, PartData())

    def register_entry(
        self,
        part: str,
        path: str,
        description: str = "",
        size: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a new cache entry or update an existing one.

        This should be called when downloading or creating a new cache entry.

        Args:
            part: Name of the part this entry belongs to.
            path: Relative path of the entry within the cache.
            description: Human-readable description.
            size: Size in bytes (if known).
            metadata: Additional library-specific metadata.
        """
        self._ensure_initialized()

        now = datetime.now()
        entry = EntryMetadata(
            path=path,
            description=description,
            created=now,
            last_access=now,
            size=size,
            metadata=metadata or {},
        )
        self._manager.add_entry(part, entry, update_if_exists=True)

    def touch(self, part: str, path: str) -> None:
        """Update the last access time for an entry.

        This should be called when accessing/using an existing cache entry.

        Args:
            part: Name of the part.
            path: Relative path of the entry.
        """
        self._manager.update_entry_access(part, path)

    def remove(self, part: str, path: str) -> bool:
        """Remove an entry from the metadata.

        This should be called when deleting a cache entry.
        Note: This only removes the metadata, not the actual files.

        Args:
            part: Name of the part.
            path: Relative path of the entry.

        Returns:
            True if the entry was removed, False if not found.
        """
        return self._manager.remove_entry(part, path)

    def update_size(self, part: str, path: str, size: int) -> None:
        """Update the size of an entry.

        Args:
            part: Name of the part.
            path: Relative path of the entry.
            size: New size in bytes.
        """
        with self._manager.lock(exclusive=True):
            part_data = self._manager._read_part_unlocked(part)
            if part_data is None:
                return

            for entry in part_data.entries:
                if entry.path == path:
                    entry.size = size
                    self._manager._write_part_unlocked(part, part_data)
                    return

    def get_entry(self, part: str, path: str) -> EntryMetadata | None:
        """Get metadata for a specific entry.

        Args:
            part: Name of the part.
            path: Relative path of the entry.

        Returns:
            EntryMetadata if found, None otherwise.
        """
        part_data = self._manager.read_part(part)
        if part_data is None:
            return None

        for entry in part_data.entries:
            if entry.path == path:
                return entry
        return None

    def list_entries(self, part: str) -> list[EntryMetadata]:
        """List all entries in a part.

        Args:
            part: Name of the part.

        Returns:
            List of entry metadata.
        """
        part_data = self._manager.read_part(part)
        if part_data is None:
            return []
        return part_data.entries

    def list_parts(self) -> list[PartInfo]:
        """List all parts in the cache.

        Returns:
            List of part information.
        """
        info = self._manager.read_info()
        if info is None:
            return []
        return info.parts
