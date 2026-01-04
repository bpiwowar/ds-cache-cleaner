"""datamaestro cache handler."""

import os
from pathlib import Path

from ds_cache_cleaner.caches.base import CacheEntry, CacheHandler
from ds_cache_cleaner.utils import get_directory_size, get_last_access_time


def _get_datamaestro_home() -> Path:
    """Get the datamaestro home directory."""
    dm_home = os.environ.get("DATAMAESTRO_HOME")
    if dm_home:
        return Path(dm_home)
    return Path.home() / "datamaestro"


class DatamaestroCacheHandler(CacheHandler):
    """Handler for datamaestro cache folder (partial downloads, processing)."""

    def __init__(self) -> None:
        super().__init__()

    @property
    def name(self) -> str:
        return "datamaestro (cache)"

    @property
    def cache_path(self) -> Path:
        return _get_datamaestro_home() / "cache"


class DatamaestroDataHandler(CacheHandler):
    """Handler for datamaestro data folder (downloaded datasets)."""

    def __init__(self) -> None:
        super().__init__()

    @property
    def name(self) -> str:
        return "datamaestro (data)"

    @property
    def cache_path(self) -> Path:
        return _get_datamaestro_home() / "data"

    def _entries_from_filesystem(self) -> list[CacheEntry]:
        """Get data entries, walking the dataset structure."""
        if not self.exists:
            return []

        entries = []
        # Walk through the data directory structure
        # Structure: data/<repository>/<dataset_path>
        for repo in self.cache_path.iterdir():
            # Skip metadata directory
            if repo.name == "ds-cache-cleaner":
                continue

            if repo.is_dir():
                # Each repository can have nested dataset paths
                for dataset in repo.rglob("*"):
                    # Only include leaf directories (actual dataset folders)
                    if dataset.is_dir() and not any(dataset.iterdir()):
                        continue
                    if dataset.is_dir():
                        # Check if it contains actual data files
                        has_files = any(f.is_file() for f in dataset.iterdir())
                        if has_files:
                            size = get_directory_size(dataset)
                            last_access = get_last_access_time(dataset)
                            # Create a readable name from the path
                            rel_path = dataset.relative_to(self.cache_path)
                            entries.append(
                                CacheEntry(
                                    name=str(rel_path),
                                    path=dataset,
                                    size=size,
                                    handler_name=self.name,
                                    last_access=last_access,
                                )
                            )

        # If no nested entries found, fall back to top-level directories
        if not entries:
            for item in self.cache_path.iterdir():
                if item.name == "ds-cache-cleaner":
                    continue
                if item.is_dir():
                    size = get_directory_size(item)
                    last_access = get_last_access_time(item)
                    entries.append(
                        CacheEntry(
                            name=item.name,
                            path=item,
                            size=size,
                            handler_name=self.name,
                            last_access=last_access,
                        )
                    )

        return entries
