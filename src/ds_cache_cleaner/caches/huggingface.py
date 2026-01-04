"""HuggingFace Hub cache handler."""

import os
from pathlib import Path

from ds_cache_cleaner.caches.base import CacheEntry, CacheHandler
from ds_cache_cleaner.utils import get_directory_size, get_last_access_time


class HuggingFaceCacheHandler(CacheHandler):
    """Handler for HuggingFace Hub cache (~/.cache/huggingface/hub)."""

    def __init__(self) -> None:
        super().__init__()

    @property
    def name(self) -> str:
        return "HuggingFace Hub"

    @property
    def cache_path(self) -> Path:
        # Check HF_HOME first, then default
        hf_home = os.environ.get("HF_HOME")
        if hf_home:
            return Path(hf_home) / "hub"
        return Path.home() / ".cache" / "huggingface" / "hub"

    def _entries_from_filesystem(self) -> list[CacheEntry]:
        """Get cache entries by scanning filesystem, grouping by model/dataset."""
        if not self.exists:
            return []

        entries = []
        # HF hub cache structure: models--org--name or datasets--org--name
        for item in self.cache_path.iterdir():
            # Skip metadata directory
            if item.name == "ds-cache-cleaner":
                continue

            if item.is_dir() and (
                item.name.startswith("models--") or item.name.startswith("datasets--")
            ):
                # Parse the name: models--org--name -> org/name
                parts = item.name.split("--", 2)
                if len(parts) >= 3:
                    display_name = f"{parts[0]}: {parts[1]}/{parts[2]}"
                else:
                    display_name = item.name

                size = get_directory_size(item)
                last_access = get_last_access_time(item)
                entries.append(
                    CacheEntry(
                        name=display_name,
                        path=item,
                        size=size,
                        handler_name=self.name,
                        last_access=last_access,
                    )
                )
            elif item.is_dir():
                # Other directories (like .locks)
                size = get_directory_size(item)
                if size > 0:
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
