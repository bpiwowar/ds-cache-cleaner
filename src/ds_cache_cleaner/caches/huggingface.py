"""HuggingFace Hub cache handlers."""

import os
from pathlib import Path

from ds_cache_cleaner.caches.base import CacheEntry, CacheHandler
from ds_cache_cleaner.utils import get_directory_size, get_last_access_time


class HuggingFaceHubBaseHandler(CacheHandler):
    """Base handler for HuggingFace Hub cache."""

    prefix: str = ""  # Override in subclasses

    def __init__(self) -> None:
        super().__init__()

    @property
    def cache_path(self) -> Path:
        # Check HF_HOME first, then default
        hf_home = os.environ.get("HF_HOME")
        if hf_home:
            return Path(hf_home) / "hub"
        return Path.home() / ".cache" / "huggingface" / "hub"

    def _entries_from_filesystem(self) -> list[CacheEntry]:
        """Get cache entries by scanning filesystem for this prefix."""
        if not self.exists:
            return []

        entries = []
        for item in self.cache_path.glob(f"{self.prefix}*"):
            # Skip metadata directory
            name = item.name[len(self.prefix) :]
            if item.is_dir():
                # Parse the name: PREFIX--org--name -> org/name
                display_name = "/".join(name.split("--", 1))

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

        return entries


class HuggingFaceModelsHandler(HuggingFaceHubBaseHandler):
    """Handler for HuggingFace Hub models cache (~/.cache/huggingface/hub/models--)."""

    prefix = "models--"

    @property
    def name(self) -> str:
        return "HuggingFace Models (Hub)"


class HuggingFaceDatasetsHandler(HuggingFaceHubBaseHandler):
    """Handler for HuggingFace Hub datasets cache (~/.cache/huggingface/hub/datasets--)."""

    prefix = "datasets--"

    @property
    def name(self) -> str:
        return "HuggingFace Datasets (Hub)"
