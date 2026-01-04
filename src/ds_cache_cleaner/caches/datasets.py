"""HuggingFace Datasets cache handler."""

import os
from pathlib import Path

from ds_cache_cleaner.caches.base import CacheHandler


class DatasetsCacheHandler(CacheHandler):
    """Handler for HuggingFace Datasets cache."""

    def __init__(self) -> None:
        super().__init__()

    @property
    def name(self) -> str:
        return "HF Datasets"

    @property
    def cache_path(self) -> Path:
        # Check HF_DATASETS_CACHE first, then HF_HOME, then default
        datasets_cache = os.environ.get("HF_DATASETS_CACHE")
        if datasets_cache:
            return Path(datasets_cache)

        hf_home = os.environ.get("HF_HOME")
        if hf_home:
            return Path(hf_home) / "datasets"

        return Path.home() / ".cache" / "huggingface" / "datasets"
