"""Transformers cache handler."""

import os
from pathlib import Path

from ds_cache_cleaner.caches.base import CacheHandler


class TransformersCacheHandler(CacheHandler):
    """Handler for Transformers cache."""

    def __init__(self) -> None:
        super().__init__()

    @property
    def name(self) -> str:
        return "Transformers"

    @property
    def cache_path(self) -> Path:
        # Check TRANSFORMERS_CACHE first, then HF_HOME, then default
        transformers_cache = os.environ.get("TRANSFORMERS_CACHE")
        if transformers_cache:
            return Path(transformers_cache)

        hf_home = os.environ.get("HF_HOME")
        if hf_home:
            return Path(hf_home) / "transformers"

        return Path.home() / ".cache" / "huggingface" / "transformers"
