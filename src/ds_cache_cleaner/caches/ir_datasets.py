"""ir_datasets cache handler."""

import os
from pathlib import Path

from ds_cache_cleaner.caches.base import CacheHandler


class IrDatasetsCacheHandler(CacheHandler):
    """Handler for ir_datasets cache (~/.ir_datasets)."""

    def __init__(self) -> None:
        super().__init__()

    @property
    def name(self) -> str:
        return "ir_datasets"

    @property
    def cache_path(self) -> Path:
        # Check IR_DATASETS_HOME first, then default
        ir_home = os.environ.get("IR_DATASETS_HOME")
        if ir_home:
            return Path(ir_home)
        return Path.home() / ".ir_datasets"
