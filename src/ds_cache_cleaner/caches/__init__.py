"""Cache handlers for various ML/data science libraries."""

from ds_cache_cleaner.caches.base import CacheEntry, CacheHandler
from ds_cache_cleaner.caches.datamaestro import (
    DatamaestroCacheHandler,
    DatamaestroDataHandler,
)
from ds_cache_cleaner.caches.datasets import DatasetsCacheHandler
from ds_cache_cleaner.caches.huggingface import (
    HuggingFaceDatasetsHandler,
    HuggingFaceModelsHandler,
)
from ds_cache_cleaner.caches.ir_datasets import IrDatasetsCacheHandler
from ds_cache_cleaner.caches.transformers import TransformersCacheHandler

ALL_HANDLERS: list[type[CacheHandler]] = [
    HuggingFaceModelsHandler,
    HuggingFaceDatasetsHandler,
    TransformersCacheHandler,
    DatasetsCacheHandler,
    IrDatasetsCacheHandler,
    DatamaestroCacheHandler,
    DatamaestroDataHandler,
]


def get_all_handlers() -> list[CacheHandler]:
    """Get instances of all available cache handlers."""
    return [handler() for handler in ALL_HANDLERS]


__all__ = [
    "CacheEntry",
    "CacheHandler",
    "HuggingFaceModelsHandler",
    "HuggingFaceDatasetsHandler",
    "TransformersCacheHandler",
    "DatasetsCacheHandler",
    "IrDatasetsCacheHandler",
    "DatamaestroCacheHandler",
    "DatamaestroDataHandler",
    "ALL_HANDLERS",
    "get_all_handlers",
]
