"""ds-cache-cleaner: Clean up cached data from ML/data science libraries."""

try:
    from ds_cache_cleaner.version import __version__
except ImportError:
    __version__ = "0.0.0.dev0"

# Export the metadata API for use by ML libraries
from ds_cache_cleaner.metadata import (
    CacheRegistry,
    EntryMetadata,
    MetadataManager,
    PartInfo,
)

__all__ = [
    "__version__",
    "CacheRegistry",
    "EntryMetadata",
    "MetadataManager",
    "PartInfo",
]
