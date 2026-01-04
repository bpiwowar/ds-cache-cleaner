"""Tests for the metadata module."""

from datetime import datetime
from pathlib import Path

import pytest
from ds_cache_cleaner.metadata import (
    CacheInfo,
    CacheRegistry,
    EntryMetadata,
    MetadataManager,
    PartData,
    PartInfo,
)


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache directory."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir


class TestEntryMetadata:
    """Tests for EntryMetadata dataclass."""

    def test_to_dict_minimal(self):
        """Test conversion with minimal fields."""
        entry = EntryMetadata(path="test/path")
        result = entry.to_dict()
        assert result == {"path": "test/path"}

    def test_to_dict_full(self):
        """Test conversion with all fields."""
        now = datetime(2024, 1, 15, 10, 30, 0)
        entry = EntryMetadata(
            path="test/path",
            description="Test entry",
            created=now,
            last_access=now,
            size=1000,
            metadata={"key": "value"},
        )
        result = entry.to_dict()
        assert result["path"] == "test/path"
        assert result["description"] == "Test entry"
        assert result["created"] == "2024-01-15T10:30:00"
        assert result["size"] == 1000
        assert result["metadata"] == {"key": "value"}

    def test_from_dict_minimal(self):
        """Test creation from minimal dict."""
        data = {"path": "test/path"}
        entry = EntryMetadata.from_dict(data)
        assert entry.path == "test/path"
        assert entry.description == ""
        assert entry.created is None

    def test_from_dict_full(self):
        """Test creation from full dict."""
        data = {
            "path": "test/path",
            "description": "Test entry",
            "created": "2024-01-15T10:30:00",
            "last_access": "2024-01-20T14:00:00",
            "size": 1000,
            "metadata": {"key": "value"},
        }
        entry = EntryMetadata.from_dict(data)
        assert entry.path == "test/path"
        assert entry.description == "Test entry"
        assert entry.created == datetime(2024, 1, 15, 10, 30, 0)
        assert entry.last_access == datetime(2024, 1, 20, 14, 0, 0)
        assert entry.size == 1000
        assert entry.metadata == {"key": "value"}


class TestCacheInfo:
    """Tests for CacheInfo dataclass."""

    def test_to_dict(self):
        """Test conversion to dict."""
        info = CacheInfo(
            library="test-lib",
            description="Test library",
            parts=[PartInfo(name="models", description="Model files")],
        )
        result = info.to_dict()
        assert result["version"] == 1
        assert result["library"] == "test-lib"
        assert result["description"] == "Test library"
        assert len(result["parts"]) == 1
        assert result["parts"][0]["name"] == "models"

    def test_from_dict(self):
        """Test creation from dict."""
        data = {
            "version": 1,
            "library": "test-lib",
            "description": "Test library",
            "parts": [{"name": "models", "description": "Model files"}],
        }
        info = CacheInfo.from_dict(data)
        assert info.library == "test-lib"
        assert info.description == "Test library"
        assert len(info.parts) == 1
        assert info.parts[0].name == "models"


class TestMetadataManager:
    """Tests for MetadataManager."""

    def test_exists_false(self, temp_cache: Path):
        """Test exists property when no metadata."""
        manager = MetadataManager(temp_cache)
        assert not manager.exists

    def test_write_and_read_info(self, temp_cache: Path):
        """Test writing and reading cache info."""
        manager = MetadataManager(temp_cache)

        info = CacheInfo(
            library="test-lib",
            description="Test library",
            parts=[PartInfo(name="models", description="Model files")],
        )
        manager.write_info(info)

        assert manager.exists

        read_info = manager.read_info()
        assert read_info is not None
        assert read_info.library == "test-lib"
        assert len(read_info.parts) == 1

    def test_write_and_read_part(self, temp_cache: Path):
        """Test writing and reading part data."""
        manager = MetadataManager(temp_cache)

        part_data = PartData(
            entries=[
                EntryMetadata(path="model1", description="First model", size=1000),
                EntryMetadata(path="model2", description="Second model", size=2000),
            ]
        )
        manager.write_part("models", part_data)

        read_data = manager.read_part("models")
        assert read_data is not None
        assert len(read_data.entries) == 2
        assert read_data.entries[0].path == "model1"

    def test_get_all_parts(self, temp_cache: Path):
        """Test getting all parts."""
        manager = MetadataManager(temp_cache)

        manager.write_part("models", PartData(entries=[EntryMetadata(path="m1")]))
        manager.write_part("datasets", PartData(entries=[EntryMetadata(path="d1")]))

        all_parts = manager.get_all_parts()
        assert "models" in all_parts
        assert "datasets" in all_parts

    def test_add_entry(self, temp_cache: Path):
        """Test adding an entry."""
        manager = MetadataManager(temp_cache)

        entry = EntryMetadata(path="new-model", description="New model")
        manager.add_entry("models", entry)

        part_data = manager.read_part("models")
        assert part_data is not None
        assert len(part_data.entries) == 1
        assert part_data.entries[0].path == "new-model"

    def test_add_entry_update_existing(self, temp_cache: Path):
        """Test updating an existing entry."""
        manager = MetadataManager(temp_cache)

        entry1 = EntryMetadata(path="model", description="Original")
        manager.add_entry("models", entry1)

        entry2 = EntryMetadata(path="model", description="Updated")
        manager.add_entry("models", entry2, update_if_exists=True)

        part_data = manager.read_part("models")
        assert part_data is not None
        assert len(part_data.entries) == 1
        assert part_data.entries[0].description == "Updated"

    def test_remove_entry(self, temp_cache: Path):
        """Test removing an entry."""
        manager = MetadataManager(temp_cache)

        part_data = PartData(
            entries=[
                EntryMetadata(path="model1"),
                EntryMetadata(path="model2"),
            ]
        )
        manager.write_part("models", part_data)

        result = manager.remove_entry("models", "model1")
        assert result is True

        read_data = manager.read_part("models")
        assert read_data is not None
        assert len(read_data.entries) == 1
        assert read_data.entries[0].path == "model2"

    def test_remove_entry_not_found(self, temp_cache: Path):
        """Test removing a non-existent entry."""
        manager = MetadataManager(temp_cache)

        part_data = PartData(entries=[EntryMetadata(path="model1")])
        manager.write_part("models", part_data)

        result = manager.remove_entry("models", "nonexistent")
        assert result is False

    def test_update_entry_access(self, temp_cache: Path):
        """Test updating entry access time."""
        manager = MetadataManager(temp_cache)

        old_time = datetime(2024, 1, 1, 0, 0, 0)
        part_data = PartData(
            entries=[EntryMetadata(path="model", last_access=old_time)]
        )
        manager.write_part("models", part_data)

        manager.update_entry_access("models", "model")

        read_data = manager.read_part("models")
        assert read_data is not None
        assert read_data.entries[0].last_access is not None
        assert read_data.entries[0].last_access > old_time


class TestCacheRegistry:
    """Tests for CacheRegistry high-level API."""

    def test_register_part(self, temp_cache: Path):
        """Test registering a part."""
        registry = CacheRegistry(
            cache_path=temp_cache,
            library="test-lib",
            description="Test library",
        )
        registry.register_part("models", "Model files")

        parts = registry.list_parts()
        assert len(parts) == 1
        assert parts[0].name == "models"
        assert parts[0].description == "Model files"

    def test_register_entry(self, temp_cache: Path):
        """Test registering an entry."""
        registry = CacheRegistry(
            cache_path=temp_cache,
            library="test-lib",
        )
        registry.register_part("models")
        registry.register_entry(
            part="models",
            path="bert-base",
            description="BERT model",
            size=1000000,
        )

        entries = registry.list_entries("models")
        assert len(entries) == 1
        assert entries[0].path == "bert-base"
        assert entries[0].description == "BERT model"
        assert entries[0].size == 1000000
        assert entries[0].created is not None
        assert entries[0].last_access is not None

    def test_touch(self, temp_cache: Path):
        """Test touching an entry updates last_access."""
        registry = CacheRegistry(cache_path=temp_cache, library="test-lib")
        registry.register_part("models")
        registry.register_entry(part="models", path="model1")

        # Get initial last_access
        entry = registry.get_entry("models", "model1")
        assert entry is not None
        initial_access = entry.last_access
        assert initial_access is not None

        # Touch and verify update
        registry.touch("models", "model1")
        entry = registry.get_entry("models", "model1")
        assert entry is not None
        assert entry.last_access is not None
        assert entry.last_access >= initial_access

    def test_remove(self, temp_cache: Path):
        """Test removing an entry."""
        registry = CacheRegistry(cache_path=temp_cache, library="test-lib")
        registry.register_part("models")
        registry.register_entry(part="models", path="model1")
        registry.register_entry(part="models", path="model2")

        result = registry.remove("models", "model1")
        assert result is True

        entries = registry.list_entries("models")
        assert len(entries) == 1
        assert entries[0].path == "model2"

    def test_get_entry(self, temp_cache: Path):
        """Test getting a specific entry."""
        registry = CacheRegistry(cache_path=temp_cache, library="test-lib")
        registry.register_part("models")
        registry.register_entry(
            part="models",
            path="bert-base",
            description="BERT model",
        )

        entry = registry.get_entry("models", "bert-base")
        assert entry is not None
        assert entry.path == "bert-base"
        assert entry.description == "BERT model"

        # Non-existent entry
        entry = registry.get_entry("models", "nonexistent")
        assert entry is None

    def test_update_size(self, temp_cache: Path):
        """Test updating entry size."""
        registry = CacheRegistry(cache_path=temp_cache, library="test-lib")
        registry.register_part("models")
        registry.register_entry(part="models", path="model1", size=1000)

        registry.update_size("models", "model1", 2000)

        entry = registry.get_entry("models", "model1")
        assert entry is not None
        assert entry.size == 2000

    def test_list_parts_empty(self, temp_cache: Path):
        """Test listing parts when none registered."""
        registry = CacheRegistry(cache_path=temp_cache, library="test-lib")
        parts = registry.list_parts()
        assert parts == []

    def test_list_entries_empty(self, temp_cache: Path):
        """Test listing entries for non-existent part."""
        registry = CacheRegistry(cache_path=temp_cache, library="test-lib")
        entries = registry.list_entries("nonexistent")
        assert entries == []
