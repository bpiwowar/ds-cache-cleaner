# ds-cache-cleaner

Clean up cached data from ML/data science libraries.

## Supported Caches

- **HuggingFace Hub** - `~/.cache/huggingface/hub`
- **Transformers** - `~/.cache/huggingface/transformers`
- **HF Datasets** - `~/.cache/huggingface/datasets`
- **ir_datasets** - `~/.ir_datasets`
- **datamaestro (cache)** - `~/datamaestro/cache` (partial downloads, processing)
- **datamaestro (data)** - `~/datamaestro/data` (downloaded datasets)

## Installation

```bash
pip install ds-cache-cleaner
```

Or with uv:

```bash
uv pip install ds-cache-cleaner
```

## Usage

### List caches

```bash
ds-cache-cleaner list
```

### Show cache entries

```bash
ds-cache-cleaner show
ds-cache-cleaner show -c "HuggingFace Hub"
```

### Clean caches

```bash
# Interactive mode
ds-cache-cleaner clean

# Clean specific cache
ds-cache-cleaner clean -c "HuggingFace Hub"

# Clean all without prompting
ds-cache-cleaner clean --all

# Dry run
ds-cache-cleaner clean --dry-run
```

### Interactive TUI

```bash
ds-cache-cleaner tui
```

## Library Integration

ML libraries can integrate with ds-cache-cleaner to provide rich metadata about their cached data. This enables better descriptions, accurate last-access times, and more.

### Metadata Format

The metadata is stored in a `ds-cache-cleaner/` folder inside each cache directory:

```
~/.cache/mylib/
├── ds-cache-cleaner/
│   ├── lock                    # Lock file for concurrent access
│   ├── information.json        # Cache info and parts list
│   └── part_models.json        # Entries for "models" part
└── ... (actual cache data)
```

### Using the CacheRegistry API

```python
from ds_cache_cleaner import CacheRegistry

# Initialize once for your library
registry = CacheRegistry(
    cache_path="~/.cache/mylib",
    library="mylib",
    description="My ML Library cache",
)

# Register a part (e.g., models, datasets)
registry.register_part("models", "Downloaded model weights")

# When downloading a new model
registry.register_entry(
    part="models",
    path="bert-base",  # relative path within cache
    description="BERT base model",
    size=438_000_000,
)

# When accessing an existing entry (updates last_access time)
registry.touch("models", "bert-base")

# When deleting an entry (removes from metadata)
registry.remove("models", "bert-base")
```

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
hatch run test

# Lint
hatch run lint:check

# Format
hatch run lint:fix
```

## License

MIT
