"""Cache manager for pipeline output snapshots."""

from data_pipeline_engine.cache_manager.manager import read_from_cache, write_to_cache

__all__ = ["write_to_cache", "read_from_cache"]
