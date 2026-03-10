"""Cache manager package module exposing cache read/write helpers.

Provides pipeline functionality and includes: write_to_cache, read_from_cache.

Usage example:
.. code-block:: python

    from data_pipeline_engine.cache_manager import write_to_cache

    write_to_cache(...)"""

from data_pipeline_engine.cache_manager.manager import read_from_cache, write_to_cache

__all__ = ["write_to_cache", "read_from_cache"]
