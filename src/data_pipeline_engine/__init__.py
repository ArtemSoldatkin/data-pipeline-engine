"""Top-level package module for running the data pipeline engine.

Provides pipeline functionality and includes: run_pipeline.

Usage example:
.. code-block:: python

    from data_pipeline_engine import run_pipeline

    run_pipeline(...)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

__all__ = ["run_pipeline"]


def run_pipeline(
    csv_path: str | Path,
    validation_config_path: str | Path | None = None,
    transformation_config_path: str | Path | None = None,
    inspection_config_path: str | Path | None = None,
    cache_size: int = 1,
) -> dict[str, Any]:
    """Run pipeline.
    
    Args:
        csv_path: Path to the input CSV file.
        validation_config_path: Path to the validation YAML config, if provided.
        transformation_config_path: Path to the transformation YAML config, if provided.
        inspection_config_path: Path to the inspection YAML config, if provided.
        cache_size: Maximum number of cache snapshots to keep per source CSV.
    
    Returns:
        Dictionary containing computed results for this operation.
    """
    from data_pipeline_engine.engine import run_pipeline as _run_pipeline

    return _run_pipeline(
        csv_path=csv_path,
        validation_config_path=validation_config_path,
        transformation_config_path=transformation_config_path,
        inspection_config_path=inspection_config_path,
        cache_size=cache_size,
    )
