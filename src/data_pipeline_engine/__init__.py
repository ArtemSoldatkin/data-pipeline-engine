"""Data pipeline engine package."""

from __future__ import annotations

from pathlib import Path
from typing import Any

__all__ = ["run_pipeline"]


def run_pipeline(
    csv_path: str | Path,
    validation_config_path: str | Path | None = None,
    transformation_config_path: str | Path | None = None,
    skew_config_path: str | Path | None = None,
) -> dict[str, Any]:
    from data_pipeline_engine.engine import run_pipeline as _run_pipeline

    return _run_pipeline(
        csv_path=csv_path,
        validation_config_path=validation_config_path,
        transformation_config_path=transformation_config_path,
        skew_config_path=skew_config_path,
    )
