from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from data_pipeline_engine.cache_manager import write_to_cache
from data_pipeline_engine.config_loader import load_pipeline_configs
from data_pipeline_engine.inspection import inspection
from data_pipeline_engine.transformation import StageExecutionError, transformation
from data_pipeline_engine.validation import ValidationExecutionError, validation


class PipelineExecutionError(Exception):
    """Raised when pipeline validations fail."""


def run_pipeline(
    csv_path: str | Path,
    validation_config_path: str | Path | None = None,
    transformation_config_path: str | Path | None = None,
    inspection_config_path: str | Path | None = None,
    baseline_file_path: str | Path | None = None,
    cache_size: int = 1,
) -> dict[str, Any]:
    """Run pipeline over a CSV with optional YAML configs (at least one required)."""
    if (
        transformation_config_path is None
        and validation_config_path is None
        and inspection_config_path is None
    ):
        raise PipelineExecutionError(
            "At least one config path must be provided: "
            "transformation_config_path, validation_config_path, or inspection_config_path"
        )

    configs = load_pipeline_configs(
        transformation_config_path=transformation_config_path,
        validation_config_path=validation_config_path,
        inspection_config_path=inspection_config_path,
    )

    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file does not exist: {csv_file}")

    data = pl.read_csv(csv_file)
    inspection_metrics: dict[str, Any] = {}
    try:
        data = transformation(data, configs.transformation)
        data = validation(data, configs.validation)
        inspection_result = inspection(
            data,
            configs.inspection,
            source_csv=csv_file,
            baseline_csv=baseline_file_path,
            return_metrics=True,
        )
        if isinstance(inspection_result, tuple):
            data, inspection_metrics = inspection_result
        else:
            data = inspection_result
    except (StageExecutionError, ValidationExecutionError, ValueError) as exc:
        raise PipelineExecutionError(str(exc)) from exc

    cached_output = write_to_cache(data=data, source_csv=csv_file, cache_size=cache_size)

    return {
        "status": "success",
        "rows": data.height,
        "columns": data.columns,
        "validation_applied": configs.validation is not None,
        "transformation_applied": configs.transformation is not None,
        "inspection_applied": configs.inspection is not None,
        "inspection_metrics": inspection_metrics,
        "cached_output_path": str(cached_output),
    }
