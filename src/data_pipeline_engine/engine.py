"""Pipeline engine module for orchestrating transformation, validation, and inspection.

Provides pipeline functionality and includes: run_pipeline, PipelineExecutionError.

Usage example:
.. code-block:: python

    from data_pipeline_engine.engine import PipelineExecutionError

    PipelineExecutionError(...)"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_pipeline_engine.cache_manager import write_to_cache
from data_pipeline_engine.config_loader import load_pipeline_configs
from data_pipeline_engine.inspection import inspection
from data_pipeline_engine.inspection.types import InspectionMetrics, InspectionStatusOnlyMetrics
from data_pipeline_engine.transformation import StageExecutionError, transformation
from data_pipeline_engine.types import PipelineRunResult
from data_pipeline_engine.validation import ValidationExecutionError, validation


class PipelineExecutionError(Exception):
    """Raised when pipeline validations fail."""


__all__ = ["PipelineExecutionError", "run_pipeline"]


def run_pipeline(
    csv_path: str | Path,
    validation_config_path: str | Path | None = None,
    transformation_config_path: str | Path | None = None,
    inspection_config_path: str | Path | None = None,
    reference_dataset_path: str | Path | None = None,
    cache_size: int = 1,
) -> PipelineRunResult:
    """Run pipeline.
    
    Args:
        csv_path: Path to the input CSV file.
        validation_config_path: Path to the validation YAML config, if provided.
        transformation_config_path: Path to the transformation YAML config, if provided.
        inspection_config_path: Path to the inspection YAML config, if provided.
        reference_dataset_path: Path to the baseline CSV used for reference-dataset inspection mode.
        cache_size: Maximum number of cache snapshots to keep per source CSV.
    
    Returns:
        Dictionary containing computed results for this operation.
    
    Raises:
        ValueError: If provided arguments are invalid.
    """
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

    data = pd.read_csv(csv_file)
    inspection_metrics: InspectionMetrics | InspectionStatusOnlyMetrics = {
        "overall_status": "not_configured"
    }
    try:
        data = transformation(data, configs.transformation)
        data = validation(data, configs.validation)
        inspection_result = inspection(
            data,
            configs.inspection,
            source_csv=csv_file,
            baseline_csv=reference_dataset_path,
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
        "rows": len(data),
        "columns": list(data.columns),
        "validation_applied": configs.validation is not None,
        "transformation_applied": configs.transformation is not None,
        "inspection_applied": configs.inspection is not None,
        "inspection_metrics": inspection_metrics,
        "cached_output_path": str(cached_output),
    }
