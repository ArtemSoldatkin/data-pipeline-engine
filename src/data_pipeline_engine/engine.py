from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from data_pipeline_engine.config_loader import load_pipeline_configs
from data_pipeline_engine.models.rules import (
    InspectionRuleConfig,
    TransformationRuleConfig,
    ValidationRuleConfig,
)


class PipelineExecutionError(Exception):
    """Raised when pipeline validations fail."""


def run_transformation(data: pl.DataFrame, config: TransformationRuleConfig | None) -> pl.DataFrame:
    """Placeholder transformation stage."""
    return data


def run_validation(data: pl.DataFrame, config: ValidationRuleConfig | None) -> pl.DataFrame:
    """Placeholder validation stage."""
    return data


def run_inspection(data: pl.DataFrame, config: InspectionRuleConfig | None) -> pl.DataFrame:
    """Placeholder inspection stage."""
    return data


def run_pipeline(
    csv_path: str | Path,
    validation_config_path: str | Path | None = None,
    transformation_config_path: str | Path | None = None,
    inspection_config_path: str | Path | None = None,
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
    data = run_transformation(data, configs.transformation)
    data = run_validation(data, configs.validation)
    data = run_inspection(data, configs.inspection)

    return {
        "status": "success",
        "rows": data.height,
        "columns": data.columns,
        "validation_applied": configs.validation is not None,
        "transformation_applied": configs.transformation is not None,
        "inspection_applied": configs.inspection is not None,
    }
