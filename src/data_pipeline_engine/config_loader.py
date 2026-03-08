from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from data_pipeline_engine.models.rules import (
    InspectionRuleConfig,
    PipelineConfigs,
    TransformationRuleConfig,
    ValidationRuleConfig,
)


class ConfigLoadError(Exception):
    """Raised when YAML config cannot be loaded or parsed."""


def _load_yaml_file(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        raise ConfigLoadError(f"Config file does not exist: {file_path}")

    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ConfigLoadError(f"Failed to load YAML config from {file_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigLoadError(f"Config root must be a mapping/object: {file_path}")

    return data


def load_pipeline_configs(
    transformation_config_path: str | Path | None = None,
    validation_config_path: str | Path | None = None,
    inspection_config_path: str | Path | None = None,
) -> PipelineConfigs:
    if (
        transformation_config_path is None
        and validation_config_path is None
        and inspection_config_path is None
    ):
        raise ConfigLoadError(
            "At least one config path must be provided: "
            "transformation_config_path, validation_config_path, or inspection_config_path"
        )

    transformation = (
        TransformationRuleConfig.model_validate(_load_yaml_file(transformation_config_path))
        if transformation_config_path is not None
        else None
    )
    validation = (
        ValidationRuleConfig.model_validate(_load_yaml_file(validation_config_path))
        if validation_config_path is not None
        else None
    )
    inspection = (
        InspectionRuleConfig.model_validate(_load_yaml_file(inspection_config_path))
        if inspection_config_path is not None
        else None
    )

    return PipelineConfigs(
        validation=validation, transformation=transformation, inspection=inspection
    )
