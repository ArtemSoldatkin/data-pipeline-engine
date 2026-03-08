from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from data_pipeline_engine.models.rules import (
    DataSkewRuleConfig,
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
    validation_config_path: str | Path | None = None,
    transformation_config_path: str | Path | None = None,
    skew_config_path: str | Path | None = None,
) -> PipelineConfigs:
    if (
        validation_config_path is None
        and transformation_config_path is None
        and skew_config_path is None
    ):
        raise ConfigLoadError(
            "At least one config path must be provided: "
            "validation_config_path, transformation_config_path, or skew_config_path"
        )

    validation = (
        ValidationRuleConfig.model_validate(_load_yaml_file(validation_config_path))
        if validation_config_path is not None
        else None
    )
    transformation = (
        TransformationRuleConfig.model_validate(_load_yaml_file(transformation_config_path))
        if transformation_config_path is not None
        else None
    )
    skew = (
        DataSkewRuleConfig.model_validate(_load_yaml_file(skew_config_path))
        if skew_config_path is not None
        else None
    )

    return PipelineConfigs(validation=validation, transformation=transformation, skew=skew)
