from __future__ import annotations

from pathlib import Path

from data_pipeline_engine.config_loader import load_pipeline_configs
from data_pipeline_engine.models.rules import ValidationRuleConfig


def test_loads_new_validation_yaml_shape() -> None:
    validation_path = Path(__file__).resolve().parents[1] / "configs" / "examples" / "validation.yaml"

    configs = load_pipeline_configs(validation_config_path=validation_path)
    validation = configs.validation

    assert validation is not None
    assert validation.allow_empty is False
    assert validation.min_rows == 3
    assert validation.max_rows == 100000
    assert validation.primary_key == ["id"]
    assert len(validation.schema) == 6
    assert validation.allow_extra_columns is False
    assert "status" in validation.constraints
    assert len(validation.row_rules) == 2
    assert validation.row_rules[0].name == "score_required_when_active_or_pending"


def test_supports_legacy_flat_validation_shape() -> None:
    config = ValidationRuleConfig.model_validate(
        {
            "allow_empty": False,
            "min_rows": 2,
            "max_rows": 10,
            "allow_extra_columns": False,
            "schema": [{"name": "id", "type": "int", "required": True, "nullable": False}],
        }
    )

    assert config.allow_empty is False
    assert config.min_rows == 2
    assert config.max_rows == 10
    assert config.allow_extra_columns is False
    assert config.schema[0].name == "id"
