from __future__ import annotations

import pandas as pd
import pytest

from data_pipeline_engine.models.rules import ValidationRuleConfig
from data_pipeline_engine.validation import ValidationExecutionError, validation


def test_validation_succeeds_for_valid_dataset() -> None:
    data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "status": ["active", "inactive", "pending"],
            "score": [91.0, None, 50.0],
        }
    )
    config = ValidationRuleConfig.model_validate(
        {
            "rows": {
                "allow_empty": False,
                "min_rows": 1,
                "max_rows": 10,
                "primary_key": ["id"],
            },
            "columns": {
                "allow_extra": False,
                "schema": [
                    {"name": "id", "type": "int", "required": True, "nullable": False},
                    {"name": "name", "type": "string", "required": True, "nullable": False},
                    {"name": "status", "type": "string", "required": True, "nullable": False},
                    {"name": "score", "type": "float", "required": False, "nullable": True},
                ],
            },
            "constraints": {
                "id": {"unique": True},
                "name": {"pattern": "^[A-Za-z]+$", "min_length": 1, "max_length": 32},
                "status": {"allowed_values": ["active", "inactive", "pending"]},
                "score": {
                    "min": 0,
                    "max": 100,
                    "allow_nan": False,
                    "allow_inf": False,
                    "max_null_fraction": 0.5,
                },
            },
            "row_rules": [
                {
                    "name": "score_required_when_active",
                    "expression": 'status != "inactive" implies score != null',
                },
                {
                    "name": "score_null_when_inactive",
                    "expression": 'status == "inactive" implies score == null',
                },
            ],
        }
    )

    output = validation(data, config)
    assert len(output) == 3


def test_validation_fails_on_primary_key_duplicate() -> None:
    data = pd.DataFrame({"id": [1, 1], "name": ["Alice", "Bob"]})
    config = ValidationRuleConfig.model_validate(
        {
            "rows": {"primary_key": ["id"]},
            "columns": {"schema": [{"name": "id", "type": "int"}]},
        }
    )

    with pytest.raises(ValidationExecutionError):
        validation(data, config)


def test_validation_fails_on_allow_empty() -> None:
    data = pd.DataFrame({"id": pd.Series(dtype="Int64")})
    config = ValidationRuleConfig.model_validate({"rows": {"allow_empty": False}})

    with pytest.raises(ValidationExecutionError):
        validation(data, config)


def test_validation_fails_on_row_rule() -> None:
    data = pd.DataFrame({"status": ["inactive"], "score": [10.0]})
    config = ValidationRuleConfig.model_validate(
        {
            "row_rules": [
                {
                    "name": "score_null_when_inactive",
                    "expression": 'status == "inactive" implies score == null',
                }
            ]
        }
    )

    with pytest.raises(ValidationExecutionError):
        validation(data, config)
