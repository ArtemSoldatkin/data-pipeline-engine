from __future__ import annotations

import pandas as pd
import pytest

from data_pipeline_engine.inspection import inspection
from data_pipeline_engine.models.rules import (
    InspectionRuleConfig,
    TransformationRuleConfig,
    ValidationRuleConfig,
)
from data_pipeline_engine.transformation import run_transformation
from data_pipeline_engine.validation import ValidationExecutionError, validation


def test_run_transformation_applies_operations() -> None:
    data = pd.DataFrame(
        {
            "user_id": [1, 1, 2],
            "full_name": [" Alice ", " Alice ", "Bob "],
            "status": ["Active", "Active", "Deleted"],
            "score": ["91.5", "91.5", "30.0"],
            "temp_flag": [1, 1, 0],
        }
    )
    config = TransformationRuleConfig.model_validate(
        {
            "columns": {
                "rename": {"user_id": "id", "full_name": "name"},
                "drop": ["temp_flag"],
                "cast": {"score": "float"},
                "normalize": {"name": ["trim"], "status": ["lowercase"]},
                "derive": [],
            },
            "rows": {},
        }
    )

    transformed = run_transformation(data, config)
    assert transformed.columns.tolist() == ["id", "name", "status", "score"]
    assert len(transformed) == 3
    assert transformed["name"].iloc[0] == "Alice"
    assert transformed["status"].iloc[0] == "active"


def test_validation_raises_on_invalid_data() -> None:
    data = pd.DataFrame({"id": [1]})
    config = ValidationRuleConfig.model_validate(
        {
            "rows": {"allow_empty": False},
            "columns": {
                "allow_extra": True,
                "schema": [
                    {"name": "id", "type": "int", "required": True, "nullable": False},
                    {"name": "name", "type": "string", "required": True, "nullable": False},
                ],
            },
        }
    )

    with pytest.raises(ValidationExecutionError):
        validation(data, config)


def test_inspection_returns_data() -> None:
    data = pd.DataFrame({"id": [1, 2], "status": ["active", "inactive"]})
    config = InspectionRuleConfig.model_validate(
        {"categorical_distribution_drift": {"columns": {"status": {"warn_above": 0.1}}}}
    )

    inspected = inspection(data, config)
    assert len(inspected) == 2
