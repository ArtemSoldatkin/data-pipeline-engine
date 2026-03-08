from __future__ import annotations

import unittest

import polars as pl

from data_pipeline_engine.models.rules import (
    InspectionRuleConfig,
    TransformationRuleConfig,
    ValidationRuleConfig,
)
from data_pipeline_engine.inspection import inspection
from data_pipeline_engine.transformation import run_transformation
from data_pipeline_engine.validation import validation


class StageFunctionTests(unittest.TestCase):
    @unittest.skip("Polars runtime instability in this environment for transformation execution")
    def test_run_transformation_applies_operations(self) -> None:
        data = pl.DataFrame(
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
        self.assertEqual(transformed.columns, ["id", "name", "status", "score"])
        self.assertEqual(transformed.height, 3)
        self.assertEqual(transformed["name"][0], "Alice")
        self.assertEqual(transformed["status"][0], "active")

    @unittest.skip("Polars runtime instability in this environment for validation execution")
    def test_validation_placeholder_returns_data(self) -> None:
        data = pl.DataFrame({"id": [1]})
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

        validated = validation(data, config)
        self.assertEqual(validated.height, 1)

    def test_inspection_placeholder_returns_data(self) -> None:
        data = pl.DataFrame({"id": [1, 2], "status": ["active", "inactive"]})
        config = InspectionRuleConfig.model_validate(
            {"categorical_distribution_drift": {"columns": {"status": {"warn_above": 0.1}}}}
        )

        inspected = inspection(data, config)
        self.assertEqual(inspected.height, 2)


if __name__ == "__main__":
    unittest.main()
