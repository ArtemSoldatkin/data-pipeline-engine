from __future__ import annotations

import unittest
from pathlib import Path

from data_pipeline_engine.config_loader import load_pipeline_configs
from data_pipeline_engine.models.rules import ValidationRuleConfig


class ValidationRuleConfigTests(unittest.TestCase):
    def test_loads_new_validation_yaml_shape(self) -> None:
        validation_path = (
            Path(__file__).resolve().parents[1] / "configs" / "examples" / "validation.yaml"
        )

        configs = load_pipeline_configs(validation_config_path=validation_path)
        validation = configs.validation

        self.assertIsNotNone(validation)
        assert validation is not None
        self.assertFalse(validation.allow_empty)
        self.assertEqual(validation.min_rows, 1)
        self.assertEqual(validation.max_rows, 100000)
        self.assertEqual(validation.primary_key, ["id"])
        self.assertEqual(len(validation.schema), 4)
        self.assertFalse(validation.allow_extra_columns)
        self.assertIn("status", validation.constraints)
        self.assertEqual(len(validation.row_rules), 2)
        self.assertEqual(validation.row_rules[0].name, "score_required_when_active")

    def test_supports_legacy_flat_validation_shape(self) -> None:
        config = ValidationRuleConfig.model_validate(
            {
                "allow_empty": False,
                "min_rows": 2,
                "max_rows": 10,
                "allow_extra_columns": False,
                "schema": [{"name": "id", "type": "int", "required": True, "nullable": False}],
            }
        )

        self.assertFalse(config.allow_empty)
        self.assertEqual(config.min_rows, 2)
        self.assertEqual(config.max_rows, 10)
        self.assertFalse(config.allow_extra_columns)
        self.assertEqual(config.schema[0].name, "id")


if __name__ == "__main__":
    unittest.main()
