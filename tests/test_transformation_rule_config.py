from __future__ import annotations

import unittest
from pathlib import Path

from data_pipeline_engine.config_loader import load_pipeline_configs


class TransformationRuleConfigTests(unittest.TestCase):
    def test_loads_transformation_yaml_shape(self) -> None:
        transformation_path = (
            Path(__file__).resolve().parents[1]
            / "configs"
            / "examples"
            / "transformation.yaml"
        )

        configs = load_pipeline_configs(transformation_config_path=transformation_path)
        transformation = configs.transformation

        self.assertIsNotNone(transformation)
        assert transformation is not None

        self.assertEqual(transformation.columns.rename["user_id"], "id")
        self.assertIn("temp_flag", transformation.columns.drop)
        self.assertEqual(transformation.columns.cast["score"].value, "float")
        self.assertEqual(transformation.columns.cast["created_at"].value, "timestamp")
        self.assertEqual(transformation.columns.normalize["status"], ["lowercase"])
        self.assertEqual(transformation.columns.derive[0].column, "score_bucket")
        self.assertEqual(transformation.rows.filter[0].expression, "status != 'deleted'")
        self.assertIsNotNone(transformation.rows.deduplication)
        assert transformation.rows.deduplication is not None
        self.assertEqual(transformation.rows.deduplication.keys, ["id"])
        self.assertEqual(transformation.rows.deduplication.strategy, "keep_first")


if __name__ == "__main__":
    unittest.main()
