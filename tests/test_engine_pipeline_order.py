from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data_pipeline_engine.engine import run_pipeline


class EnginePipelineOrderTests(unittest.TestCase):
    def test_runs_stages_in_requested_order(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        transformation_path = project_root / "configs" / "examples" / "transformation.yaml"
        validation_path = project_root / "configs" / "examples" / "validation.yaml"
        inspection_path = project_root / "configs" / "examples" / "inspection.yaml"

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "input.csv"
            csv_path.write_text("id,name,status,score\n1,Alice,active,91.5\n", encoding="utf-8")

            calls: list[str] = []

            def transformation_stage(data, config):
                self.assertIsNotNone(config)
                calls.append("transformation")
                return data

            def validation_stage(data, config):
                self.assertIsNotNone(config)
                calls.append("validation")
                return data

            def inspection_stage(data, config):
                self.assertIsNotNone(config)
                calls.append("inspection")
                return data

            with patch(
                "data_pipeline_engine.engine.run_transformation", side_effect=transformation_stage
            ), patch("data_pipeline_engine.engine.run_validation", side_effect=validation_stage), patch(
                "data_pipeline_engine.engine.run_inspection", side_effect=inspection_stage
            ):
                result = run_pipeline(
                    csv_path=csv_path,
                    transformation_config_path=transformation_path,
                    validation_config_path=validation_path,
                    inspection_config_path=inspection_path,
                )

        self.assertEqual(calls, ["transformation", "validation", "inspection"])
        self.assertEqual(result["status"], "success")
        self.assertTrue(result["transformation_applied"])
        self.assertTrue(result["validation_applied"])
        self.assertTrue(result["inspection_applied"])


if __name__ == "__main__":
    unittest.main()
