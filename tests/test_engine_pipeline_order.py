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
            cache_dir = Path(temp_dir) / ".data_pipeline_cache"

            calls: list[str] = []

            def transformation_stage(data, config):
                self.assertIsNotNone(config)
                calls.append("transformation")
                return data

            def validation_stage(data, config):
                self.assertIsNotNone(config)
                calls.append("validation")
                return data

            def inspection_stage(data, config, source_csv=None, baseline_csv=None):
                self.assertIsNotNone(config)
                self.assertIsNotNone(source_csv)
                self.assertIsNone(baseline_csv)
                calls.append("inspection")
                return data

            with patch(
                "data_pipeline_engine.engine.transformation", side_effect=transformation_stage
            ), patch("data_pipeline_engine.engine.validation", side_effect=validation_stage), patch(
                "data_pipeline_engine.engine.inspection", side_effect=inspection_stage
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
            self.assertTrue(cache_dir.exists())
            self.assertTrue(Path(result["cached_output_path"]).exists())
            self.assertRegex(
                Path(result["cached_output_path"]).name,
                r"^\d{8}T\d{9,}Z_input\.csv$",
            )

    def test_prunes_cache_to_cache_size(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        inspection_path = project_root / "configs" / "examples" / "inspection.yaml"

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "input.csv"
            csv_path.write_text("id,name,status,score\n1,Alice,active,91.5\n", encoding="utf-8")
            cache_dir = Path(temp_dir) / ".data_pipeline_cache"

            with patch("data_pipeline_engine.engine.transformation", side_effect=lambda d, c: d), patch(
                "data_pipeline_engine.engine.validation", side_effect=lambda d, c: d
            ), patch(
                "data_pipeline_engine.engine.inspection",
                side_effect=lambda d, c, source_csv=None, baseline_csv=None: d,
            ):
                result1 = run_pipeline(
                    csv_path=csv_path,
                    inspection_config_path=inspection_path,
                    cache_size=1,
                )
                result2 = run_pipeline(
                    csv_path=csv_path,
                    inspection_config_path=inspection_path,
                    cache_size=1,
                )

            cached_files = [path for path in cache_dir.iterdir() if path.is_file()]
            self.assertEqual(len(cached_files), 1)
            self.assertEqual(Path(result2["cached_output_path"]), cached_files[0])
            self.assertNotEqual(result1["cached_output_path"], result2["cached_output_path"])

    def test_passes_baseline_file_to_inspection_for_reference_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            csv_path = temp_path / "input.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            baseline_csv_path = temp_path / "baseline.csv"
            baseline_csv_path.write_text("id\n10\n", encoding="utf-8")
            inspection_path = temp_path / "inspection.yaml"
            inspection_path.write_text("baseline:\n  source: reference_dataset\n", encoding="utf-8")

            def inspection_stage(data, config, source_csv=None, baseline_csv=None):
                self.assertIsNotNone(config)
                self.assertEqual(Path(source_csv), csv_path)
                self.assertEqual(Path(baseline_csv), baseline_csv_path)
                return data

            with patch("data_pipeline_engine.engine.transformation", side_effect=lambda d, c: d), patch(
                "data_pipeline_engine.engine.validation", side_effect=lambda d, c: d
            ), patch("data_pipeline_engine.engine.inspection", side_effect=inspection_stage):
                run_pipeline(
                    csv_path=csv_path,
                    inspection_config_path=inspection_path,
                    baseline_file_path=baseline_csv_path,
                )


if __name__ == "__main__":
    unittest.main()
