from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data_pipeline_engine.inspection.baseline import evaluate_baseline, load_baseline_frames
from data_pipeline_engine.models.rules import InspectionBaselineConfig


class InspectionBaselineTests(unittest.TestCase):
    def test_reference_dataset_requires_baseline_file_path(self) -> None:
        config = InspectionBaselineConfig(source="reference_dataset")

        with self.assertRaisesRegex(
            ValueError, "baseline_file_path must be provided when baseline source is reference_dataset"
        ):
            load_baseline_frames(config)

    def test_reference_dataset_uses_baseline_file_instead_of_cache(self) -> None:
        config = InspectionBaselineConfig(source="reference_dataset")
        fake_frame = type("FakeFrame", (), {"height": 1})()

        with tempfile.TemporaryDirectory() as temp_dir:
            baseline_csv = Path(temp_dir) / "baseline.csv"
            baseline_csv.write_text("id\n1\n", encoding="utf-8")

            with patch("data_pipeline_engine.inspection.baseline.read_from_cache") as read_cache:
                read_cache.return_value = [fake_frame]
                result = evaluate_baseline(config, baseline_csv=baseline_csv)

        read_cache.assert_called_once_with(
            source_csv=Path("."),
            strategy="reference_dataset",
            reference_csv=baseline_csv,
        )
        self.assertEqual(result["source"], "reference_dataset")
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["baseline_runs_available"], 1)
        self.assertEqual(result["baseline_rows"], [1])

    def test_previous_run_uses_cache(self) -> None:
        config = InspectionBaselineConfig(source="previous_run")
        baseline_frame = type("FakeFrame", (), {"height": 2})()

        with patch(
            "data_pipeline_engine.inspection.baseline.read_from_cache",
            return_value=[baseline_frame, baseline_frame],
        ) as read_cache:
            result = evaluate_baseline(config, source_csv="input.csv")

        read_cache.assert_called_once_with(
            source_csv="input.csv",
            strategy="previous_run",
            reference_csv=None,
        )
        self.assertEqual(result["source"], "previous_run")
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["baseline_runs_available"], 2)
        self.assertEqual(result["baseline_rows"], [2, 2])


if __name__ == "__main__":
    unittest.main()
