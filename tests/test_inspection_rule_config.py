from __future__ import annotations

import unittest
from pathlib import Path

from data_pipeline_engine.config_loader import load_pipeline_configs


class InspectionRuleConfigTests(unittest.TestCase):
    def test_loads_inspection_yaml_shape(self) -> None:
        inspection_path = (
            Path(__file__).resolve().parents[1] / "configs" / "examples" / "inspection.yaml"
        )

        configs = load_pipeline_configs(inspection_config_path=inspection_path)
        inspection = configs.inspection

        self.assertIsNotNone(inspection)
        assert inspection is not None

        self.assertEqual(inspection.baseline.source, "previous_run")
        self.assertEqual(inspection.row_count.change_pct.warn_above, 20)
        self.assertEqual(inspection.row_count.change_pct.fail_above, 50)
        self.assertEqual(inspection.null_fraction.columns["score"].warn_change_pct, 10)
        self.assertEqual(inspection.null_fraction.columns["name"].warn_change_pct, 5)
        self.assertEqual(inspection.distinct_count.columns["id"].fail_change_pct, 10)
        self.assertEqual(
            inspection.numeric_distribution_drift.columns["score"].method, "psi"
        )
        self.assertEqual(
            inspection.categorical_distribution_drift.columns["status"].warn_above, 0.15
        )


if __name__ == "__main__":
    unittest.main()
