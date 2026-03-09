from __future__ import annotations

import unittest

from data_pipeline_engine.inspection.comparison_utils import (
    absolute_delta_pct,
    mean_or_none,
    relative_change_pct,
    status_from_thresholds,
)


class InspectionComparisonUtilsTests(unittest.TestCase):
    def test_mean_or_none(self) -> None:
        self.assertIsNone(mean_or_none([]))
        self.assertEqual(mean_or_none([2.0, 4.0]), 3.0)

    def test_relative_change_pct(self) -> None:
        self.assertIsNone(relative_change_pct(10.0, None))
        self.assertEqual(relative_change_pct(20.0, 10.0), 100.0)
        self.assertEqual(relative_change_pct(5.0, 0.0), 500.0)

    def test_absolute_delta_pct(self) -> None:
        self.assertIsNone(absolute_delta_pct(0.2, None))
        self.assertEqual(absolute_delta_pct(0.2, 0.1), 10.0)

    def test_status_from_thresholds(self) -> None:
        self.assertEqual(status_from_thresholds(None, 10, 20), "no_baseline")
        self.assertEqual(status_from_thresholds(5.0, 10, 20), "pass")
        self.assertEqual(status_from_thresholds(15.0, 10, 20), "warn")
        self.assertEqual(status_from_thresholds(25.0, 10, 20), "fail")


if __name__ == "__main__":
    unittest.main()
