from __future__ import annotations

from data_pipeline_engine.inspection.comparison_utils import (
    absolute_delta_pct,
    mean_or_none,
    relative_change_pct,
    status_from_thresholds,
)


def test_mean_or_none() -> None:
    assert mean_or_none([]) is None
    assert mean_or_none([2.0, 4.0]) == 3.0


def test_relative_change_pct() -> None:
    assert relative_change_pct(10.0, None) is None
    assert relative_change_pct(20.0, 10.0) == 100.0
    assert relative_change_pct(5.0, 0.0) == 500.0


def test_absolute_delta_pct() -> None:
    assert absolute_delta_pct(0.2, None) is None
    assert absolute_delta_pct(0.2, 0.1) == 10.0


def test_status_from_thresholds() -> None:
    assert status_from_thresholds(None, 10, 20) == "no_baseline"
    assert status_from_thresholds(5.0, 10, 20) == "pass"
    assert status_from_thresholds(15.0, 10, 20) == "warn"
    assert status_from_thresholds(25.0, 10, 20) == "fail"
