from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from data_pipeline_engine.inspection.baseline import evaluate_baseline, load_baseline_frames
from data_pipeline_engine.models.rules import InspectionBaselineConfig


def test_reference_dataset_requires_baseline_file_path() -> None:
    config = InspectionBaselineConfig(source="reference_dataset")

    with pytest.raises(
        ValueError,
        match="reference_csv must be provided when strategy is reference_dataset",
    ):
        load_baseline_frames(config)


def test_reference_dataset_uses_baseline_file_instead_of_cache(tmp_path: Path) -> None:
    config = InspectionBaselineConfig(source="reference_dataset")
    fake_frame = type("FakeFrame", (), {"height": 1})()

    baseline_csv = tmp_path / "baseline.csv"
    baseline_csv.write_text("id\n1\n", encoding="utf-8")

    with patch("data_pipeline_engine.inspection.baseline.read_from_cache") as read_cache:
        read_cache.return_value = [fake_frame]
        result = evaluate_baseline(config, baseline_csv=baseline_csv)

    read_cache.assert_called_once_with(
        source_csv=Path("."),
        strategy="reference_dataset",
        reference_csv=baseline_csv,
    )
    assert result["source"] == "reference_dataset"
    assert result["status"] == "ready"
    assert result["baseline_runs_available"] == 1
    assert result["baseline_rows"] == [1]


def test_previous_run_uses_cache() -> None:
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
    assert result["source"] == "previous_run"
    assert result["status"] == "ready"
    assert result["baseline_runs_available"] == 2
    assert result["baseline_rows"] == [2, 2]
