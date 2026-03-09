from __future__ import annotations

from pathlib import Path

from data_pipeline_engine.config_loader import load_pipeline_configs


def test_loads_inspection_yaml_shape() -> None:
    inspection_path = Path(__file__).resolve().parents[1] / "configs" / "examples" / "inspection.yaml"

    configs = load_pipeline_configs(inspection_config_path=inspection_path)
    inspection = configs.inspection

    assert inspection is not None
    assert inspection.baseline.source == "previous_run"
    assert inspection.row_count.change_pct.warn_above == 25
    assert inspection.row_count.change_pct.fail_above == 50
    assert inspection.null_fraction.columns["score"].warn_change_pct == 10
    assert inspection.distinct_count.columns["id"].fail_change_pct == 25
    assert inspection.numeric_distribution_drift.columns["score"].method == "ks"
    assert inspection.categorical_distribution_drift.columns["status"].warn_above == 0.15
    assert inspection.categorical_distribution_drift.columns["score_bucket"].fail_above == 0.3
