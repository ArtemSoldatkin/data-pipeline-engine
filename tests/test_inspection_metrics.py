from __future__ import annotations

import pandas as pd
import pytest

from data_pipeline_engine.inspection import (
    evaluate_categorical_distribution_drift,
    evaluate_distinct_count,
    evaluate_null_fraction,
    evaluate_numeric_distribution_drift,
    evaluate_row_count,
    inspection,
)
from data_pipeline_engine.models.rules import InspectionRuleConfig


def test_row_count_comparison() -> None:
    current = pd.DataFrame({"id": [1, 2, 3, 4]})
    baseline = [pd.DataFrame({"id": [1, 2]})]
    config = InspectionRuleConfig.model_validate(
        {"row_count": {"change_pct": {"warn_above": 20, "fail_above": 80}}}
    )

    result = evaluate_row_count(current, config.row_count, baseline_frames=baseline)
    assert result["current_rows"] == 4
    assert result["baseline_rows"] == 2.0
    assert result["comparison_status"] == "fail"


def test_row_count_defaults_without_thresholds() -> None:
    current = pd.DataFrame({"id": [1, 2, 3]})
    config = InspectionRuleConfig.model_validate({})

    result = evaluate_row_count(current, config.row_count)
    assert result["warn_above"] is None
    assert result["fail_above"] is None
    assert result["comparison_status"] == "no_baseline"


def test_null_fraction_comparison() -> None:
    current = pd.DataFrame({"score": [1.0, None, 2.0, None]})
    baseline = [pd.DataFrame({"score": [1.0, None, 2.0, 3.0]})]
    config = InspectionRuleConfig.model_validate(
        {"null_fraction": {"columns": {"score": {"warn_change_pct": 10, "fail_change_pct": 30}}}}
    )

    result = evaluate_null_fraction(current, config.null_fraction, baseline_frames=baseline)
    assert result["score"]["comparison_status"] == "warn"
    assert result["score"]["change_pct"] == pytest.approx(25.0)


def test_null_fraction_defaults_without_thresholds() -> None:
    current = pd.DataFrame({"score": [1.0, None, 2.0]})
    config = InspectionRuleConfig.model_validate({"null_fraction": {"columns": {"score": {}}}})

    result = evaluate_null_fraction(current, config.null_fraction)
    assert result["score"]["warn_change_pct"] is None
    assert result["score"]["fail_change_pct"] is None
    assert result["score"]["comparison_status"] == "no_baseline"


def test_distinct_count_comparison() -> None:
    current = pd.DataFrame({"id": [1, 2, 3, 4]})
    baseline = [pd.DataFrame({"id": [1, 1, 2, 2]})]
    config = InspectionRuleConfig.model_validate(
        {"distinct_count": {"columns": {"id": {"warn_change_pct": 20, "fail_change_pct": 50}}}}
    )

    result = evaluate_distinct_count(current, config.distinct_count, baseline_frames=baseline)
    assert result["id"]["comparison_status"] == "fail"
    assert result["id"]["change_pct"] == pytest.approx(100.0)


def test_distinct_count_defaults_without_thresholds() -> None:
    current = pd.DataFrame({"id": [1, 2, 3]})
    config = InspectionRuleConfig.model_validate({"distinct_count": {"columns": {"id": {}}}})

    result = evaluate_distinct_count(current, config.distinct_count)
    assert result["id"]["warn_change_pct"] is None
    assert result["id"]["fail_change_pct"] is None
    assert result["id"]["comparison_status"] == "no_baseline"


def test_numeric_distribution_drift_ks() -> None:
    current = pd.DataFrame({"score": [1.0, 2.0, 3.0, 4.0]})
    baseline = [pd.DataFrame({"score": [1.0, 1.0, 1.0, 1.0]})]
    config = InspectionRuleConfig.model_validate(
        {
            "numeric_distribution_drift": {
                "columns": {"score": {"method": "ks", "warn_above": 0.2, "fail_above": 0.5}}
            }
        }
    )

    result = evaluate_numeric_distribution_drift(
        current, config.numeric_distribution_drift, baseline_frames=baseline
    )
    assert result["score"]["comparison_status"] == "fail"
    assert result["score"]["distance"] is not None


def test_numeric_distribution_drift_defaults_without_thresholds() -> None:
    current = pd.DataFrame({"score": [1.0, 2.0, 3.0]})
    config = InspectionRuleConfig.model_validate(
        {"numeric_distribution_drift": {"columns": {"score": {"method": "psi"}}}}
    )

    result = evaluate_numeric_distribution_drift(current, config.numeric_distribution_drift)
    assert result["score"]["warn_above"] is None
    assert result["score"]["fail_above"] is None
    assert result["score"]["comparison_status"] == "no_baseline"


def test_categorical_distribution_drift() -> None:
    current = pd.DataFrame({"status": ["active", "active", "active", "inactive"]})
    baseline = [pd.DataFrame({"status": ["active", "inactive", "inactive", "inactive"]})]
    config = InspectionRuleConfig.model_validate(
        {
            "categorical_distribution_drift": {
                "columns": {"status": {"warn_above": 0.2, "fail_above": 0.4}}
            }
        }
    )

    result = evaluate_categorical_distribution_drift(
        current, config.categorical_distribution_drift, baseline_frames=baseline
    )
    assert result["status"]["comparison_status"] == "fail"
    assert result["status"]["distance"] is not None


def test_categorical_distribution_drift_defaults_without_thresholds() -> None:
    current = pd.DataFrame({"status": ["active", "inactive"]})
    config = InspectionRuleConfig.model_validate(
        {"categorical_distribution_drift": {"columns": {"status": {}}}}
    )

    result = evaluate_categorical_distribution_drift(
        current, config.categorical_distribution_drift
    )
    assert result["status"]["warn_above"] is None
    assert result["status"]["fail_above"] is None
    assert result["status"]["comparison_status"] == "no_baseline"


def test_inspection_returns_metrics_when_requested() -> None:
    current = pd.DataFrame({"id": [1, 2], "status": ["active", "inactive"]})
    config = InspectionRuleConfig.model_validate(
        {"categorical_distribution_drift": {"columns": {"status": {"warn_above": 0.1}}}}
    )

    data, metrics = inspection(
        current,
        config,
        return_metrics=True,
        source_csv="input.csv",
        baseline_csv="baseline.csv",
    )

    assert len(data) == 2
    assert "overall_status" in metrics
