"""Inspection orchestration module for computing inspection metrics.

Provides pipeline functionality and includes: _collect_statuses, _overall_status, inspection.

Usage example:
.. code-block:: python

    from data_pipeline_engine.inspection.inspection import inspection

    inspection(...)"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from data_pipeline_engine.inspection.baseline import evaluate_baseline, load_baseline_frames
from data_pipeline_engine.inspection.categorical_distribution_drift import (
    evaluate_categorical_distribution_drift,
)
from data_pipeline_engine.inspection.distinct_count import evaluate_distinct_count
from data_pipeline_engine.inspection.null_fraction import evaluate_null_fraction
from data_pipeline_engine.inspection.numeric_distribution_drift import (
    evaluate_numeric_distribution_drift,
)
from data_pipeline_engine.inspection.row_count import evaluate_row_count
from data_pipeline_engine.inspection.types import (
    InspectionMetrics,
    InspectionStatusOnlyMetrics,
)
from data_pipeline_engine.models.rules import InspectionRuleConfig


def _collect_statuses(value: Any) -> list[str]:
    """Collect statuses.
    
    Args:
        value: Computed metric value to classify.
    
    Returns:
        Flattened list of nested comparison statuses extracted from metric payloads.
    """
    if isinstance(value, dict):
        statuses: list[str] = []
        if "comparison_status" in value and isinstance(value["comparison_status"], str):
            statuses.append(value["comparison_status"])
        for nested in value.values():
            statuses.extend(_collect_statuses(nested))
        return statuses
    if isinstance(value, list):
        statuses: list[str] = []
        for item in value:
            statuses.extend(_collect_statuses(item))
        return statuses
    return []


def _overall_status(metrics: dict[str, Any]) -> str:
    """Overall status.
    
    Args:
        metrics: Metrics value used by this operation.
    
    Returns:
        Aggregate inspection status derived from nested metric statuses.
    """
    statuses = _collect_statuses(metrics)
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    if "pass" in statuses:
        return "pass"
    if "missing_column" in statuses:
        return "missing_column"
    return "no_baseline"


def inspection(
    data: pd.DataFrame,
    config: InspectionRuleConfig | None,
    source_csv: str | Path | None = None,
    baseline_csv: str | Path | None = None,
    return_metrics: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, InspectionMetrics | InspectionStatusOnlyMetrics]:
    """Inspection.
    
    Args:
        data: Dataset to process.
        config: Stage configuration object controlling the operation.
        source_csv: Source CSV path used to resolve cache or baseline data.
        baseline_csv: Path to baseline CSV data for inspection comparisons.
        return_metrics: Return metrics value used by this operation.
    
    Returns:
        Original dataset, optionally paired with inspection metrics when requested.
    """
    if config is None:
        if return_metrics:
            return data, {"overall_status": "not_configured"}
        return data

    baseline_frames = load_baseline_frames(
        config.baseline, source_csv=source_csv, baseline_csv=baseline_csv
    )
    baseline_metrics = evaluate_baseline(
        config.baseline,
        source_csv=source_csv,
        baseline_csv=baseline_csv,
    )
    row_count_metrics = evaluate_row_count(data, config.row_count, baseline_frames=baseline_frames)
    null_fraction_metrics = evaluate_null_fraction(
        data, config.null_fraction, baseline_frames=baseline_frames
    )
    distinct_count_metrics = evaluate_distinct_count(
        data, config.distinct_count, baseline_frames=baseline_frames
    )
    numeric_distribution_metrics = evaluate_numeric_distribution_drift(
        data, config.numeric_distribution_drift, baseline_frames=baseline_frames
    )
    categorical_distribution_metrics = evaluate_categorical_distribution_drift(
        data, config.categorical_distribution_drift, baseline_frames=baseline_frames
    )
    overall_status = _overall_status(
        {
            "baseline": baseline_metrics,
            "row_count": row_count_metrics,
            "null_fraction": null_fraction_metrics,
            "distinct_count": distinct_count_metrics,
            "numeric_distribution_drift": numeric_distribution_metrics,
            "categorical_distribution_drift": categorical_distribution_metrics,
        }
    )
    metrics: InspectionMetrics = {
        "baseline": baseline_metrics,
        "row_count": row_count_metrics,
        "null_fraction": null_fraction_metrics,
        "distinct_count": distinct_count_metrics,
        "numeric_distribution_drift": numeric_distribution_metrics,
        "categorical_distribution_drift": categorical_distribution_metrics,
        "overall_status": overall_status,
    }
    if return_metrics:
        return data, metrics
    return data
