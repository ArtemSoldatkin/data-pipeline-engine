from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

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
from data_pipeline_engine.models.rules import InspectionRuleConfig


def _collect_statuses(value: Any) -> list[str]:
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
    data: pl.DataFrame,
    config: InspectionRuleConfig | None,
    source_csv: str | Path | None = None,
    baseline_csv: str | Path | None = None,
    return_metrics: bool = False,
) -> pl.DataFrame | tuple[pl.DataFrame, dict[str, Any]]:
    """Run inspection calculations and optionally return computed inspection metrics."""
    if config is None:
        if return_metrics:
            return data, {"overall_status": "not_configured"}
        return data

    baseline_frames = load_baseline_frames(
        config.baseline, source_csv=source_csv, baseline_csv=baseline_csv
    )
    metrics: dict[str, object] = {
        "baseline": evaluate_baseline(
            config.baseline,
            source_csv=source_csv,
            baseline_csv=baseline_csv,
        ),
        "row_count": evaluate_row_count(data, config.row_count, baseline_frames=baseline_frames),
        "null_fraction": evaluate_null_fraction(
            data, config.null_fraction, baseline_frames=baseline_frames
        ),
        "distinct_count": evaluate_distinct_count(
            data, config.distinct_count, baseline_frames=baseline_frames
        ),
        "numeric_distribution_drift": evaluate_numeric_distribution_drift(
            data, config.numeric_distribution_drift, baseline_frames=baseline_frames
        ),
        "categorical_distribution_drift": evaluate_categorical_distribution_drift(
            data, config.categorical_distribution_drift, baseline_frames=baseline_frames
        ),
    }
    metrics["overall_status"] = _overall_status(metrics)
    if return_metrics:
        return data, metrics
    return data
