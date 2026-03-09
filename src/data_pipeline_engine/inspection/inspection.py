from __future__ import annotations

from pathlib import Path

import polars as pl

from data_pipeline_engine.inspection.baseline import evaluate_baseline_placeholder
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


def _compare_to_baseline_placeholder(metrics: dict[str, object]) -> None:
    _ = metrics


def inspection(
    data: pl.DataFrame,
    config: InspectionRuleConfig | None,
    source_csv: str | Path | None = None,
) -> pl.DataFrame:
    """Run inspection calculations; baseline comparison is currently a placeholder."""
    if config is None:
        return data

    metrics: dict[str, object] = {
        "baseline": evaluate_baseline_placeholder(config.baseline, source_csv=source_csv),
        "row_count": evaluate_row_count(data, config.row_count),
        "null_fraction": evaluate_null_fraction(data, config.null_fraction),
        "distinct_count": evaluate_distinct_count(data, config.distinct_count),
        "numeric_distribution_drift": evaluate_numeric_distribution_drift(
            data, config.numeric_distribution_drift
        ),
        "categorical_distribution_drift": evaluate_categorical_distribution_drift(
            data, config.categorical_distribution_drift
        ),
    }
    _compare_to_baseline_placeholder(metrics)
    return data
