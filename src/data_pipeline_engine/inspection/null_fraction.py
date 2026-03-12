"""Null fraction inspection module for column-level baseline comparisons.

Provides pipeline functionality and includes: evaluate_null_fraction.

Usage example:
.. code-block:: python

    from data_pipeline_engine.inspection.null_fraction import evaluate_null_fraction

    evaluate_null_fraction(...)"""

from __future__ import annotations

import pandas as pd

from data_pipeline_engine.inspection.comparison_utils import (
    absolute_delta_pct,
    mean_or_none,
    status_from_thresholds,
)
from data_pipeline_engine.models.rules import InspectionColumnThresholdsConfig
from data_pipeline_engine.inspection.types import (
    MissingColumnNullFractionMetric,
    NullFractionColumnMetric,
    NullFractionMetric,
)


def evaluate_null_fraction(
    data: pd.DataFrame,
    config: InspectionColumnThresholdsConfig,
    baseline_frames: list[pd.DataFrame] | None = None,
) -> dict[str, NullFractionColumnMetric]:
    """Evaluate null fraction.
    
    Args:
        data: Dataset to process.
        config: Stage configuration object controlling the operation.
        baseline_frames: Baseline data frames used for metric comparisons.
    
    Returns:
        Dictionary containing computed results for this operation.
    """
    result: dict[str, NullFractionColumnMetric] = {}
    denominator = max(len(data), 1)

    for column, thresholds in config.columns.items():
        warn_change_pct = (
            float(thresholds.warn_change_pct) if thresholds.warn_change_pct is not None else None
        )
        fail_change_pct = (
            float(thresholds.fail_change_pct) if thresholds.fail_change_pct is not None else None
        )

        if column not in data.columns:
            missing_metric: MissingColumnNullFractionMetric = {
                "present": False,
                "warn_change_pct": warn_change_pct,
                "fail_change_pct": fail_change_pct,
                "comparison_status": "missing_column",
            }
            result[column] = missing_metric
            continue

        current_fraction = float(data[column].isna().sum()) / denominator
        baseline_fractions: list[float] = []
        for frame in baseline_frames or []:
            if column not in frame.columns:
                continue
            frame_denominator = max(len(frame), 1)
            baseline_fractions.append(float(frame[column].isna().sum()) / frame_denominator)

        baseline_fraction = mean_or_none(baseline_fractions)
        change_pct = absolute_delta_pct(current_fraction, baseline_fraction)
        metric: NullFractionMetric = {
            "present": True,
            "current_null_fraction": current_fraction,
            "baseline_null_fraction": baseline_fraction,
            "warn_change_pct": warn_change_pct,
            "fail_change_pct": fail_change_pct,
            "change_pct": change_pct,
            "comparison_status": status_from_thresholds(
                change_pct, warn_change_pct, fail_change_pct
            ),
        }
        result[column] = metric

    return result
