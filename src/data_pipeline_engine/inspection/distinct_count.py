from __future__ import annotations

from typing import Any

import polars as pl

from data_pipeline_engine.inspection.comparison_utils import (
    mean_or_none,
    relative_change_pct,
    status_from_thresholds,
)
from data_pipeline_engine.models.rules import InspectionColumnThresholdsConfig


def evaluate_distinct_count(
    data: pl.DataFrame,
    config: InspectionColumnThresholdsConfig,
    baseline_frames: list[pl.DataFrame] | None = None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    for column, thresholds in config.columns.items():
        if column not in data.columns:
            result[column] = {
                "present": False,
                "warn_change_pct": thresholds.warn_change_pct,
                "fail_change_pct": thresholds.fail_change_pct,
                "comparison_status": "missing_column",
            }
            continue

        current_distinct = int(data[column].n_unique())
        baseline_values: list[float] = []
        for frame in baseline_frames or []:
            if column in frame.columns:
                baseline_values.append(float(frame[column].n_unique()))
        baseline_distinct = mean_or_none(baseline_values)
        change_pct = relative_change_pct(float(current_distinct), baseline_distinct)

        result[column] = {
            "present": True,
            "current_distinct_count": current_distinct,
            "baseline_distinct_count": baseline_distinct,
            "warn_change_pct": thresholds.warn_change_pct,
            "fail_change_pct": thresholds.fail_change_pct,
            "change_pct": change_pct,
            "comparison_status": status_from_thresholds(
                change_pct, thresholds.warn_change_pct, thresholds.fail_change_pct
            ),
        }

    return result
