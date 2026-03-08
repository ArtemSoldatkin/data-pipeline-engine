from __future__ import annotations

from typing import Any

import polars as pl

from data_pipeline_engine.models.rules import InspectionColumnThresholdsConfig


def evaluate_distinct_count(
    data: pl.DataFrame, config: InspectionColumnThresholdsConfig
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    for column, thresholds in config.columns.items():
        if column not in data.columns:
            result[column] = {
                "present": False,
                "warn_change_pct": thresholds.warn_change_pct,
                "fail_change_pct": thresholds.fail_change_pct,
                "comparison_status": "placeholder",
            }
            continue

        current_distinct = int(data[column].n_unique())
        result[column] = {
            "present": True,
            "current_distinct_count": current_distinct,
            "warn_change_pct": thresholds.warn_change_pct,
            "fail_change_pct": thresholds.fail_change_pct,
            "change_pct_placeholder": 0.0,
            "comparison_status": "placeholder",
        }

    return result
