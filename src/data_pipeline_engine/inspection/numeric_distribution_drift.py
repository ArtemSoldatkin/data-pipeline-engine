from __future__ import annotations

from typing import Any

import polars as pl

from data_pipeline_engine.models.rules import InspectionNumericDistributionDriftConfig


def evaluate_numeric_distribution_drift(
    data: pl.DataFrame, config: InspectionNumericDistributionDriftConfig
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    for column, thresholds in config.columns.items():
        if column not in data.columns:
            result[column] = {
                "present": False,
                "method": thresholds.method,
                "warn_above": thresholds.warn_above,
                "fail_above": thresholds.fail_above,
                "comparison_status": "placeholder",
            }
            continue

        series = data[column].cast(pl.Float64, strict=False).drop_nulls()
        current_stats = {
            "count": series.len(),
            "mean": float(series.mean()) if series.len() > 0 else None,
            "std": float(series.std()) if series.len() > 1 else None,
            "min": float(series.min()) if series.len() > 0 else None,
            "max": float(series.max()) if series.len() > 0 else None,
        }
        result[column] = {
            "present": True,
            "method": thresholds.method,
            "warn_above": thresholds.warn_above,
            "fail_above": thresholds.fail_above,
            "current_stats": current_stats,
            "distance_placeholder": 0.0,
            "comparison_status": "placeholder",
        }

    return result
