from __future__ import annotations

from typing import Any

import polars as pl

from data_pipeline_engine.inspection.comparison_utils import status_from_thresholds
from data_pipeline_engine.models.rules import InspectionCategoricalDistributionDriftConfig


def _distribution(data: pl.DataFrame, column: str) -> dict[str, float]:
    counts = (
        data[column]
        .drop_nulls()
        .cast(pl.String)
        .value_counts(sort=False)
        .to_dicts()
    )
    total = sum(float(item["count"]) for item in counts)
    if total == 0:
        return {}
    return {str(item[column]): float(item["count"]) / total for item in counts}


def _total_variation_distance(
    current_distribution: dict[str, float], baseline_distribution: dict[str, float]
) -> float:
    keys = set(current_distribution) | set(baseline_distribution)
    return 0.5 * sum(
        abs(current_distribution.get(key, 0.0) - baseline_distribution.get(key, 0.0))
        for key in keys
    )


def evaluate_categorical_distribution_drift(
    data: pl.DataFrame,
    config: InspectionCategoricalDistributionDriftConfig,
    baseline_frames: list[pl.DataFrame] | None = None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    for column, thresholds in config.columns.items():
        if column not in data.columns:
            result[column] = {
                "present": False,
                "warn_above": thresholds.warn_above,
                "fail_above": thresholds.fail_above,
                "comparison_status": "missing_column",
            }
            continue

        current_distribution = _distribution(data, column)
        baseline_distribution: dict[str, float] = {}
        baseline_counts: dict[str, float] = {}
        for frame in baseline_frames or []:
            if column not in frame.columns:
                continue
            frame_distribution = _distribution(frame, column)
            if not frame_distribution:
                continue
            for key, fraction in frame_distribution.items():
                baseline_counts[key] = baseline_counts.get(key, 0.0) + fraction

        if baseline_counts:
            normalization = sum(baseline_counts.values())
            baseline_distribution = {
                key: count / normalization for key, count in baseline_counts.items()
            }
        distance = None
        if current_distribution and baseline_distribution:
            distance = _total_variation_distance(current_distribution, baseline_distribution)

        result[column] = {
            "present": True,
            "warn_above": thresholds.warn_above,
            "fail_above": thresholds.fail_above,
            "current_distribution": current_distribution,
            "baseline_distribution": baseline_distribution,
            "distance": distance,
            "comparison_status": status_from_thresholds(
                distance, thresholds.warn_above, thresholds.fail_above
            ),
        }

    return result
