"""Categorical drift module for inspection distribution comparisons.

Provides pipeline functionality and includes: _distribution, _total_variation_distance, evaluate_categorical_distribution_drift.

Usage example:
.. code-block:: python

    from data_pipeline_engine.inspection.categorical_distribution_drift import evaluate_categorical_distribution_drift

    evaluate_categorical_distribution_drift(...)"""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_pipeline_engine.inspection.comparison_utils import status_from_thresholds
from data_pipeline_engine.models.rules import InspectionCategoricalDistributionDriftConfig


def _distribution(data: pd.DataFrame, column: str) -> dict[str, float]:
    """Distribution.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
    
    Returns:
        Dictionary containing computed results for this operation.
    """
    counts = data[column].dropna().astype("string").value_counts(normalize=True)
    return {str(key): float(value) for key, value in counts.items()}


def _total_variation_distance(
    current_distribution: dict[str, float], baseline_distribution: dict[str, float]
) -> float:
    """Total variation distance.
    
    Args:
        current_distribution: Current categorical distribution.
        baseline_distribution: Baseline categorical distribution.
    
    Returns:
        Computed result of this operation.
    """
    keys = set(current_distribution) | set(baseline_distribution)
    return 0.5 * sum(
        abs(current_distribution.get(key, 0.0) - baseline_distribution.get(key, 0.0))
        for key in keys
    )


def evaluate_categorical_distribution_drift(
    data: pd.DataFrame,
    config: InspectionCategoricalDistributionDriftConfig,
    baseline_frames: list[pd.DataFrame] | None = None,
) -> dict[str, dict[str, Any]]:
    """Evaluate categorical distribution drift.
    
    Args:
        data: Dataset to process.
        config: Stage configuration object controlling the operation.
        baseline_frames: Baseline data frames used for metric comparisons.
    
    Returns:
        Dictionary containing computed results for this operation.
    """
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
