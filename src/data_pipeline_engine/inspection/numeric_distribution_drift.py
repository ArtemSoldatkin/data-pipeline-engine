from __future__ import annotations

import math
from typing import Any

import pandas as pd

from data_pipeline_engine.inspection.comparison_utils import status_from_thresholds
from data_pipeline_engine.models.rules import InspectionNumericDistributionDriftConfig


def _to_numeric_values(data: pd.DataFrame, column: str) -> list[float]:
    if column not in data.columns:
        return []
    series = pd.to_numeric(data[column], errors="coerce").dropna()
    return [float(value) for value in series.tolist()]


def _uniform_edges(values: list[float], bins: int = 10) -> list[float]:
    min_value = min(values)
    max_value = max(values)
    if min_value == max_value:
        return [min_value, min_value + 1.0]
    width = (max_value - min_value) / bins
    return [min_value + width * idx for idx in range(bins + 1)]


def _histogram_proportions(values: list[float], edges: list[float]) -> list[float]:
    counts = [0.0] * (len(edges) - 1)
    if not values:
        return counts
    for value in values:
        if value <= edges[0]:
            counts[0] += 1
            continue
        if value >= edges[-1]:
            counts[-1] += 1
            continue
        for idx in range(len(edges) - 1):
            left = edges[idx]
            right = edges[idx + 1]
            if left <= value < right:
                counts[idx] += 1
                break
    total = sum(counts)
    return [count / total if total else 0.0 for count in counts]


def _psi_distance(current: list[float], baseline: list[float]) -> float:
    edges = _uniform_edges(current + baseline)
    current_p = _histogram_proportions(current, edges)
    baseline_p = _histogram_proportions(baseline, edges)
    epsilon = 1e-8
    return sum(
        (cur - base) * math.log((cur + epsilon) / (base + epsilon))
        for cur, base in zip(current_p, baseline_p)
    )


def _js_divergence(current: list[float], baseline: list[float]) -> float:
    edges = _uniform_edges(current + baseline)
    current_p = _histogram_proportions(current, edges)
    baseline_p = _histogram_proportions(baseline, edges)
    epsilon = 1e-8
    midpoint = [(cur + base) / 2.0 for cur, base in zip(current_p, baseline_p)]
    kl_current = sum(
        cur * math.log((cur + epsilon) / (mid + epsilon))
        for cur, mid in zip(current_p, midpoint)
        if cur > 0
    )
    kl_baseline = sum(
        base * math.log((base + epsilon) / (mid + epsilon))
        for base, mid in zip(baseline_p, midpoint)
        if base > 0
    )
    return (kl_current + kl_baseline) / 2.0


def _ks_distance(current: list[float], baseline: list[float]) -> float:
    current_sorted = sorted(current)
    baseline_sorted = sorted(baseline)
    all_values = sorted(set(current_sorted + baseline_sorted))
    if not all_values:
        return 0.0

    current_index = 0
    baseline_index = 0
    max_distance = 0.0
    for value in all_values:
        while current_index < len(current_sorted) and current_sorted[current_index] <= value:
            current_index += 1
        while baseline_index < len(baseline_sorted) and baseline_sorted[baseline_index] <= value:
            baseline_index += 1
        current_cdf = current_index / len(current_sorted)
        baseline_cdf = baseline_index / len(baseline_sorted)
        max_distance = max(max_distance, abs(current_cdf - baseline_cdf))

    return max_distance


def _distribution_distance(method: str, current: list[float], baseline: list[float]) -> float:
    if method == "ks":
        return _ks_distance(current, baseline)
    if method == "js_divergence":
        return _js_divergence(current, baseline)
    return _psi_distance(current, baseline)


def evaluate_numeric_distribution_drift(
    data: pd.DataFrame,
    config: InspectionNumericDistributionDriftConfig,
    baseline_frames: list[pd.DataFrame] | None = None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    for column, thresholds in config.columns.items():
        if column not in data.columns:
            result[column] = {
                "present": False,
                "method": thresholds.method,
                "warn_above": thresholds.warn_above,
                "fail_above": thresholds.fail_above,
                "comparison_status": "missing_column",
            }
            continue

        current_values = _to_numeric_values(data, column)
        current_series = pd.Series(current_values, dtype="float64")
        current_stats = {
            "count": len(current_values),
            "mean": float(current_series.mean()) if not current_series.empty else None,
            "std": float(current_series.std()) if len(current_values) > 1 else None,
            "min": float(current_series.min()) if not current_series.empty else None,
            "max": float(current_series.max()) if not current_series.empty else None,
        }
        baseline_values: list[float] = []
        for frame in baseline_frames or []:
            baseline_values.extend(_to_numeric_values(frame, column))

        distance = None
        if current_values and baseline_values:
            distance = _distribution_distance(thresholds.method, current_values, baseline_values)

        result[column] = {
            "present": True,
            "method": thresholds.method,
            "warn_above": thresholds.warn_above,
            "fail_above": thresholds.fail_above,
            "current_stats": current_stats,
            "baseline_count": len(baseline_values),
            "distance": distance,
            "comparison_status": status_from_thresholds(
                distance, thresholds.warn_above, thresholds.fail_above
            ),
        }

    return result
