"""Inspection comparison utilities module for threshold-based status calculations.

Provides pipeline functionality and includes: mean_or_none, relative_change_pct, absolute_delta_pct, status_from_thresholds.

Usage example:
.. code-block:: python

    from data_pipeline_engine.inspection.comparison_utils import mean_or_none

    mean_or_none(...)"""

from __future__ import annotations


def mean_or_none(values: list[float]) -> float | None:
    """Mean or none.
    
    Args:
        values: Numeric values to aggregate into bins.
    
    Returns:
        Computed result of this operation.
    """
    if not values:
        return None
    return sum(values) / len(values)


def relative_change_pct(current: float, baseline: float | None) -> float | None:
    """Relative change pct.
    
    Args:
        current: Current numeric sample values.
        baseline: Baseline numeric sample values.
    
    Returns:
        Computed result of this operation.
    """
    if baseline is None:
        return None
    denominator = abs(baseline) if baseline != 0 else 1.0
    return abs(current - baseline) / denominator * 100.0


def absolute_delta_pct(current: float, baseline: float | None) -> float | None:
    """Absolute delta pct.
    
    Args:
        current: Current numeric sample values.
        baseline: Baseline numeric sample values.
    
    Returns:
        Computed result of this operation.
    """
    if baseline is None:
        return None
    return abs(current - baseline) * 100.0


def status_from_thresholds(
    value: float | None, warn_threshold: float | int | None, fail_threshold: float | int | None
) -> str:
    """Status from thresholds.
    
    Args:
        value: Computed metric value to classify.
        warn_threshold: Warning threshold for metric classification.
        fail_threshold: Failure threshold for metric classification.
    
    Returns:
        Computed status string for this operation.
    """
    if value is None:
        return "no_baseline"
    if fail_threshold is not None and value >= float(fail_threshold):
        return "fail"
    if warn_threshold is not None and value >= float(warn_threshold):
        return "warn"
    return "pass"
