from __future__ import annotations

from typing import Any

import pandas as pd

from data_pipeline_engine.inspection.comparison_utils import (
    absolute_delta_pct,
    mean_or_none,
    status_from_thresholds,
)
from data_pipeline_engine.models.rules import InspectionColumnThresholdsConfig


def evaluate_null_fraction(
    data: pd.DataFrame,
    config: InspectionColumnThresholdsConfig,
    baseline_frames: list[pd.DataFrame] | None = None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    denominator = max(len(data), 1)

    for column, thresholds in config.columns.items():
        if column not in data.columns:
            result[column] = {
                "present": False,
                "warn_change_pct": thresholds.warn_change_pct,
                "fail_change_pct": thresholds.fail_change_pct,
                "comparison_status": "missing_column",
            }
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
        result[column] = {
            "present": True,
            "current_null_fraction": current_fraction,
            "baseline_null_fraction": baseline_fraction,
            "warn_change_pct": thresholds.warn_change_pct,
            "fail_change_pct": thresholds.fail_change_pct,
            "change_pct": change_pct,
            "comparison_status": status_from_thresholds(
                change_pct, thresholds.warn_change_pct, thresholds.fail_change_pct
            ),
        }

    return result
