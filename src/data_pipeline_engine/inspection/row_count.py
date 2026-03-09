from __future__ import annotations

from typing import Any

import pandas as pd

from data_pipeline_engine.inspection.comparison_utils import (
    mean_or_none,
    relative_change_pct,
    status_from_thresholds,
)
from data_pipeline_engine.models.rules import InspectionRowCountConfig


def evaluate_row_count(
    data: pd.DataFrame,
    config: InspectionRowCountConfig,
    baseline_frames: list[pd.DataFrame] | None = None,
) -> dict[str, Any]:
    current_rows = len(data)
    baseline_values = [float(len(frame)) for frame in (baseline_frames or [])]
    baseline_rows = mean_or_none(baseline_values)
    change_pct = relative_change_pct(float(current_rows), baseline_rows)

    return {
        "current_rows": current_rows,
        "baseline_rows": baseline_rows,
        "warn_above": config.change_pct.warn_above,
        "fail_above": config.change_pct.fail_above,
        "change_pct": change_pct,
        "comparison_status": status_from_thresholds(
            change_pct, config.change_pct.warn_above, config.change_pct.fail_above
        ),
    }
