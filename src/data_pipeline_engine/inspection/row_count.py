"""Row count inspection module for baseline comparison metrics.

Provides pipeline functionality and includes: evaluate_row_count.

Usage example:
.. code-block:: python

    from data_pipeline_engine.inspection.row_count import evaluate_row_count

    evaluate_row_count(...)"""

from __future__ import annotations

import pandas as pd

from data_pipeline_engine.inspection.comparison_utils import (
    mean_or_none,
    relative_change_pct,
    status_from_thresholds,
)
from data_pipeline_engine.models.rules import InspectionRowCountConfig
from data_pipeline_engine.inspection.types import RowCountMetrics


def evaluate_row_count(
    data: pd.DataFrame,
    config: InspectionRowCountConfig,
    baseline_frames: list[pd.DataFrame] | None = None,
) -> RowCountMetrics:
    """Evaluate row count.
    
    Args:
        data: Dataset to process.
        config: Stage configuration object controlling the operation.
        baseline_frames: Baseline data frames used for metric comparisons.
    
    Returns:
        Dictionary containing computed results for this operation.
    """
    current_rows = len(data)
    baseline_values = [float(len(frame)) for frame in (baseline_frames or [])]
    baseline_rows = mean_or_none(baseline_values)
    change_pct = relative_change_pct(float(current_rows), baseline_rows)
    warn_above = (
        float(config.change_pct.warn_above) if config.change_pct.warn_above is not None else None
    )
    fail_above = (
        float(config.change_pct.fail_above) if config.change_pct.fail_above is not None else None
    )

    result: RowCountMetrics = {
        "current_rows": current_rows,
        "baseline_rows": baseline_rows,
        "warn_above": warn_above,
        "fail_above": fail_above,
        "change_pct": change_pct,
        "comparison_status": status_from_thresholds(
            change_pct, warn_above, fail_above
        ),
    }
    return result
