from __future__ import annotations

import polars as pl

from data_pipeline_engine.models.rules import InspectionRowCountConfig


def evaluate_row_count(
    data: pl.DataFrame, config: InspectionRowCountConfig
) -> dict[str, int | float | None]:
    current_rows = data.height
    return {
        "current_rows": current_rows,
        "warn_above": config.change_pct.warn_above,
        "fail_above": config.change_pct.fail_above,
        "change_pct_placeholder": 0.0,
        "comparison_status": "placeholder",
    }
