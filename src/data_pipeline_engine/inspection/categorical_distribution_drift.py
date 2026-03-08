from __future__ import annotations

from typing import Any

import polars as pl

from data_pipeline_engine.models.rules import InspectionCategoricalDistributionDriftConfig


def evaluate_categorical_distribution_drift(
    data: pl.DataFrame, config: InspectionCategoricalDistributionDriftConfig
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    for column, thresholds in config.columns.items():
        if column not in data.columns:
            result[column] = {
                "present": False,
                "warn_above": thresholds.warn_above,
                "fail_above": thresholds.fail_above,
                "comparison_status": "placeholder",
            }
            continue

        counts_df = (
            data[column]
            .drop_nulls()
            .value_counts(sort=False)
            .rename({"count": "count"})
            .with_columns((pl.col("count") / pl.sum("count")).alias("fraction"))
        )
        result[column] = {
            "present": True,
            "warn_above": thresholds.warn_above,
            "fail_above": thresholds.fail_above,
            "current_distribution": counts_df.to_dicts(),
            "distance_placeholder": 0.0,
            "comparison_status": "placeholder",
        }

    return result
