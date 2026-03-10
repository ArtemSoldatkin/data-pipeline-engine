"""Transformation deduplication module for dataframe transformation operations.

Provides pipeline functionality and includes: deduplicate_rows.

Usage example:
.. code-block:: python

    from data_pipeline_engine.transformation.deduplication import deduplicate_rows

    deduplicate_rows(...)"""

from __future__ import annotations

import pandas as pd

from data_pipeline_engine.models.rules import RowDeduplicateRuleConfig


def deduplicate_rows(
    data: pd.DataFrame, deduplication: RowDeduplicateRuleConfig | None
) -> pd.DataFrame:
    """Deduplicate rows.
    
    Args:
        data: Dataset to process.
        deduplication: Deduplication settings including key columns and keep strategy.
    
    Returns:
        Dataset after applying the configured transformation logic.
    """
    if deduplication is None or not deduplication.keys:
        return data

    subset = [column for column in deduplication.keys if column in data.columns]
    if not subset:
        return data

    keep = "first" if deduplication.strategy == "keep_first" else "last"
    return data.drop_duplicates(subset=subset, keep=keep)
