"""Transformation filter module for dataframe transformation operations.

Provides pipeline functionality and includes: filter_rows.

Usage example:
.. code-block:: python

    from data_pipeline_engine.transformation.filter import filter_rows

    filter_rows(...)"""

from __future__ import annotations

import pandas as pd

from data_pipeline_engine.expressions import predicate_to_expr
from data_pipeline_engine.models.rules import RowFilterRuleConfig
from data_pipeline_engine.transformation.errors import StageExecutionError


def filter_rows(data: pd.DataFrame, filters: list[RowFilterRuleConfig]) -> pd.DataFrame:
    """Filter rows.
    
    Args:
        data: Dataset to process.
        filters: Row filter rules to combine and apply.
    
    Returns:
        Dataset after applying configured row filters.
    
    Raises:
        ValueError: If provided arguments are invalid.
    """
    if not filters:
        return data

    combined_mask: pd.Series | None = None
    for rule in filters:
        try:
            predicate = predicate_to_expr(data, rule.expression).fillna(False)
        except ValueError as exc:
            raise StageExecutionError(str(exc)) from exc
        combined_mask = predicate if combined_mask is None else (combined_mask & predicate)

    return data[combined_mask] if combined_mask is not None else data
