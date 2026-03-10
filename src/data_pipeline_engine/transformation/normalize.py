"""Transformation normalize module for dataframe transformation operations.

Provides pipeline functionality and includes: normalize_columns.

Usage example:
.. code-block:: python

    from data_pipeline_engine.transformation.normalize import normalize_columns

    normalize_columns(...)
"""

from __future__ import annotations

import pandas as pd

from data_pipeline_engine.transformation.errors import StageExecutionError


def normalize_columns(data: pd.DataFrame, ops: dict[str, list[str]]) -> pd.DataFrame:
    """Normalize columns.
    
    Args:
        data: Dataset to process.
        ops: Normalization operations grouped by column name.
    
    Returns:
        Dataset after applying the configured transformation logic.
    
    Raises:
        ValueError: If provided arguments are invalid.
    """
    if not ops:
        return data

    result = data.copy()
    for column, operations in ops.items():
        if column not in result.columns:
            continue

        series = result[column].astype("string")
        for operation in operations:
            if operation == "trim":
                series = series.str.strip()
            elif operation == "lowercase":
                series = series.str.lower()
            elif operation == "uppercase":
                series = series.str.upper()
            else:
                raise StageExecutionError(f"Unsupported normalization operation: {operation}")
        result[column] = series

    return result
