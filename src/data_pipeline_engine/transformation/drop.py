"""Transformation drop module for dataframe transformation operations.

Provides pipeline functionality and includes: drop_columns.

Usage example:
.. code-block:: python

    from data_pipeline_engine.transformation.drop import drop_columns

    drop_columns(...)
"""

from __future__ import annotations

import pandas as pd


def drop_columns(data: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Drop columns.
    
    Args:
        data: Dataset to process.
        columns: List of column names to operate on.
    
    Returns:
        Dataset after applying the configured transformation logic.
    """
    existing = [column for column in columns if column in data.columns]
    return data.drop(columns=existing) if existing else data
