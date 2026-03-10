"""Transformation rename module for dataframe transformation operations.

Provides pipeline functionality and includes: rename_columns.

Usage example:
.. code-block:: python

    from data_pipeline_engine.transformation.rename import rename_columns

    rename_columns(...)"""

from __future__ import annotations

import pandas as pd


def rename_columns(data: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Rename columns.
    
    Args:
        data: Dataset to process.
        mapping: Source-to-target column rename mapping.
    
    Returns:
        Dataset after applying the configured transformation logic.
    """
    applicable = {src: dst for src, dst in mapping.items() if src in data.columns}
    return data.rename(columns=applicable) if applicable else data
