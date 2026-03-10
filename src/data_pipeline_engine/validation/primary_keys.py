"""Validation primary keys module for dataframe validation checks.

Provides pipeline functionality and includes: check_primary_keys.

Usage example:
.. code-block:: python

    from data_pipeline_engine.validation.primary_keys import check_primary_keys

    check_primary_keys(...)
"""

from __future__ import annotations

import pandas as pd


def check_primary_keys(data: pd.DataFrame, primary_key: list[str]) -> list[str]:
    """Check primary keys.
    
    Args:
        data: Dataset to process.
        primary_key: Columns that together define the primary key.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if not primary_key:
        return []

    errors: list[str] = []
    missing = [col for col in primary_key if col not in data.columns]
    if missing:
        errors.append(f"Primary key columns are missing: {missing}")
        return errors

    null_columns = [col for col in primary_key if data[col].isna().any()]
    if null_columns:
        errors.append(f"Primary key contains nulls in columns: {null_columns}")

    if data.duplicated(subset=primary_key).any():
        errors.append(f"Primary key is not unique: {primary_key}")

    return errors
