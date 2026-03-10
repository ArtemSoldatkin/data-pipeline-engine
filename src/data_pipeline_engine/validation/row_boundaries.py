"""Validation row boundaries module for dataframe validation checks.

Provides pipeline functionality and includes: check_row_boundaries.

Usage example:
.. code-block:: python

    from data_pipeline_engine.validation.row_boundaries import check_row_boundaries

    check_row_boundaries(...)
"""

from __future__ import annotations

import pandas as pd


def check_row_boundaries(
    data: pd.DataFrame, min_rows: int | None, max_rows: int | None
) -> list[str]:
    """Check row boundaries.
    
    Args:
        data: Dataset to process.
        min_rows: Minimum allowed row count.
        max_rows: Maximum allowed row count.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    errors: list[str] = []
    rows = len(data)
    if min_rows is not None and rows < min_rows:
        errors.append(f"Row count {rows} is below min_rows {min_rows}")
    if max_rows is not None and rows > max_rows:
        errors.append(f"Row count {rows} exceeds max_rows {max_rows}")
    return errors
