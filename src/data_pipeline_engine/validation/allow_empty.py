"""Validation allow empty module for dataframe validation checks.

Provides pipeline functionality and includes: check_allow_empty.

Usage example:
.. code-block:: python

    from data_pipeline_engine.validation.allow_empty import check_allow_empty

    check_allow_empty(...)
"""

from __future__ import annotations

import pandas as pd


def check_allow_empty(data: pd.DataFrame, allow_empty: bool) -> list[str]:
    """Check allow empty.
    
    Args:
        data: Dataset to process.
        allow_empty: Whether an empty dataset is allowed.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if allow_empty or len(data) > 0:
        return []
    return ["DataFrame is empty, but allow_empty is false"]
