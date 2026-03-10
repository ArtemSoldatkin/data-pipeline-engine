"""Transformation derive module for dataframe transformation operations.

Provides pipeline functionality and includes: derive_columns.

Usage example:
.. code-block:: python

    from data_pipeline_engine.transformation.derive import derive_columns

    derive_columns(...)
"""

from __future__ import annotations

import pandas as pd

from data_pipeline_engine.expressions import evaluate_derive
from data_pipeline_engine.models.rules import DeriveRuleConfig


def derive_columns(data: pd.DataFrame, derive_rules: list[DeriveRuleConfig]) -> pd.DataFrame:
    """Derive columns.
    
    Args:
        data: Dataset to process.
        derive_rules: Derived-column rules to evaluate for each row.
    
    Returns:
        Dataset after applying the configured transformation logic.
    """
    if not derive_rules:
        return data

    rows = data.to_dict("records")
    for row in rows:
        for rule in derive_rules:
            row[rule.column] = evaluate_derive(rule.expression, row)
    return pd.DataFrame(rows) if rows else data
