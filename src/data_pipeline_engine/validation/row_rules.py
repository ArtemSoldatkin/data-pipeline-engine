"""Validation row rules module for dataframe validation checks.

Provides pipeline functionality and includes: run_row_rules.

Usage example:
.. code-block:: python

    from data_pipeline_engine.validation.row_rules import run_row_rules

    run_row_rules(...)
"""

from __future__ import annotations

import pandas as pd

from data_pipeline_engine.expressions import row_rule_to_expr
from data_pipeline_engine.models.rules import RowRuleConfig


def run_row_rules(data: pd.DataFrame, row_rules: list[RowRuleConfig]) -> list[str]:
    """Run row rules.
    
    Args:
        data: Dataset to process.
        row_rules: Row-level validation rules to evaluate.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    errors: list[str] = []

    for rule in row_rules:
        try:
            mask = row_rule_to_expr(data, rule.expression).fillna(False)
        except ValueError as exc:
            errors.append(f"Row rule '{rule.name}' has invalid expression: {exc}")
            continue

        failing_rows = int((~mask).sum())
        if failing_rows > 0:
            errors.append(f"Row rule '{rule.name}' failed for {failing_rows} rows")

    return errors
