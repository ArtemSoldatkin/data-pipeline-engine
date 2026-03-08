from __future__ import annotations

import polars as pl

from data_pipeline_engine.expressions import row_rule_to_expr
from data_pipeline_engine.models.rules import RowRuleConfig


def run_row_rules(data: pl.DataFrame, row_rules: list[RowRuleConfig]) -> list[str]:
    errors: list[str] = []

    for rule in row_rules:
        try:
            expr = row_rule_to_expr(rule.expression)
        except ValueError as exc:
            errors.append(f"Row rule '{rule.name}' has invalid expression: {exc}")
            continue

        failing_rows = data.filter(~expr.fill_null(False)).height
        if failing_rows > 0:
            errors.append(f"Row rule '{rule.name}' failed for {failing_rows} rows")

    return errors
