from __future__ import annotations

import polars as pl

from data_pipeline_engine.expressions import predicate_to_expr
from data_pipeline_engine.models.rules import RowFilterRuleConfig
from data_pipeline_engine.transformation.errors import StageExecutionError


def filter_rows(data: pl.DataFrame, filters: list[RowFilterRuleConfig]) -> pl.DataFrame:
    if not filters:
        return data

    combined_expr: pl.Expr | None = None
    for rule in filters:
        try:
            predicate = predicate_to_expr(rule.expression)
        except ValueError as exc:
            raise StageExecutionError(str(exc)) from exc
        combined_expr = predicate if combined_expr is None else (combined_expr & predicate)

    return data.filter(combined_expr) if combined_expr is not None else data
