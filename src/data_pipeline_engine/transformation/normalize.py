from __future__ import annotations

import polars as pl

from data_pipeline_engine.transformation.errors import StageExecutionError


def normalize_columns(data: pl.DataFrame, ops: dict[str, list[str]]) -> pl.DataFrame:
    if not ops:
        return data

    expressions: list[pl.Expr] = []
    for column, operations in ops.items():
        if column not in data.columns:
            continue

        expr = pl.col(column).cast(pl.String)
        for operation in operations:
            if operation == "trim":
                expr = expr.str.strip_chars()
            elif operation == "lowercase":
                expr = expr.str.to_lowercase()
            elif operation == "uppercase":
                expr = expr.str.to_uppercase()
            else:
                raise StageExecutionError(f"Unsupported normalization operation: {operation}")
        expressions.append(expr.alias(column))

    return data.with_columns(expressions) if expressions else data
