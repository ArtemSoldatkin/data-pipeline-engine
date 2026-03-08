from __future__ import annotations

import polars as pl

from data_pipeline_engine.expressions import evaluate_derive
from data_pipeline_engine.models.rules import DeriveRuleConfig


def derive_columns(data: pl.DataFrame, derive_rules: list[DeriveRuleConfig]) -> pl.DataFrame:
    if not derive_rules:
        return data

    rows = data.to_dicts()
    for row in rows:
        for rule in derive_rules:
            row[rule.column] = evaluate_derive(rule.expression, row)
    return pl.DataFrame(rows) if rows else data
