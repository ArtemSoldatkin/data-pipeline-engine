from __future__ import annotations

import pandas as pd

from data_pipeline_engine.expressions import evaluate_derive
from data_pipeline_engine.models.rules import DeriveRuleConfig


def derive_columns(data: pd.DataFrame, derive_rules: list[DeriveRuleConfig]) -> pd.DataFrame:
    if not derive_rules:
        return data

    rows = data.to_dict("records")
    for row in rows:
        for rule in derive_rules:
            row[rule.column] = evaluate_derive(rule.expression, row)
    return pd.DataFrame(rows) if rows else data
