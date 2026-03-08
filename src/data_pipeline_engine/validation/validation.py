from __future__ import annotations

import polars as pl

from data_pipeline_engine.models.rules import ValidationRuleConfig


def validation(data: pl.DataFrame, config: ValidationRuleConfig | None) -> pl.DataFrame:
    """Placeholder validation stage."""
    return data
