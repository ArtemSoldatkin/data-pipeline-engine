from __future__ import annotations

import polars as pl

from data_pipeline_engine.models.rules import InspectionRuleConfig


def inspection(data: pl.DataFrame, config: InspectionRuleConfig | None) -> pl.DataFrame:
    """Placeholder inspection stage."""
    return data
