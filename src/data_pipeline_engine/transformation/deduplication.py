from __future__ import annotations

import polars as pl

from data_pipeline_engine.models.rules import RowDeduplicateRuleConfig


def deduplicate_rows(
    data: pl.DataFrame, deduplication: RowDeduplicateRuleConfig | None
) -> pl.DataFrame:
    if deduplication is None or not deduplication.keys:
        return data

    subset = [column for column in deduplication.keys if column in data.columns]
    if not subset:
        return data

    keep = "first" if deduplication.strategy == "keep_first" else "last"
    return data.unique(subset=subset, keep=keep, maintain_order=True)
