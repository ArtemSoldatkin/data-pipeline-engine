from __future__ import annotations

import pandas as pd

from data_pipeline_engine.models.rules import RowDeduplicateRuleConfig


def deduplicate_rows(
    data: pd.DataFrame, deduplication: RowDeduplicateRuleConfig | None
) -> pd.DataFrame:
    if deduplication is None or not deduplication.keys:
        return data

    subset = [column for column in deduplication.keys if column in data.columns]
    if not subset:
        return data

    keep = "first" if deduplication.strategy == "keep_first" else "last"
    return data.drop_duplicates(subset=subset, keep=keep)
