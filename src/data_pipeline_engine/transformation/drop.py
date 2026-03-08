from __future__ import annotations

import polars as pl


def drop_columns(data: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    existing = [column for column in columns if column in data.columns]
    return data.drop(existing) if existing else data
