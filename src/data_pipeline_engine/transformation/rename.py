from __future__ import annotations

import polars as pl


def rename_columns(data: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    applicable = {src: dst for src, dst in mapping.items() if src in data.columns}
    return data.rename(applicable) if applicable else data
