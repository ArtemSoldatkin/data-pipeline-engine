from __future__ import annotations

import polars as pl


def check_allow_empty(data: pl.DataFrame, allow_empty: bool) -> list[str]:
    if allow_empty or data.height > 0:
        return []
    return ["DataFrame is empty, but allow_empty is false"]
