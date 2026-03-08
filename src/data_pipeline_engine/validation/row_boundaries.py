from __future__ import annotations

import polars as pl


def check_row_boundaries(
    data: pl.DataFrame, min_rows: int | None, max_rows: int | None
) -> list[str]:
    errors: list[str] = []
    rows = data.height
    if min_rows is not None and rows < min_rows:
        errors.append(f"Row count {rows} is below min_rows {min_rows}")
    if max_rows is not None and rows > max_rows:
        errors.append(f"Row count {rows} exceeds max_rows {max_rows}")
    return errors
