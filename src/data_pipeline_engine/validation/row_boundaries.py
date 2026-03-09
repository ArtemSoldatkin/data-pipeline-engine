from __future__ import annotations

import pandas as pd


def check_row_boundaries(
    data: pd.DataFrame, min_rows: int | None, max_rows: int | None
) -> list[str]:
    errors: list[str] = []
    rows = len(data)
    if min_rows is not None and rows < min_rows:
        errors.append(f"Row count {rows} is below min_rows {min_rows}")
    if max_rows is not None and rows > max_rows:
        errors.append(f"Row count {rows} exceeds max_rows {max_rows}")
    return errors
