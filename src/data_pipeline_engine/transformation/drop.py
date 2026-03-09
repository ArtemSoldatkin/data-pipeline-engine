from __future__ import annotations

import pandas as pd


def drop_columns(data: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    existing = [column for column in columns if column in data.columns]
    return data.drop(columns=existing) if existing else data
