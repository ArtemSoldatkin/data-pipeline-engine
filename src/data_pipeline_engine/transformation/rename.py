from __future__ import annotations

import pandas as pd


def rename_columns(data: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    applicable = {src: dst for src, dst in mapping.items() if src in data.columns}
    return data.rename(columns=applicable) if applicable else data
