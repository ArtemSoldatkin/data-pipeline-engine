from __future__ import annotations

import pandas as pd


def check_allow_empty(data: pd.DataFrame, allow_empty: bool) -> list[str]:
    if allow_empty or len(data) > 0:
        return []
    return ["DataFrame is empty, but allow_empty is false"]
