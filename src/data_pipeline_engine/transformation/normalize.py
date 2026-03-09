from __future__ import annotations

import pandas as pd

from data_pipeline_engine.transformation.errors import StageExecutionError


def normalize_columns(data: pd.DataFrame, ops: dict[str, list[str]]) -> pd.DataFrame:
    if not ops:
        return data

    result = data.copy()
    for column, operations in ops.items():
        if column not in result.columns:
            continue

        series = result[column].astype("string")
        for operation in operations:
            if operation == "trim":
                series = series.str.strip()
            elif operation == "lowercase":
                series = series.str.lower()
            elif operation == "uppercase":
                series = series.str.upper()
            else:
                raise StageExecutionError(f"Unsupported normalization operation: {operation}")
        result[column] = series

    return result
