from __future__ import annotations

import pandas as pd

from data_pipeline_engine.models.rules import TransformationCastType


_DTYPE_MAP: dict[TransformationCastType, str] = {
    TransformationCastType.INT: "Int64",
    TransformationCastType.FLOAT: "float64",
    TransformationCastType.STRING: "string",
    TransformationCastType.BOOL: "boolean",
    TransformationCastType.DATE: "datetime64[ns]",
    TransformationCastType.DATETIME: "datetime64[ns]",
    TransformationCastType.TIMESTAMP: "datetime64[ns]",
}


def cast_columns(data: pd.DataFrame, casts: dict[str, TransformationCastType]) -> pd.DataFrame:
    if not casts:
        return data

    result = data.copy()
    for column, cast_type in casts.items():
        if column not in result.columns:
            continue
        target = _DTYPE_MAP[cast_type]
        if target == "datetime64[ns]":
            result[column] = pd.to_datetime(result[column], errors="coerce")
        elif target == "float64":
            result[column] = pd.to_numeric(result[column], errors="coerce")
        elif target == "Int64":
            result[column] = pd.to_numeric(result[column], errors="coerce").astype("Int64")
        elif target == "boolean":
            result[column] = result[column].astype("boolean")
        elif target == "string":
            result[column] = result[column].astype("string")
        else:
            result[column] = result[column].astype(target)

    return result
