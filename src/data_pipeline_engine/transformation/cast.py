from __future__ import annotations

import polars as pl

from data_pipeline_engine.models.rules import TransformationCastType


_DTYPE_MAP: dict[TransformationCastType, pl.DataType] = {
    TransformationCastType.INT: pl.Int64,
    TransformationCastType.FLOAT: pl.Float64,
    TransformationCastType.STRING: pl.String,
    TransformationCastType.BOOL: pl.Boolean,
    TransformationCastType.DATE: pl.Date,
    TransformationCastType.DATETIME: pl.Datetime,
    TransformationCastType.TIMESTAMP: pl.Datetime,
}


def cast_columns(
    data: pl.DataFrame, casts: dict[str, TransformationCastType]
) -> pl.DataFrame:
    if not casts:
        return data

    expressions = [
        pl.col(column).cast(_DTYPE_MAP[cast_type], strict=False).alias(column)
        for column, cast_type in casts.items()
        if column in data.columns
    ]
    return data.with_columns(expressions) if expressions else data
