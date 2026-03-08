from __future__ import annotations

import polars as pl

from data_pipeline_engine.models.rules import ColumnType, TableSchemaColumn


def _map_polars_type(dtype: pl.DataType) -> ColumnType:
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return ColumnType.INT
    if dtype in (pl.Float32, pl.Float64):
        return ColumnType.FLOAT
    if dtype == pl.Boolean:
        return ColumnType.BOOL
    if dtype == pl.Date:
        return ColumnType.DATE
    if dtype == pl.Datetime:
        return ColumnType.DATETIME
    return ColumnType.STRING


def verify_schema(
    data: pl.DataFrame, schema: list[TableSchemaColumn], allow_extra_columns: bool
) -> list[str]:
    errors: list[str] = []
    data_columns = set(data.columns)
    expected = {col.name for col in schema}

    for column in schema:
        if column.name not in data_columns:
            if column.required:
                errors.append(f"Missing required column: {column.name}")
            continue

        series = data[column.name]
        actual_type = _map_polars_type(series.dtype)
        if actual_type != column.type:
            errors.append(
                f"Column '{column.name}' has type '{actual_type.value}', "
                f"expected '{column.type.value}'"
            )

        if not column.nullable and series.null_count() > 0:
            errors.append(f"Column '{column.name}' contains nulls, but nullable is false")

    if not allow_extra_columns:
        extra = sorted(data_columns - expected)
        if extra:
            errors.append(f"Extra columns found but allow_extra_columns is false: {extra}")

    return errors
