from __future__ import annotations

import pandas as pd

from data_pipeline_engine.models.rules import ColumnType, TableSchemaColumn


def _map_pandas_type(dtype: object) -> ColumnType:
    if pd.api.types.is_integer_dtype(dtype):
        return ColumnType.INT
    if pd.api.types.is_float_dtype(dtype):
        return ColumnType.FLOAT
    if pd.api.types.is_bool_dtype(dtype):
        return ColumnType.BOOL
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return ColumnType.DATETIME
    return ColumnType.STRING


def verify_schema(
    data: pd.DataFrame, schema: list[TableSchemaColumn], allow_extra_columns: bool
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
        actual_type = _map_pandas_type(series.dtype)
        if actual_type != column.type:
            errors.append(
                f"Column '{column.name}' has type '{actual_type.value}', "
                f"expected '{column.type.value}'"
            )

        if not column.nullable and bool(series.isna().any()):
            errors.append(f"Column '{column.name}' contains nulls, but nullable is false")

    if not allow_extra_columns:
        extra = sorted(data_columns - expected)
        if extra:
            errors.append(f"Extra columns found but allow_extra_columns is false: {extra}")

    return errors
