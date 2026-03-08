from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from data_pipeline_engine.config_loader import load_pipeline_configs
from data_pipeline_engine.models.rules import ColumnType, TableSchemaColumn


class PipelineExecutionError(Exception):
    """Raised when pipeline validations fail."""


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


def _validate_row_count(df: pl.DataFrame, allow_empty: bool, min_rows: int | None, max_rows: int | None) -> list[str]:
    errors: list[str] = []
    rows = df.height

    if not allow_empty and rows == 0:
        errors.append("DataFrame is empty, but allow_empty is false")
    if min_rows is not None and rows < min_rows:
        errors.append(f"Row count {rows} is below min_rows {min_rows}")
    if max_rows is not None and rows > max_rows:
        errors.append(f"Row count {rows} exceeds max_rows {max_rows}")

    return errors


def _validate_schema(df: pl.DataFrame, schema: list[TableSchemaColumn], allow_extra_columns: bool) -> list[str]:
    errors: list[str] = []
    df_columns = set(df.columns)
    expected_columns = {col.name for col in schema}

    for expected_col in schema:
        if expected_col.name not in df.columns:
            if expected_col.required:
                errors.append(f"Missing required column: {expected_col.name}")
            continue

        series = df[expected_col.name]
        actual_type = _map_polars_type(series.dtype)
        if actual_type != expected_col.type:
            errors.append(
                f"Column '{expected_col.name}' has type '{actual_type.value}', expected '{expected_col.type.value}'"
            )

        if not expected_col.nullable and series.null_count() > 0:
            errors.append(f"Column '{expected_col.name}' contains nulls, but nullable is false")

    if not allow_extra_columns:
        extra_columns = df_columns - expected_columns
        if extra_columns:
            errors.append(f"Extra columns found but allow_extra_columns is false: {sorted(extra_columns)}")

    return errors


def run_pipeline(
    csv_path: str | Path,
    validation_config_path: str | Path | None = None,
    cleaning_config_path: str | Path | None = None,
    skew_config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Run pipeline over a CSV with optional YAML configs (at least one required)."""
    configs = load_pipeline_configs(
        validation_config_path=validation_config_path,
        cleaning_config_path=cleaning_config_path,
        skew_config_path=skew_config_path,
    )

    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file does not exist: {csv_file}")

    df = pl.read_csv(csv_file)

    errors: list[str] = []

    if configs.validation is not None:
        errors.extend(
            _validate_row_count(
                df=df,
                allow_empty=configs.validation.allow_empty,
                min_rows=configs.validation.min_rows,
                max_rows=configs.validation.max_rows,
            )
        )
        errors.extend(
            _validate_schema(
                df=df,
                schema=configs.validation.schema,
                allow_extra_columns=configs.validation.allow_extra_columns,
            )
        )

    if errors:
        raise PipelineExecutionError("Validation failed:\n- " + "\n- ".join(errors))

    return {
        "status": "success",
        "rows": df.height,
        "columns": df.columns,
        "validation_applied": configs.validation is not None,
        "cleaning_applied": configs.cleaning is not None,
        "skew_applied": configs.skew is not None,
    }
