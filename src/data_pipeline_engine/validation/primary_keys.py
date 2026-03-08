from __future__ import annotations

import polars as pl


def check_primary_keys(data: pl.DataFrame, primary_key: list[str]) -> list[str]:
    if not primary_key:
        return []

    errors: list[str] = []
    missing = [col for col in primary_key if col not in data.columns]
    if missing:
        errors.append(f"Primary key columns are missing: {missing}")
        return errors

    null_flags = data.select([pl.col(col).is_null().any().alias(col) for col in primary_key]).row(0)
    null_columns = [col for col, has_null in zip(primary_key, null_flags) if has_null]
    if null_columns:
        errors.append(f"Primary key contains nulls in columns: {null_columns}")

    if data.height != data.unique(subset=primary_key).height:
        errors.append(f"Primary key is not unique: {primary_key}")

    return errors
