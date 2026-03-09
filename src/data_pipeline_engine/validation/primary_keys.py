from __future__ import annotations

import pandas as pd


def check_primary_keys(data: pd.DataFrame, primary_key: list[str]) -> list[str]:
    if not primary_key:
        return []

    errors: list[str] = []
    missing = [col for col in primary_key if col not in data.columns]
    if missing:
        errors.append(f"Primary key columns are missing: {missing}")
        return errors

    null_columns = [col for col in primary_key if data[col].isna().any()]
    if null_columns:
        errors.append(f"Primary key contains nulls in columns: {null_columns}")

    if data.duplicated(subset=primary_key).any():
        errors.append(f"Primary key is not unique: {primary_key}")

    return errors
