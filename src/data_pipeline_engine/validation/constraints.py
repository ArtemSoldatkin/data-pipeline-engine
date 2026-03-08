from __future__ import annotations

from typing import Any, Callable

import polars as pl


ConstraintChecker = Callable[[pl.DataFrame, str, Any], list[str]]


def _check_unique(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is True and data.height != data.unique(subset=[column]).height:
        return [f"Column '{column}' violates unique constraint"]
    return []


def _check_pattern(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is None:
        return []
    non_null = data[column].drop_nulls()
    matches = non_null.cast(pl.String).str.contains(str(rule_value))
    if matches.len() > 0 and not bool(matches.all()):
        return [f"Column '{column}' violates pattern constraint: {rule_value}"]
    return []


def _check_min_length(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is None:
        return []
    non_null = data[column].drop_nulls()
    lengths = non_null.cast(pl.String).str.len_chars()
    if lengths.len() > 0 and bool((lengths < int(rule_value)).any()):
        return [f"Column '{column}' violates min_length {rule_value}"]
    return []


def _check_max_length(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is None:
        return []
    non_null = data[column].drop_nulls()
    lengths = non_null.cast(pl.String).str.len_chars()
    if lengths.len() > 0 and bool((lengths > int(rule_value)).any()):
        return [f"Column '{column}' violates max_length {rule_value}"]
    return []


def _check_min(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is None:
        return []
    non_null = data[column].drop_nulls()
    numeric = non_null.cast(pl.Float64, strict=False)
    if numeric.len() > 0 and bool((numeric < float(rule_value)).any()):
        return [f"Column '{column}' violates min {rule_value}"]
    return []


def _check_max(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is None:
        return []
    non_null = data[column].drop_nulls()
    numeric = non_null.cast(pl.Float64, strict=False)
    if numeric.len() > 0 and bool((numeric > float(rule_value)).any()):
        return [f"Column '{column}' violates max {rule_value}"]
    return []


def _check_allowed_values(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is None:
        return []
    non_null = data[column].drop_nulls()
    invalid = non_null.is_in(rule_value).not_()
    if invalid.len() > 0 and bool(invalid.any()):
        return [f"Column '{column}' contains values outside allowed_values"]
    return []


def _check_allow_nan(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is False:
        as_float = data[column].cast(pl.Float64, strict=False)
        if bool(as_float.is_nan().any()):
            return [f"Column '{column}' contains NaN, but allow_nan is false"]
    return []


def _check_allow_inf(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is False:
        as_float = data[column].cast(pl.Float64, strict=False)
        if bool(as_float.is_infinite().any()):
            return [f"Column '{column}' contains inf, but allow_inf is false"]
    return []


def _check_max_null_fraction(data: pl.DataFrame, column: str, rule_value: Any) -> list[str]:
    if rule_value is None:
        return []
    null_fraction = data[column].null_count() / max(data.height, 1)
    if null_fraction > float(rule_value):
        return [
            f"Column '{column}' null fraction {null_fraction:.4f} exceeds "
            f"max_null_fraction {rule_value}"
        ]
    return []


_CONSTRAINT_CHECKERS: dict[str, ConstraintChecker] = {
    "unique": _check_unique,
    "pattern": _check_pattern,
    "min_length": _check_min_length,
    "max_length": _check_max_length,
    "min": _check_min,
    "max": _check_max,
    "allowed_values": _check_allowed_values,
    "allow_nan": _check_allow_nan,
    "allow_inf": _check_allow_inf,
    "max_null_fraction": _check_max_null_fraction,
}


def run_constraints(data: pl.DataFrame, constraints: dict[str, dict[str, Any]]) -> list[str]:
    errors: list[str] = []

    for column, rules in constraints.items():
        if column not in data.columns:
            errors.append(f"Constraint references missing column: {column}")
            continue

        for rule_name, rule_value in rules.items():
            checker = _CONSTRAINT_CHECKERS.get(rule_name)
            if checker is None:
                continue
            errors.extend(checker(data, column, rule_value))

    return errors
