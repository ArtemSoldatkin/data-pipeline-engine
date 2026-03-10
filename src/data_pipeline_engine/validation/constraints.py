"""Validation constraints module for dataframe validation checks.

Provides pipeline functionality and includes: _check_unique, _check_pattern, _check_min_length, _check_max_length.

Usage example:
.. code-block:: python

    from data_pipeline_engine.validation.constraints import run_constraints

    run_constraints(...)"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
import pandas as pd


ConstraintChecker = Callable[[pd.DataFrame, str, Any], list[str]]


def _check_unique(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check unique.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is True and data[column].duplicated().any():
        return [f"Column '{column}' violates unique constraint"]
    return []


def _check_pattern(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check pattern.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is None:
        return []
    non_null = data[column].dropna().astype("string")
    matches = non_null.str.contains(str(rule_value), regex=True, na=False)
    if not matches.empty and not bool(matches.all()):
        return [f"Column '{column}' violates pattern constraint: {rule_value}"]
    return []


def _check_min_length(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check min length.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is None:
        return []
    lengths = data[column].dropna().astype("string").str.len()
    if not lengths.empty and bool((lengths < int(rule_value)).any()):
        return [f"Column '{column}' violates min_length {rule_value}"]
    return []


def _check_max_length(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check max length.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is None:
        return []
    lengths = data[column].dropna().astype("string").str.len()
    if not lengths.empty and bool((lengths > int(rule_value)).any()):
        return [f"Column '{column}' violates max_length {rule_value}"]
    return []


def _check_min(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check min.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is None:
        return []
    numeric = pd.to_numeric(data[column].dropna(), errors="coerce").dropna()
    if not numeric.empty and bool((numeric < float(rule_value)).any()):
        return [f"Column '{column}' violates min {rule_value}"]
    return []


def _check_max(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check max.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is None:
        return []
    numeric = pd.to_numeric(data[column].dropna(), errors="coerce").dropna()
    if not numeric.empty and bool((numeric > float(rule_value)).any()):
        return [f"Column '{column}' violates max {rule_value}"]
    return []


def _check_allowed_values(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check allowed values.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is None:
        return []
    non_null = data[column].dropna()
    invalid = ~non_null.isin(rule_value)
    if not invalid.empty and bool(invalid.any()):
        return [f"Column '{column}' contains values outside allowed_values"]
    return []


def _check_allow_nan(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check allow nan.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is False:
        string_nan = data[column].dropna().astype("string").str.lower().eq("nan")
        if not string_nan.empty and bool(string_nan.any()):
            return [f"Column '{column}' contains NaN, but allow_nan is false"]
    return []


def _check_allow_inf(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check allow inf.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is False:
        as_float = pd.to_numeric(data[column], errors="coerce")
        if bool(as_float.isin([float("inf"), float("-inf")]).any()):
            return [f"Column '{column}' contains inf, but allow_inf is false"]
    return []


def _check_max_null_fraction(data: pd.DataFrame, column: str, rule_value: Any) -> list[str]:
    """Check max null fraction.
    
    Args:
        data: Dataset to process.
        column: Column name to evaluate or transform.
        rule_value: Configured value for a constraint rule.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
    if rule_value is None:
        return []
    null_fraction = float(data[column].isna().sum()) / max(len(data), 1)
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


def run_constraints(data: pd.DataFrame, constraints: dict[str, dict[str, Any]]) -> list[str]:
    """Run constraints.
    
    Args:
        data: Dataset to process.
        constraints: Column constraints keyed by column name.
    
    Returns:
        Validation issues found by this check. Empty when all checks pass.
    """
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
