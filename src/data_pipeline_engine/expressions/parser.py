"""Expression parser module for evaluating derive and predicate expressions.

Provides pipeline functionality and includes: parse_literal, resolve_token, evaluate_derive, predicate_to_mask.

Usage example:
.. code-block:: python

    from data_pipeline_engine.expressions.parser import parse_literal

    parse_literal(...)"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd


def parse_literal(token: str) -> Any:
    """Parse literal.
    
    Args:
        token: Token value to parse or resolve.
    
    Returns:
        Computed result of this operation.
    """
    normalized = token.strip()
    if normalized.lower() == "null":
        return None
    if normalized.startswith(("'", '"')) and normalized.endswith(("'", '"')):
        return normalized[1:-1]
    try:
        if "." in normalized:
            return float(normalized)
        return int(normalized)
    except ValueError:
        return ("column_ref", normalized)


def resolve_token(token: Any, row: dict[str, Any]) -> Any:
    """Resolve token.
    
    Args:
        token: Token value to parse or resolve.
        row: Single row represented as a dictionary of column values.
    
    Returns:
        Computed result of this operation.
    """
    if isinstance(token, tuple) and token[0] == "column_ref":
        return row.get(token[1])
    return token


def evaluate_derive(expression: str, row: dict[str, Any]) -> Any:
    """Evaluate derive.
    
    Args:
        expression: Expression string to parse or evaluate.
        row: Single row represented as a dictionary of column values.
    
    Returns:
        Computed result of this operation.
    """
    compact = " ".join(expression.split())
    case_pattern = re.compile(
        r"^case when ([A-Za-z_]\w*) >= ([\d.]+) then \"([^\"]+)\" "
        r"when ([A-Za-z_]\w*) >= ([\d.]+) then \"([^\"]+)\" else \"([^\"]+)\"$",
        flags=re.IGNORECASE,
    )
    case_match = case_pattern.match(compact)
    if case_match:
        col1, threshold1, label1, col2, threshold2, label2, label_else = case_match.groups()
        if (row.get(col1) or float("-inf")) >= float(threshold1):
            return label1
        if (row.get(col2) or float("-inf")) >= float(threshold2):
            return label2
        return label_else

    return resolve_token(parse_literal(compact), row)


def predicate_to_mask(data: pd.DataFrame, expression: str) -> pd.Series:
    """Predicate to mask.
    
    Args:
        data: Dataset to process.
        expression: Expression string to parse or evaluate.
    
    Returns:
        Boolean mask indicating which rows satisfy the expression.
    
    Raises:
        ValueError: If provided arguments are invalid.
    """
    match = re.match(r"^\s*([A-Za-z_][\w]*)\s*(==|!=|>=|<=|>|<)\s*(.+?)\s*$", expression)
    if not match:
        raise ValueError(f"Unsupported expression: {expression}")

    lhs_col, operator, rhs_raw = match.groups()
    if lhs_col not in data.columns:
        raise ValueError(f"Unknown column in expression: {lhs_col}")

    lhs = data[lhs_col]
    parsed_rhs = parse_literal(rhs_raw)

    if parsed_rhs is None:
        if operator == "==":
            return lhs.isna()
        if operator == "!=":
            return lhs.notna()
        raise ValueError(f"Unsupported null comparison in expression: {expression}")

    if isinstance(parsed_rhs, tuple) and parsed_rhs[0] == "column_ref":
        rhs_col = parsed_rhs[1]
        if rhs_col not in data.columns:
            raise ValueError(f"Unknown column in expression: {rhs_col}")
        rhs: Any = data[rhs_col]
    else:
        rhs = parsed_rhs

    if operator == "==":
        return lhs == rhs
    if operator == "!=":
        return lhs != rhs
    if operator == ">":
        return lhs > rhs
    if operator == "<":
        return lhs < rhs
    if operator == ">=":
        return lhs >= rhs
    return lhs <= rhs


def row_rule_to_mask(data: pd.DataFrame, expression: str) -> pd.Series:
    """Row rule to mask.
    
    Args:
        data: Dataset to process.
        expression: Expression string to parse or evaluate.
    
    Returns:
        Boolean mask indicating rows that satisfy the row rule.
    """
    parts = re.split(r"\s+implies\s+", expression, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) == 2:
        antecedent = predicate_to_mask(data, parts[0]).fillna(False)
        consequent = predicate_to_mask(data, parts[1]).fillna(False)
        return (~antecedent) | consequent
    return predicate_to_mask(data, expression)


# Backward-compatible alias name used by older call sites.
predicate_to_expr = predicate_to_mask
row_rule_to_expr = row_rule_to_mask
