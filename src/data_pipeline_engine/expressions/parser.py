from __future__ import annotations

import re
from typing import Any

import polars as pl


def parse_literal(token: str) -> Any:
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
    if isinstance(token, tuple) and token[0] == "column_ref":
        return row.get(token[1])
    return token


def evaluate_derive(expression: str, row: dict[str, Any]) -> Any:
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


def predicate_to_expr(expression: str) -> pl.Expr:
    match = re.match(r"^\s*([A-Za-z_][\w]*)\s*(==|!=|>=|<=|>|<)\s*(.+?)\s*$", expression)
    if not match:
        raise ValueError(f"Unsupported expression: {expression}")

    lhs_col, operator, rhs_raw = match.groups()
    lhs = pl.col(lhs_col)
    parsed_rhs = parse_literal(rhs_raw)

    if parsed_rhs is None:
        if operator == "==":
            return lhs.is_null()
        if operator == "!=":
            return lhs.is_not_null()
        raise ValueError(f"Unsupported null comparison in expression: {expression}")

    if isinstance(parsed_rhs, tuple) and parsed_rhs[0] == "column_ref":
        rhs = pl.col(parsed_rhs[1])
    else:
        rhs = pl.lit(parsed_rhs)

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


def row_rule_to_expr(expression: str) -> pl.Expr:
    parts = re.split(r"\s+implies\s+", expression, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) == 2:
        antecedent = predicate_to_expr(parts[0]).fill_null(False)
        consequent = predicate_to_expr(parts[1]).fill_null(False)
        return (~antecedent) | consequent
    return predicate_to_expr(expression)
