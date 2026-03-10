"""Expressions package module exposing shared expression helpers.

Provides pipeline functionality and includes: parse_literal, predicate_to_expr, row_rule_to_expr, evaluate_derive.

Usage example:
.. code-block:: python

    from data_pipeline_engine.expressions import parse_literal

    parse_literal(...)
"""

from data_pipeline_engine.expressions.parser import (
    evaluate_derive,
    parse_literal,
    predicate_to_expr,
    row_rule_to_expr,
)

__all__ = ["parse_literal", "predicate_to_expr", "row_rule_to_expr", "evaluate_derive"]
