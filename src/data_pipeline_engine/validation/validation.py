"""Validation orchestration module for executing all validation checks.

Provides pipeline functionality and includes: validation.

Usage example:
.. code-block:: python

    from data_pipeline_engine.validation.validation import validation

    validation(...)
"""

from __future__ import annotations

import pandas as pd

from data_pipeline_engine.models.rules import ValidationRuleConfig
from data_pipeline_engine.validation.allow_empty import check_allow_empty
from data_pipeline_engine.validation.constraints import run_constraints
from data_pipeline_engine.validation.errors import ValidationExecutionError
from data_pipeline_engine.validation.primary_keys import check_primary_keys
from data_pipeline_engine.validation.row_boundaries import check_row_boundaries
from data_pipeline_engine.validation.row_rules import run_row_rules
from data_pipeline_engine.validation.schema import verify_schema


def validation(data: pd.DataFrame, config: ValidationRuleConfig | None) -> pd.DataFrame:
    """Validation.
    
    Args:
        data: Dataset to process.
        config: Stage configuration object controlling the operation.
    
    Returns:
        Validated dataset when all checks pass.
    
    Raises:
        ValueError: If provided arguments are invalid.
    """
    if config is None:
        return data

    errors: list[str] = []
    errors.extend(check_allow_empty(data, config.allow_empty))
    errors.extend(check_row_boundaries(data, config.min_rows, config.max_rows))
    errors.extend(check_primary_keys(data, config.primary_key))
    errors.extend(verify_schema(data, config.schema, config.allow_extra_columns))
    errors.extend(run_constraints(data, config.constraints))
    errors.extend(run_row_rules(data, config.row_rules))

    if errors:
        raise ValidationExecutionError("Validation failed:\n- " + "\n- ".join(errors))
    return data
