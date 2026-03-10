"""Validation package module exposing validation stage entrypoints.

Provides pipeline functionality and includes: run_validation, ValidationExecutionError, validation.

Usage example:
.. code-block:: python

    from data_pipeline_engine.validation import run_validation

    run_validation(...)
"""

from data_pipeline_engine.validation.errors import ValidationExecutionError
from data_pipeline_engine.validation.validation import validation

run_validation = validation

__all__ = ["ValidationExecutionError", "validation", "run_validation"]
