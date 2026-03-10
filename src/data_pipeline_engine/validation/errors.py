"""Validation errors module defining validation execution exceptions.

Provides pipeline functionality and includes: ValidationExecutionError.

Usage example:
.. code-block:: python

    from data_pipeline_engine.validation.errors import ValidationExecutionError

    ValidationExecutionError(...)
"""

class ValidationExecutionError(Exception):
    """Raised when validation stage fails."""
