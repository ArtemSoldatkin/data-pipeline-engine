"""Transformation errors module defining stage execution exceptions.

Provides pipeline functionality and includes: StageExecutionError.

Usage example:
.. code-block:: python

    from data_pipeline_engine.transformation.errors import StageExecutionError

    StageExecutionError(...)"""

class StageExecutionError(Exception):
    """Raised when any pipeline stage fails."""
