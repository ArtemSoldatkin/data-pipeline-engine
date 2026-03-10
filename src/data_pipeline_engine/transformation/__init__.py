"""Transformation package module exposing transformation stage entrypoints.

Provides pipeline functionality and includes: run_transformation, StageExecutionError, transformation.

Usage example:
.. code-block:: python

    from data_pipeline_engine.transformation import StageExecutionError

    StageExecutionError(...)"""

from data_pipeline_engine.transformation.errors import StageExecutionError
from data_pipeline_engine.transformation.transformation import transformation

run_transformation = transformation

__all__ = [
    "StageExecutionError",
    "transformation",
    "run_transformation",
]
