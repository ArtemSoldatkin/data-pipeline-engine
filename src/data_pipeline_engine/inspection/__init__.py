"""Inspection package module exposing inspection stage utilities.

Provides pipeline functionality and includes: run_inspection, inspection, evaluate_baseline, load_baseline_frames.

Usage example:
.. code-block:: python

    from data_pipeline_engine.inspection import inspection

    inspection(...)"""

from data_pipeline_engine.inspection.baseline import (
    evaluate_baseline,
    load_baseline_frames,
)
from data_pipeline_engine.inspection.categorical_distribution_drift import (
    evaluate_categorical_distribution_drift,
)
from data_pipeline_engine.inspection.distinct_count import evaluate_distinct_count
from data_pipeline_engine.inspection.inspection import inspection
from data_pipeline_engine.inspection.null_fraction import evaluate_null_fraction
from data_pipeline_engine.inspection.numeric_distribution_drift import (
    evaluate_numeric_distribution_drift,
)
from data_pipeline_engine.inspection.row_count import evaluate_row_count

run_inspection = inspection

__all__ = [
    "inspection",
    "run_inspection",
    "evaluate_baseline",
    "load_baseline_frames",
    "evaluate_row_count",
    "evaluate_null_fraction",
    "evaluate_distinct_count",
    "evaluate_numeric_distribution_drift",
    "evaluate_categorical_distribution_drift",
]
