"""TypedDict models for top-level pipeline responses."""

from __future__ import annotations

from typing import Literal, TypedDict

from data_pipeline_engine.inspection.types import InspectionMetrics, InspectionStatusOnlyMetrics


class PipelineRunResult(TypedDict):
    """Return payload for successful run_pipeline execution."""

    status: Literal["success"]
    rows: int
    columns: list[str]
    validation_applied: bool
    transformation_applied: bool
    inspection_applied: bool
    inspection_metrics: InspectionMetrics | InspectionStatusOnlyMetrics
    cached_output_path: str

