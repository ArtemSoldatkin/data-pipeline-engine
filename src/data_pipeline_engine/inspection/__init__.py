"""Inspection stage package."""

from data_pipeline_engine.inspection.inspection import inspection

run_inspection = inspection

__all__ = ["inspection", "run_inspection"]
