"""Validation stage package."""

from data_pipeline_engine.validation.validation import validation

run_validation = validation

__all__ = ["validation", "run_validation"]
