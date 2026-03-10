"""Inspection baseline module for loading and summarizing baseline frames.

Provides pipeline functionality and includes: load_baseline_frames, evaluate_baseline.

Usage example:
.. code-block:: python

    from data_pipeline_engine.inspection.baseline import load_baseline_frames

    load_baseline_frames(...)
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_pipeline_engine.cache_manager import read_from_cache
from data_pipeline_engine.models.rules import InspectionBaselineConfig


def load_baseline_frames(
    config: InspectionBaselineConfig,
    source_csv: str | Path | None = None,
    baseline_csv: str | Path | None = None,
) -> list[pd.DataFrame]:
    """Load baseline frames.
    
    Args:
        config: Stage configuration object controlling the operation.
        source_csv: Source CSV path used to resolve cache or baseline data.
        baseline_csv: Path to baseline CSV data for inspection comparisons.
    
    Returns:
        Processed dataset produced by this operation.
    """
    if source_csv is None and config.source != "reference_dataset":
        return []
    return read_from_cache(
        source_csv=source_csv or Path("."),
        strategy=config.source,
        reference_csv=baseline_csv,
    )


def evaluate_baseline(
    config: InspectionBaselineConfig,
    source_csv: str | Path | None = None,
    baseline_csv: str | Path | None = None,
) -> dict[str, object]:
    """Evaluate baseline.
    
    Args:
        config: Stage configuration object controlling the operation.
        source_csv: Source CSV path used to resolve cache or baseline data.
        baseline_csv: Path to baseline CSV data for inspection comparisons.
    
    Returns:
        Baseline availability and row-count summary for inspection metrics.
    """
    baseline_frames = load_baseline_frames(
        config=config, source_csv=source_csv, baseline_csv=baseline_csv
    )
    baseline_rows = [
        len(frame) if hasattr(frame, "__len__") else int(getattr(frame, "height", 0))
        for frame in baseline_frames
    ]
    return {
        "source": config.source,
        "status": "ready" if baseline_frames else "missing",
        "baseline_runs_available": len(baseline_frames),
        "baseline_rows": baseline_rows,
    }
