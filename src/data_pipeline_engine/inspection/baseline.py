from __future__ import annotations

from pathlib import Path

from data_pipeline_engine.cache_manager import read_from_cache
from data_pipeline_engine.models.rules import InspectionBaselineConfig


def evaluate_baseline_placeholder(
    config: InspectionBaselineConfig, source_csv: str | Path | None = None
) -> dict[str, str | int]:
    cached_runs_count = 0
    if source_csv is not None:
        cached_runs_count = len(read_from_cache(source_csv=source_csv, strategy=config.source))

    return {
        "source": config.source,
        "status": "placeholder",
        "message": "Baseline comparison is not implemented yet.",
        "cached_runs_available": cached_runs_count,
    }
