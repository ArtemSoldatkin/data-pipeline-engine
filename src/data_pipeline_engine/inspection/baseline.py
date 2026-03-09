from __future__ import annotations

from pathlib import Path

import polars as pl

from data_pipeline_engine.cache_manager import read_from_cache
from data_pipeline_engine.models.rules import InspectionBaselineConfig


def load_baseline_frames(
    config: InspectionBaselineConfig,
    source_csv: str | Path | None = None,
    baseline_csv: str | Path | None = None,
) -> list[pl.DataFrame]:
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
    baseline_frames = load_baseline_frames(
        config=config, source_csv=source_csv, baseline_csv=baseline_csv
    )
    baseline_rows = [frame.height for frame in baseline_frames]
    return {
        "source": config.source,
        "status": "ready" if baseline_frames else "missing",
        "baseline_runs_available": len(baseline_frames),
        "baseline_rows": baseline_rows,
    }
