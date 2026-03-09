from __future__ import annotations

from pathlib import Path

import polars as pl

from data_pipeline_engine.cache_manager import read_from_cache
from data_pipeline_engine.models.rules import InspectionBaselineConfig


def evaluate_baseline_placeholder(
    config: InspectionBaselineConfig,
    source_csv: str | Path | None = None,
    baseline_csv: str | Path | None = None,
) -> dict[str, str | int]:
    cached_runs_count = 0
    if config.source == "reference_dataset":
        if baseline_csv is None:
            raise ValueError(
                "baseline_file_path must be provided when baseline source is reference_dataset"
            )

        baseline_path = Path(baseline_csv)
        if not baseline_path.exists():
            raise FileNotFoundError(f"Baseline CSV file does not exist: {baseline_path}")

        baseline_rows = pl.read_csv(baseline_path).height
        return {
            "source": config.source,
            "status": "placeholder",
            "message": "Baseline comparison is not implemented yet.",
            "cached_runs_available": 0,
            "reference_rows": baseline_rows,
        }

    if source_csv is not None:
        cached_runs_count = len(read_from_cache(source_csv=source_csv, strategy=config.source))

    return {
        "source": config.source,
        "status": "placeholder",
        "message": "Baseline comparison is not implemented yet.",
        "cached_runs_available": cached_runs_count,
    }
