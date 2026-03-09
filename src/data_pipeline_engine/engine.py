from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any

import polars as pl

from data_pipeline_engine.config_loader import load_pipeline_configs
from data_pipeline_engine.inspection import inspection
from data_pipeline_engine.transformation import StageExecutionError, transformation
from data_pipeline_engine.validation import ValidationExecutionError, validation


class PipelineExecutionError(Exception):
    """Raised when pipeline validations fail."""


_CACHE_CSV_PATTERN = re.compile(r"^\d{8}T\d{9,}Z_.*\.csv$")


def _write_to_cache(data: pl.DataFrame, source_csv: Path, cache_size: int) -> Path:
    if cache_size < 1:
        raise PipelineExecutionError("cache_size must be at least 1")

    cache_dir = source_csv.parent / ".data_pipeline_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output_path = cache_dir / f"{timestamp}_{source_csv.stem}.csv"
    data.write_csv(output_path)

    cached_files = sorted(
        [
            path
            for path in cache_dir.glob("*.csv")
            if path.is_file() and _CACHE_CSV_PATTERN.match(path.name)
        ],
        key=lambda path: path.name,
    )
    excess = len(cached_files) - cache_size
    for old_path in cached_files[: max(excess, 0)]:
        old_path.unlink(missing_ok=True)

    return output_path


def run_pipeline(
    csv_path: str | Path,
    validation_config_path: str | Path | None = None,
    transformation_config_path: str | Path | None = None,
    inspection_config_path: str | Path | None = None,
    cache_size: int = 1,
) -> dict[str, Any]:
    """Run pipeline over a CSV with optional YAML configs (at least one required)."""
    if (
        transformation_config_path is None
        and validation_config_path is None
        and inspection_config_path is None
    ):
        raise PipelineExecutionError(
            "At least one config path must be provided: "
            "transformation_config_path, validation_config_path, or inspection_config_path"
        )

    configs = load_pipeline_configs(
        transformation_config_path=transformation_config_path,
        validation_config_path=validation_config_path,
        inspection_config_path=inspection_config_path,
    )

    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file does not exist: {csv_file}")

    data = pl.read_csv(csv_file)
    try:
        data = transformation(data, configs.transformation)
        data = validation(data, configs.validation)
        data = inspection(data, configs.inspection)
    except (StageExecutionError, ValidationExecutionError, ValueError) as exc:
        raise PipelineExecutionError(str(exc)) from exc

    cached_output = _write_to_cache(data=data, source_csv=csv_file, cache_size=cache_size)

    return {
        "status": "success",
        "rows": data.height,
        "columns": data.columns,
        "validation_applied": configs.validation is not None,
        "transformation_applied": configs.transformation is not None,
        "inspection_applied": configs.inspection is not None,
        "cached_output_path": str(cached_output),
    }
