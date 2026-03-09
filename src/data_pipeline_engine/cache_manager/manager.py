from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Literal

import pandas as pd


_CACHE_CSV_PATTERN = re.compile(r"^\d{8}T\d{9,}Z_.*\.csv$")


def _cache_dir_for_source(source_csv: Path) -> Path:
    return source_csv.parent / ".data_pipeline_cache"


def _cache_files(source_csv: Path) -> list[Path]:
    cache_dir = _cache_dir_for_source(source_csv)
    if not cache_dir.exists():
        return []
    return sorted(
        [
            path
            for path in cache_dir.glob("*.csv")
            if path.is_file() and _CACHE_CSV_PATTERN.match(path.name)
        ],
        key=lambda path: path.name,
    )


def write_to_cache(data: pd.DataFrame, source_csv: str | Path, cache_size: int) -> Path:
    source_path = Path(source_csv)
    if cache_size < 1:
        raise ValueError("cache_size must be at least 1")

    cache_dir = _cache_dir_for_source(source_path)
    cache_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output_path = cache_dir / f"{timestamp}_{source_path.stem}.csv"
    data.to_csv(output_path, index=False)

    cached_files = _cache_files(source_path)
    excess = len(cached_files) - cache_size
    for old_path in cached_files[: max(excess, 0)]:
        old_path.unlink(missing_ok=True)

    return output_path


def read_from_cache(
    source_csv: str | Path,
    strategy: Literal["previous_run", "rolling_window", "reference_dataset"],
    rolling_window_size: int = 3,
    reference_csv: str | Path | None = None,
) -> list[pd.DataFrame]:
    source_path = Path(source_csv)
    if strategy == "reference_dataset":
        if reference_csv is None:
            raise ValueError(
                "baseline_file_path must be provided when baseline source is reference_dataset"
            )
        reference_path = Path(reference_csv)
        if not reference_path.exists():
            raise FileNotFoundError(f"Baseline CSV file does not exist: {reference_path}")
        return [pd.read_csv(reference_path)]

    cached_files = _cache_files(source_path)
    if not cached_files:
        return []

    if strategy == "previous_run":
        selected = cached_files[-1:]
    else:
        selected = cached_files[-max(rolling_window_size, 1) :]

    return [pd.read_csv(path) for path in selected]
