from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from data_pipeline_engine.cache_manager import read_from_cache, write_to_cache


def test_read_from_cache_by_strategy(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")

    data = pd.DataFrame({"id": [1]})
    write_to_cache(data, csv_path, cache_size=10)
    write_to_cache(data, csv_path, cache_size=10)
    write_to_cache(data, csv_path, cache_size=10)

    previous = read_from_cache(csv_path, strategy="previous_run")
    rolling = read_from_cache(csv_path, strategy="rolling_window", rolling_window_size=2)
    reference_dataset_path = tmp_path / "reference.csv"
    reference_dataset_path.write_text("id\n9\n", encoding="utf-8")
    reference = read_from_cache(
        csv_path,
        strategy="reference_dataset",
        reference_csv=reference_dataset_path,
    )

    assert len(previous) == 1
    assert len(rolling) == 2
    assert len(reference) == 1


def test_write_to_cache_rejects_invalid_cache_size(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    data = pd.DataFrame({"id": [1]})

    with pytest.raises(ValueError, match="cache_size must be at least 1"):
        write_to_cache(data, csv_path, cache_size=0)


def test_reference_dataset_requires_reference_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"

    with pytest.raises(
        ValueError, match="reference_csv must be provided when strategy is reference_dataset"
    ):
        read_from_cache(csv_path, strategy="reference_dataset")


def test_reference_dataset_rejects_missing_reference_file(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    missing_reference = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError, match="Reference CSV file does not exist"):
        read_from_cache(
            csv_path,
            strategy="reference_dataset",
            reference_csv=missing_reference,
        )


def test_rolling_window_non_positive_size_falls_back_to_one(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")

    data = pd.DataFrame({"id": [1]})
    write_to_cache(data, csv_path, cache_size=10)
    write_to_cache(data, csv_path, cache_size=10)
    write_to_cache(data, csv_path, cache_size=10)

    rolling_zero = read_from_cache(csv_path, strategy="rolling_window", rolling_window_size=0)
    rolling_negative = read_from_cache(
        csv_path, strategy="rolling_window", rolling_window_size=-3
    )

    assert len(rolling_zero) == 1
    assert len(rolling_negative) == 1
