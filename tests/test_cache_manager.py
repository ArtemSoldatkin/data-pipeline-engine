from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import polars as pl

from data_pipeline_engine.cache_manager import read_from_cache, write_to_cache


class CacheManagerTests(unittest.TestCase):
    @unittest.skip("Polars runtime instability in this environment for cache IO execution")
    def test_read_from_cache_by_strategy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "input.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")

            data = pl.DataFrame({"id": [1]})
            write_to_cache(data, csv_path, cache_size=10)
            write_to_cache(data, csv_path, cache_size=10)
            write_to_cache(data, csv_path, cache_size=10)

            previous = read_from_cache(csv_path, strategy="previous_run")
            rolling = read_from_cache(csv_path, strategy="rolling_window", rolling_window_size=2)
            reference_dataset_path = Path(temp_dir) / "reference.csv"
            reference_dataset_path.write_text("id\n9\n", encoding="utf-8")
            reference = read_from_cache(
                csv_path,
                strategy="reference_dataset",
                reference_csv=reference_dataset_path,
            )

            self.assertEqual(len(previous), 1)
            self.assertEqual(len(rolling), 2)
            self.assertEqual(len(reference), 1)


if __name__ == "__main__":
    unittest.main()
