from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from typer.testing import CliRunner

from data_pipeline_engine.__main__ import app


class CliMainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()

    def test_cli_invokes_run_pipeline(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            csv_path = root / "input.csv"
            inspection_path = root / "inspection.yaml"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            inspection_path.write_text("baseline:\n  source: previous_run\n", encoding="utf-8")

            with patch("data_pipeline_engine.__main__.run_pipeline") as run_pipeline_mock:
                run_pipeline_mock.return_value = {"status": "success"}
                result = self.runner.invoke(
                    app,
                    [
                        str(csv_path),
                        "--inspection-config",
                        str(inspection_path),
                        "--cache-size",
                        "2",
                    ],
                )

        self.assertEqual(result.exit_code, 0)
        run_pipeline_mock.assert_called_once_with(
            csv_path=csv_path,
            validation_config_path=None,
            transformation_config_path=None,
            inspection_config_path=inspection_path,
            baseline_file_path=None,
            cache_size=2,
        )
        self.assertIn("'status': 'success'", result.stdout)

    def test_cli_requires_at_least_one_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "input.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            result = self.runner.invoke(app, [str(csv_path)])

        self.assertEqual(result.exit_code, 2)
        self.assertIn("At least one of --validation-config", result.stderr)


if __name__ == "__main__":
    unittest.main()
