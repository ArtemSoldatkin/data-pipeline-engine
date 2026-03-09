from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from data_pipeline_engine.__main__ import app


def test_cli_invokes_run_pipeline(tmp_path: Path) -> None:
    runner = CliRunner()
    csv_path = tmp_path / "input.csv"
    inspection_path = tmp_path / "inspection.yaml"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    inspection_path.write_text("baseline:\n  source: previous_run\n", encoding="utf-8")

    with patch("data_pipeline_engine.__main__.run_pipeline") as run_pipeline_mock:
        run_pipeline_mock.return_value = {"status": "success"}
        result = runner.invoke(
            app,
            [
                str(csv_path),
                "--inspection-config",
                str(inspection_path),
                "--cache-size",
                "2",
            ],
        )

    assert result.exit_code == 0
    run_pipeline_mock.assert_called_once_with(
        csv_path=csv_path,
        validation_config_path=None,
        transformation_config_path=None,
        inspection_config_path=inspection_path,
        baseline_file_path=None,
        cache_size=2,
    )
    assert "'status': 'success'" in result.stdout


def test_cli_requires_at_least_one_config(tmp_path: Path) -> None:
    runner = CliRunner()
    csv_path = tmp_path / "input.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    result = runner.invoke(app, [str(csv_path)])

    assert result.exit_code == 2
    assert "At least one of --validation-config" in result.stderr
