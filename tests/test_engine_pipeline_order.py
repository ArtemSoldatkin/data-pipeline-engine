from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from data_pipeline_engine.engine import run_pipeline


def test_runs_stages_in_requested_order(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    transformation_path = project_root / "configs" / "examples" / "transformation.yaml"
    validation_path = project_root / "configs" / "examples" / "validation.yaml"
    inspection_path = project_root / "configs" / "examples" / "inspection.yaml"

    csv_path = tmp_path / "input.csv"
    csv_path.write_text("id,name,status,score\n1,Alice,active,91.5\n", encoding="utf-8")
    cache_dir = tmp_path / ".data_pipeline_cache"

    calls: list[str] = []

    def transformation_stage(data, config):
        assert config is not None
        calls.append("transformation")
        return data

    def validation_stage(data, config):
        assert config is not None
        calls.append("validation")
        return data

    def inspection_stage(data, config, source_csv=None, baseline_csv=None, return_metrics=False):
        assert config is not None
        assert source_csv is not None
        assert baseline_csv is None
        assert return_metrics is True
        calls.append("inspection")
        return data, {"overall_status": "pass"}

    with patch("data_pipeline_engine.engine.transformation", side_effect=transformation_stage), patch(
        "data_pipeline_engine.engine.validation", side_effect=validation_stage
    ), patch("data_pipeline_engine.engine.inspection", side_effect=inspection_stage):
        result = run_pipeline(
            csv_path=csv_path,
            transformation_config_path=transformation_path,
            validation_config_path=validation_path,
            inspection_config_path=inspection_path,
        )

    assert calls == ["transformation", "validation", "inspection"]
    assert result["status"] == "success"
    assert result["transformation_applied"] is True
    assert result["validation_applied"] is True
    assert result["inspection_applied"] is True
    assert result["inspection_metrics"]["overall_status"] == "pass"
    assert cache_dir.exists()
    assert Path(result["cached_output_path"]).exists()
    assert Path(result["cached_output_path"]).name.endswith("_input.csv")


def test_prunes_cache_to_cache_size(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    inspection_path = project_root / "configs" / "examples" / "inspection.yaml"

    csv_path = tmp_path / "input.csv"
    csv_path.write_text("id,name,status,score\n1,Alice,active,91.5\n", encoding="utf-8")
    cache_dir = tmp_path / ".data_pipeline_cache"

    with patch("data_pipeline_engine.engine.transformation", side_effect=lambda d, c: d), patch(
        "data_pipeline_engine.engine.validation", side_effect=lambda d, c: d
    ), patch(
        "data_pipeline_engine.engine.inspection",
        side_effect=lambda d, c, source_csv=None, baseline_csv=None, return_metrics=False: (
            d,
            {"overall_status": "pass"},
        ),
    ):
        result1 = run_pipeline(
            csv_path=csv_path,
            inspection_config_path=inspection_path,
            cache_size=1,
        )
        result2 = run_pipeline(
            csv_path=csv_path,
            inspection_config_path=inspection_path,
            cache_size=1,
        )

    cached_files = [path for path in cache_dir.iterdir() if path.is_file()]
    assert len(cached_files) == 1
    assert Path(result2["cached_output_path"]) == cached_files[0]
    assert result1["cached_output_path"] != result2["cached_output_path"]
    assert result2["inspection_metrics"]["overall_status"] == "pass"


def test_passes_baseline_file_to_inspection_for_reference_dataset(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    baseline_csv_path = tmp_path / "baseline.csv"
    baseline_csv_path.write_text("id\n10\n", encoding="utf-8")
    inspection_path = tmp_path / "inspection.yaml"
    inspection_path.write_text("baseline:\n  source: reference_dataset\n", encoding="utf-8")

    def inspection_stage(data, config, source_csv=None, baseline_csv=None, return_metrics=False):
        assert config is not None
        assert Path(source_csv) == csv_path
        assert Path(baseline_csv) == baseline_csv_path
        assert return_metrics is True
        return data, {"overall_status": "pass"}

    with patch("data_pipeline_engine.engine.transformation", side_effect=lambda d, c: d), patch(
        "data_pipeline_engine.engine.validation", side_effect=lambda d, c: d
    ), patch("data_pipeline_engine.engine.inspection", side_effect=inspection_stage):
        run_pipeline(
            csv_path=csv_path,
            inspection_config_path=inspection_path,
            baseline_file_path=baseline_csv_path,
        )
