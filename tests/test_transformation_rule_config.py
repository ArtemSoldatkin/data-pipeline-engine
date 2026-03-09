from __future__ import annotations

from pathlib import Path

from data_pipeline_engine.config_loader import load_pipeline_configs


def test_loads_transformation_yaml_shape() -> None:
    transformation_path = Path(__file__).resolve().parents[1] / "configs" / "examples" / "transformation.yaml"

    configs = load_pipeline_configs(transformation_config_path=transformation_path)
    transformation = configs.transformation

    assert transformation is not None
    assert transformation.columns.rename["user_id"] == "id"
    assert "temp_flag" in transformation.columns.drop
    assert transformation.columns.cast["score"].value == "float"
    assert transformation.columns.cast["created_at"].value == "timestamp"
    assert transformation.columns.normalize["status"] == ["lowercase"]
    assert transformation.columns.derive[0].column == "score_bucket"
    assert transformation.rows.filter[0].expression == "status != 'deleted'"
    assert transformation.rows.deduplication is not None
    assert transformation.rows.deduplication.keys == ["id"]
    assert transformation.rows.deduplication.strategy == "keep_first"
