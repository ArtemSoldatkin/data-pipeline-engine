from __future__ import annotations

from data_pipeline_engine.models.rules import InspectionBaselineConfig


def evaluate_baseline_placeholder(config: InspectionBaselineConfig) -> dict[str, str]:
    return {
        "source": config.source,
        "status": "placeholder",
        "message": "Baseline comparison is not implemented yet.",
    }
