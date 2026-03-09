from __future__ import annotations

import pandas as pd

from data_pipeline_engine.models.rules import TransformationRuleConfig
from data_pipeline_engine.transformation.cast import cast_columns
from data_pipeline_engine.transformation.deduplication import deduplicate_rows
from data_pipeline_engine.transformation.derive import derive_columns
from data_pipeline_engine.transformation.drop import drop_columns
from data_pipeline_engine.transformation.filter import filter_rows
from data_pipeline_engine.transformation.normalize import normalize_columns
from data_pipeline_engine.transformation.rename import rename_columns


def transformation(data: pd.DataFrame, config: TransformationRuleConfig | None) -> pd.DataFrame:
    if config is None:
        return data

    data = rename_columns(data, config.columns.rename)
    data = drop_columns(data, config.columns.drop)
    data = cast_columns(data, config.columns.cast)
    data = normalize_columns(data, config.columns.normalize)
    data = derive_columns(data, config.columns.derive)
    data = filter_rows(data, config.rows.filter)
    data = deduplicate_rows(data, config.rows.deduplication)
    return data
