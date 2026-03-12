"""TypedDict models for inspection metric payloads."""

from __future__ import annotations

from typing import Literal, TypedDict


class BaselineEvaluation(TypedDict):
    """Baseline availability and size summary."""

    source: str
    status: Literal["ready", "missing"]
    baseline_runs_available: int
    baseline_rows: list[int]


class RowCountMetrics(TypedDict):
    """Row count inspection metric payload."""

    current_rows: int
    baseline_rows: float | None
    warn_above: float | None
    fail_above: float | None
    change_pct: float | None
    comparison_status: str


class MissingColumnChangeMetric(TypedDict):
    """Column metric payload when configured column is missing in the dataset."""

    present: Literal[False]
    warn_change_pct: float | None
    fail_change_pct: float | None
    comparison_status: Literal["missing_column"]


class DistinctCountMetric(TypedDict):
    """Distinct count metric payload for a present column."""

    present: Literal[True]
    current_distinct_count: int
    baseline_distinct_count: float | None
    warn_change_pct: float | None
    fail_change_pct: float | None
    change_pct: float | None
    comparison_status: str


DistinctCountColumnMetric = DistinctCountMetric | MissingColumnChangeMetric
"""Union of distinct-count metric payloads for present and missing columns."""


class MissingColumnNullFractionMetric(TypedDict):
    """Null-fraction payload when configured column is missing in the dataset."""

    present: Literal[False]
    warn_change_pct: float | None
    fail_change_pct: float | None
    comparison_status: Literal["missing_column"]


class NullFractionMetric(TypedDict):
    """Null fraction metric payload for a present column."""

    present: Literal[True]
    current_null_fraction: float
    baseline_null_fraction: float | None
    warn_change_pct: float | None
    fail_change_pct: float | None
    change_pct: float | None
    comparison_status: str


NullFractionColumnMetric = NullFractionMetric | MissingColumnNullFractionMetric
"""Union of null-fraction metric payloads for present and missing columns."""


class MissingColumnCategoricalDistributionDriftMetric(TypedDict):
    """Categorical drift payload when configured column is missing in the dataset."""

    present: Literal[False]
    warn_above: float | None
    fail_above: float | None
    comparison_status: Literal["missing_column"]


class CategoricalDistributionDriftMetric(TypedDict):
    """Categorical distribution drift payload for a present column."""

    present: Literal[True]
    warn_above: float | None
    fail_above: float | None
    current_distribution: dict[str, float]
    baseline_distribution: dict[str, float]
    distance: float | None
    comparison_status: str


CategoricalDistributionDriftColumnMetric = (
    CategoricalDistributionDriftMetric | MissingColumnCategoricalDistributionDriftMetric
)
"""Union of categorical drift payloads for present and missing columns."""


class NumericCurrentStats(TypedDict):
    """Current-series summary statistics used in numeric drift metrics."""

    count: int
    mean: float | None
    std: float | None
    min: float | None
    max: float | None


class MissingColumnNumericDistributionDriftMetric(TypedDict):
    """Numeric drift payload when configured column is missing in the dataset."""

    present: Literal[False]
    method: str
    warn_above: float | None
    fail_above: float | None
    comparison_status: Literal["missing_column"]


class NumericDistributionDriftMetric(TypedDict):
    """Numeric distribution drift payload for a present column."""

    present: Literal[True]
    method: str
    warn_above: float | None
    fail_above: float | None
    current_stats: NumericCurrentStats
    baseline_count: int
    distance: float | None
    comparison_status: str


NumericDistributionDriftColumnMetric = (
    NumericDistributionDriftMetric | MissingColumnNumericDistributionDriftMetric
)
"""Union of numeric drift payloads for present and missing columns."""


class InspectionMetrics(TypedDict):
    """Full inspection metrics payload returned by inspection stage."""

    baseline: BaselineEvaluation
    row_count: RowCountMetrics
    null_fraction: dict[str, NullFractionColumnMetric]
    distinct_count: dict[str, DistinctCountColumnMetric]
    numeric_distribution_drift: dict[str, NumericDistributionDriftColumnMetric]
    categorical_distribution_drift: dict[str, CategoricalDistributionDriftColumnMetric]
    overall_status: str


class InspectionStatusOnlyMetrics(TypedDict):
    """Fallback inspection metrics payload with status only."""

    overall_status: str
