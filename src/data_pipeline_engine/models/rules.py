"""Rules models module defining validation, transformation, and inspection config schemas.

Provides pipeline functionality and includes: ColumnType, TableSchemaColumn, ValidationRowsConfig, ValidationColumnsConfig.

Usage example:
.. code-block:: python

    from data_pipeline_engine.models.rules import ColumnType

    ColumnType(...)"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class ColumnType(str, Enum):
    """Supported logical column types for schema validation."""

    INT = "int"
    """Integer column type."""
    FLOAT = "float"
    """Floating-point column type."""
    STRING = "string"
    """String/text column type."""
    BOOL = "bool"
    """Boolean column type."""
    DATE = "date"
    """Date-only column type."""
    DATETIME = "datetime"
    """Datetime column type."""


class TableSchemaColumn(BaseModel):
    """Schema declaration for a single expected table column."""

    name: str
    """Column name."""
    type: ColumnType
    """Expected logical type of the column."""
    required: bool = True
    """Whether the column must exist in the dataset."""
    nullable: bool = False
    """Whether the column can contain null values."""


class ValidationRowsConfig(BaseModel):
    """Row-level validation settings."""

    allow_empty: bool = True
    """Whether empty datasets are allowed."""
    min_rows: int | None = Field(default=None, ge=0)
    """Minimum allowed number of rows."""
    max_rows: int | None = Field(default=None, ge=0)
    """Maximum allowed number of rows."""
    primary_key: list[str] = Field(default_factory=list)
    """Columns that define the primary key."""

    @model_validator(mode="after")
    def validate_min_max_rows(self) -> ValidationRowsConfig:
        """Validate min max rows.
        
        Args:
            None.
        
        Returns:
            Computed result of this operation.
        
        Raises:
            ValueError: If provided arguments are invalid.
        """
        if (
            self.min_rows is not None
            and self.max_rows is not None
            and self.min_rows > self.max_rows
        ):
            raise ValueError("min_rows cannot be greater than max_rows")
        return self


class ValidationColumnsConfig(BaseModel):
    """Column-level validation settings."""

    columns_schema: list[TableSchemaColumn] = Field(default_factory=list, alias="schema")
    """Expected table schema definition."""
    allow_extra: bool = True
    """Whether columns not declared in schema are allowed."""


class RowRuleConfig(BaseModel):
    """Definition of a row-level validation rule."""

    name: str
    """Rule name."""
    expression: str
    """Rule expression."""


class ValidationRuleConfig(BaseModel):
    """Top-level validation configuration model."""

    rows: ValidationRowsConfig = Field(default_factory=ValidationRowsConfig)
    """Row-level validation configuration."""
    columns: ValidationColumnsConfig = Field(default_factory=ValidationColumnsConfig)
    """Column-level validation configuration."""
    constraints: dict[str, dict[str, Any]] = Field(default_factory=dict)
    """Column constraints keyed by column name."""
    row_rules: list[RowRuleConfig] = Field(default_factory=list)
    """List of row-level rules to enforce."""

    @model_validator(mode="before")
    @classmethod
    def upgrade_flat_shape(cls, data: Any) -> Any:
        """Upgrade flat shape.
        
        Args:
            data: Dataset to process.
        
        Returns:
            Computed result of this operation.
        """
        if not isinstance(data, dict):
            return data

        # Backward compatibility with the original flat validation config shape.
        flat_keys = {"allow_empty", "min_rows", "max_rows", "schema", "allow_extra_columns"}
        if not (flat_keys & set(data.keys())):
            return data

        rows = {
            "allow_empty": data.get("allow_empty", True),
            "min_rows": data.get("min_rows"),
            "max_rows": data.get("max_rows"),
            "primary_key": data.get("primary_key", []),
        }
        columns = {
            "schema": data.get("schema", []),
            "allow_extra": data.get("allow_extra_columns", True),
        }
        return {
            "rows": rows,
            "columns": columns,
            "constraints": data.get("constraints", {}),
            "row_rules": data.get("row_rules", []),
        }

    @property
    def allow_empty(self) -> bool:
        """Allow empty.
        
        Args:
            None.
        
        Returns:
            Boolean result indicating whether the condition is satisfied.
        """
        return self.rows.allow_empty

    @property
    def min_rows(self) -> int | None:
        """Min rows.
        
        Args:
            None.
        
        Returns:
            Computed result of this operation.
        """
        return self.rows.min_rows

    @property
    def max_rows(self) -> int | None:
        """Max rows.
        
        Args:
            None.
        
        Returns:
            Computed result of this operation.
        """
        return self.rows.max_rows

    @property
    def primary_key(self) -> list[str]:
        """Primary key.
        
        Args:
            None.
        
        Returns:
            Validation issues found by this check. Empty when all checks pass.
        """
        return self.rows.primary_key

    @property
    def schema(self) -> list[TableSchemaColumn]:
        """Schema.
        
        Args:
            None.
        
        Returns:
            Computed result of this operation.
        """
        return self.columns.columns_schema

    @property
    def allow_extra_columns(self) -> bool:
        """Allow extra columns.
        
        Args:
            None.
        
        Returns:
            Boolean result indicating whether the condition is satisfied.
        """
        return self.columns.allow_extra

    @model_validator(mode="after")
    def validate_min_max_rows(self) -> ValidationRuleConfig:
        """Validate min max rows.
        
        Args:
            None.
        
        Returns:
            Computed result of this operation.
        
        Raises:
            ValueError: If provided arguments are invalid.
        """
        if (
            self.rows.min_rows is not None
            and self.rows.max_rows is not None
            and self.rows.min_rows > self.rows.max_rows
        ):
            raise ValueError("min_rows cannot be greater than max_rows")
        return self


class TransformationCastType(str, Enum):
    """Supported cast targets for transformation stage."""

    INT = "int"
    """Cast values to integer type."""
    FLOAT = "float"
    """Cast values to floating-point type."""
    STRING = "string"
    """Cast values to string type."""
    BOOL = "bool"
    """Cast values to boolean type."""
    DATE = "date"
    """Cast values to date-compatible type."""
    DATETIME = "datetime"
    """Cast values to datetime type."""
    TIMESTAMP = "timestamp"
    """Cast values to timestamp/datetime type."""


class TransformationColumnsConfig(BaseModel):
    """Column-level transformation settings."""

    rename: dict[str, str] = Field(default_factory=dict)
    """Column rename mapping: source -> target."""
    drop: list[str] = Field(default_factory=list)
    """Columns to drop."""
    cast: dict[str, TransformationCastType] = Field(default_factory=dict)
    """Column cast rules keyed by column name."""
    normalize: dict[str, list[str]] = Field(default_factory=dict)
    """Normalization operations keyed by column name."""
    derive: list[DeriveRuleConfig] = Field(default_factory=list)
    """Derived-column rules."""


class DeriveRuleConfig(BaseModel):
    """Definition of a derived-column expression."""

    column: str
    """Column name."""
    expression: str
    """Rule expression."""


class RowFilterRuleConfig(BaseModel):
    """Definition of a row filter expression."""

    expression: str
    """Filter expression."""


class RowDeduplicateRuleConfig(BaseModel):
    """Row deduplication settings."""

    keys: list[str] = Field(default_factory=list)
    """Columns used to identify duplicate rows."""
    strategy: Literal["keep_first", "keep_last"] = "keep_first"
    """Deduplication strategy for retaining duplicates."""


class TransformationRowsConfig(BaseModel):
    """Row-level transformation settings."""

    filter: list[RowFilterRuleConfig] = Field(default_factory=list)
    """Row filter rules."""
    deduplication: RowDeduplicateRuleConfig | None = None
    """Optional deduplication settings."""


class TransformationRuleConfig(BaseModel):
    """Top-level transformation configuration model."""

    columns: TransformationColumnsConfig = Field(default_factory=TransformationColumnsConfig)
    """Column-level transformation configuration."""
    rows: TransformationRowsConfig = Field(default_factory=TransformationRowsConfig)
    """Row-level transformation configuration."""


class InspectionBaselineConfig(BaseModel):
    """Baseline-source selection for inspection metrics."""

    source: Literal["previous_run", "rolling_window", "reference_dataset"] = "previous_run"
    """Source of baseline data for comparisons."""


class InspectionChangeThresholdConfig(BaseModel):
    """Warn/fail thresholds for percentage change metrics."""

    warn_change_pct: float | int | None = None
    """Warning threshold for percentage change."""
    fail_change_pct: float | int | None = None
    """Failure threshold for percentage change."""


class InspectionDriftThresholdConfig(BaseModel):
    """Warn/fail thresholds for drift distance metrics."""

    warn_above: float | int | None = None
    """Warning threshold for drift value."""
    fail_above: float | int | None = None
    """Failure threshold for drift value."""


class InspectionRowCountConfig(BaseModel):
    """Inspection settings for row-count comparison."""

    change_pct: InspectionDriftThresholdConfig = Field(
        default_factory=InspectionDriftThresholdConfig
    )
    """Row-count percentage change thresholds."""


class InspectionColumnThresholdsConfig(BaseModel):
    """Per-column threshold map for inspection comparisons."""

    columns: dict[str, InspectionChangeThresholdConfig] = Field(default_factory=dict)
    """Per-column change thresholds keyed by column name."""


class InspectionNumericDistributionConfig(BaseModel):
    """Numeric-distribution drift settings for a single column."""

    method: Literal["psi", "ks", "js_divergence"] = "psi"
    """Distance method for numeric distribution comparison."""
    warn_above: float | int | None = None
    """Warning threshold for distribution drift."""
    fail_above: float | int | None = None
    """Failure threshold for distribution drift."""


class InspectionNumericDistributionDriftConfig(BaseModel):
    """Per-column numeric distribution drift configuration."""

    columns: dict[str, InspectionNumericDistributionConfig] = Field(default_factory=dict)
    """Numeric drift settings keyed by column name."""


class InspectionCategoricalDistributionDriftConfig(BaseModel):
    """Per-column categorical distribution drift configuration."""

    columns: dict[str, InspectionDriftThresholdConfig] = Field(default_factory=dict)
    """Categorical drift settings keyed by column name."""


class InspectionRuleConfig(BaseModel):
    """Top-level inspection configuration model."""

    baseline: InspectionBaselineConfig = Field(default_factory=InspectionBaselineConfig)
    """Baseline settings for inspection metrics."""
    row_count: InspectionRowCountConfig = Field(default_factory=InspectionRowCountConfig)
    """Row-count metric settings."""
    null_fraction: InspectionColumnThresholdsConfig = Field(
        default_factory=InspectionColumnThresholdsConfig
    )
    """Null-fraction metric settings."""
    distinct_count: InspectionColumnThresholdsConfig = Field(
        default_factory=InspectionColumnThresholdsConfig
    )
    """Distinct-count metric settings."""
    numeric_distribution_drift: InspectionNumericDistributionDriftConfig = Field(
        default_factory=InspectionNumericDistributionDriftConfig
    )
    """Numeric-distribution-drift metric settings."""
    categorical_distribution_drift: InspectionCategoricalDistributionDriftConfig = Field(
        default_factory=InspectionCategoricalDistributionDriftConfig
    )
    """Categorical-distribution-drift metric settings."""


class PipelineConfigs(BaseModel):
    """Bundle of optional stage configs used by pipeline engine."""

    validation: ValidationRuleConfig | None = None
    """Validation stage configuration."""
    transformation: TransformationRuleConfig | None = None
    """Transformation stage configuration."""
    inspection: InspectionRuleConfig | None = None
    """Inspection stage configuration."""

    @model_validator(mode="after")
    def at_least_one_config(self) -> PipelineConfigs:
        """At least one config.
        
        Args:
            None.
        
        Returns:
            Computed result of this operation.
        
        Raises:
            ValueError: If provided arguments are invalid.
        """
        if self.validation is None and self.transformation is None and self.inspection is None:
            raise ValueError("At least one config must be provided")
        return self
