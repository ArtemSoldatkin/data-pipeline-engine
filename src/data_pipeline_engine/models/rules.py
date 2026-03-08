from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class ColumnType(str, Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BOOL = "bool"
    DATE = "date"
    DATETIME = "datetime"


class TableSchemaColumn(BaseModel):
    name: str
    type: ColumnType
    required: bool = True
    nullable: bool = False


class ValidationRowsConfig(BaseModel):
    allow_empty: bool = True
    min_rows: int | None = Field(default=None, ge=0)
    max_rows: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_min_max_rows(self) -> ValidationRowsConfig:
        if (
            self.min_rows is not None
            and self.max_rows is not None
            and self.min_rows > self.max_rows
        ):
            raise ValueError("min_rows cannot be greater than max_rows")
        return self


class ValidationColumnsConfig(BaseModel):
    columns_schema: list[TableSchemaColumn] = Field(default_factory=list, alias="schema")
    allow_extra: bool = True


class RowRuleConfig(BaseModel):
    name: str
    expression: str


class ValidationRuleConfig(BaseModel):
    rows: ValidationRowsConfig = Field(default_factory=ValidationRowsConfig)
    columns: ValidationColumnsConfig = Field(default_factory=ValidationColumnsConfig)
    constraints: dict[str, dict[str, Any]] = Field(default_factory=dict)
    row_rules: list[RowRuleConfig] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def upgrade_flat_shape(cls, data: Any) -> Any:
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
        return self.rows.allow_empty

    @property
    def min_rows(self) -> int | None:
        return self.rows.min_rows

    @property
    def max_rows(self) -> int | None:
        return self.rows.max_rows

    @property
    def schema(self) -> list[TableSchemaColumn]:
        return self.columns.columns_schema

    @property
    def allow_extra_columns(self) -> bool:
        return self.columns.allow_extra

    @model_validator(mode="after")
    def validate_min_max_rows(self) -> ValidationRuleConfig:
        if (
            self.rows.min_rows is not None
            and self.rows.max_rows is not None
            and self.rows.min_rows > self.rows.max_rows
        ):
            raise ValueError("min_rows cannot be greater than max_rows")
        return self


class TransformationCastType(str, Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BOOL = "bool"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"


class TransformationColumnsConfig(BaseModel):
    rename: dict[str, str] = Field(default_factory=dict)
    drop: list[str] = Field(default_factory=list)
    cast: dict[str, TransformationCastType] = Field(default_factory=dict)
    normalize: dict[str, list[str]] = Field(default_factory=dict)


class DeriveRuleConfig(BaseModel):
    column: str
    expression: str


class RowFilterRuleConfig(BaseModel):
    expression: str


class RowDeduplicateRuleConfig(BaseModel):
    keys: list[str] = Field(default_factory=list)
    strategy: Literal["keep_first", "keep_last"] = "keep_first"


class TransformationRowsConfig(BaseModel):
    filter: list[RowFilterRuleConfig] = Field(default_factory=list)
    deduplicate: RowDeduplicateRuleConfig | None = None


class TransformationRuleConfig(BaseModel):
    columns: TransformationColumnsConfig = Field(default_factory=TransformationColumnsConfig)
    derive: list[DeriveRuleConfig] = Field(default_factory=list)
    rows: TransformationRowsConfig = Field(default_factory=TransformationRowsConfig)


class DataSkewRuleConfig(BaseModel):
    mode: Literal["placeholder"] = "placeholder"


class PipelineConfigs(BaseModel):
    validation: ValidationRuleConfig | None = None
    transformation: TransformationRuleConfig | None = None
    skew: DataSkewRuleConfig | None = None

    @model_validator(mode="after")
    def at_least_one_config(self) -> PipelineConfigs:
        if self.validation is None and self.transformation is None and self.skew is None:
            raise ValueError("At least one config must be provided")
        return self
