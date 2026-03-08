from __future__ import annotations

from enum import Enum
from typing import Literal

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


class ValidationRuleConfig(BaseModel):
    allow_empty: bool = True
    min_rows: int | None = Field(default=None, ge=0)
    max_rows: int | None = Field(default=None, ge=0)
    schema: list[TableSchemaColumn] = Field(default_factory=list)
    allow_extra_columns: bool = True

    @model_validator(mode="after")
    def validate_min_max_rows(self) -> "ValidationRuleConfig":
        if self.min_rows is not None and self.max_rows is not None and self.min_rows > self.max_rows:
            raise ValueError("min_rows cannot be greater than max_rows")
        return self


class CleaningRuleConfig(BaseModel):
    mode: Literal["placeholder"] = "placeholder"


class DataSkewRuleConfig(BaseModel):
    mode: Literal["placeholder"] = "placeholder"


class PipelineConfigs(BaseModel):
    validation: ValidationRuleConfig | None = None
    cleaning: CleaningRuleConfig | None = None
    skew: DataSkewRuleConfig | None = None

    @model_validator(mode="after")
    def at_least_one_config(self) -> "PipelineConfigs":
        if self.validation is None and self.cleaning is None and self.skew is None:
            raise ValueError("At least one config must be provided")
        return self
