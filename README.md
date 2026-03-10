# Data Pipeline Engine

CSV data pipeline engine with YAML-driven `validation`, `transformation`, and `inspection` stages.

## Overview

The engine loads one or more stage configuration files, executes stages in order, and returns a structured result dictionary.

Execution order:
1. `transformation`
2. `validation`
3. `inspection`

Main API:
- `data_pipeline_engine.engine.run_pipeline`

## Installation

```bash
pip install -e .
```

## Python Usage

```python
from data_pipeline_engine.engine import run_pipeline

result = run_pipeline(
    csv_path="configs/examples/sample.csv",
    validation_config_path="configs/examples/validation.yaml",
    transformation_config_path="configs/examples/transformation.yaml",
    inspection_config_path="configs/examples/inspection.yaml",
    baseline_file_path=None,
    cache_size=3,
)

print(result["status"])
print(result["cached_output_path"])
```

At least one config path must be provided:
- `validation_config_path`
- `transformation_config_path`
- `inspection_config_path`

## CLI Usage

```bash
python -m data_pipeline_engine \
  configs/examples/sample.csv \
  --validation-config configs/examples/validation.yaml \
  --transformation-config configs/examples/transformation.yaml \
  --inspection-config configs/examples/inspection.yaml \
  --cache-size 3
```

Optional baseline file for inspection reference mode:

```bash
python -m data_pipeline_engine \
  configs/examples/sample.csv \
  --inspection-config configs/examples/inspection.yaml \
  --baseline-file path/to/reference.csv
```

## `run_pipeline` Contract

Arguments:
- `csv_path`: Input CSV file path.
- `validation_config_path`: Optional validation config YAML path.
- `transformation_config_path`: Optional transformation config YAML path.
- `inspection_config_path`: Optional inspection config YAML path.
- `baseline_file_path`: Optional baseline CSV path (used by inspection when baseline source is `reference_dataset`).
- `cache_size`: Number of cached output snapshots retained per source CSV.

Returns a dictionary with:
- `status`: `"success"` on successful execution.
- `rows`: Output row count.
- `columns`: Output column names.
- `validation_applied`: Whether validation config was provided.
- `transformation_applied`: Whether transformation config was provided.
- `inspection_applied`: Whether inspection config was provided.
- `inspection_metrics`: Inspection stage metrics payload.
- `cached_output_path`: Path to written cache snapshot.

## Configuration Examples

- Validation: `configs/examples/validation.yaml`
- Transformation: `configs/examples/transformation.yaml`
- Inspection: `configs/examples/inspection.yaml`

## Error Model

The engine raises:
- `PipelineExecutionError` for stage/config execution failures.
- `FileNotFoundError` when `csv_path` does not exist.
- `ValueError` for invalid processing conditions surfaced by stages.
