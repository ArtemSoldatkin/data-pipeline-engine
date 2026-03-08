# Data Pipeline Engine

A minimal Python scaffold for a CSV data pipeline engine that loads YAML-driven rules for:
- Validation
- Transformation
- Inspection checks

## Quick start

```bash
pip install -e .
```

## Entrypoint

```python
from data_pipeline_engine.engine import run_pipeline

result = run_pipeline(
    csv_path="/path/to/data.csv",
    validation_config_path="configs/examples/validation.yaml",
    transformation_config_path=None,
    inspection_config_path=None,
)
print(result)
```

At least one config path must be provided.
