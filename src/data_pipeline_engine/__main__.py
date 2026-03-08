from __future__ import annotations

import argparse

from data_pipeline_engine.engine import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run CSV data pipeline engine")
    parser.add_argument("csv_path", help="Path to input CSV file")
    parser.add_argument("--validation-config", dest="validation_config_path")
    parser.add_argument("--transformation-config", dest="transformation_config_path")
    parser.add_argument("--skew-config", dest="skew_config_path")

    args = parser.parse_args()
    if (
        args.validation_config_path is None
        and args.transformation_config_path is None
        and args.skew_config_path is None
    ):
        parser.error(
            "At least one of --validation-config, --transformation-config, "
            "or --skew-config must be provided."
        )

    result = run_pipeline(
        csv_path=args.csv_path,
        validation_config_path=args.validation_config_path,
        transformation_config_path=args.transformation_config_path,
        skew_config_path=args.skew_config_path,
    )
    print(result)


if __name__ == "__main__":
    main()
