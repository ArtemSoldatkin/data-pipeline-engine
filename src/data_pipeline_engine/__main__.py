from __future__ import annotations

from pathlib import Path

import typer

from data_pipeline_engine.engine import run_pipeline

app = typer.Typer(
    help="Run CSV data pipeline engine",
    add_completion=False,
)


@app.command()
def main(
    csv_path: Path = typer.Argument(..., help="Path to input CSV file"),
    validation_config_path: Path | None = typer.Option(None, "--validation-config"),
    transformation_config_path: Path | None = typer.Option(None, "--transformation-config"),
    inspection_config_path: Path | None = typer.Option(None, "--inspection-config"),
    baseline_file_path: Path | None = typer.Option(
        None,
        "--baseline-file",
        help="Path to baseline CSV used when inspection baseline.source=reference_dataset",
    ),
    cache_size: int = typer.Option(1, "--cache-size"),
) -> None:
    if (
        validation_config_path is None
        and transformation_config_path is None
        and inspection_config_path is None
    ):
        typer.echo(
            "At least one of --validation-config, --transformation-config, "
            "or --inspection-config must be provided.",
            err=True,
        )
        raise typer.Exit(code=2)

    result = run_pipeline(
        csv_path=csv_path,
        validation_config_path=validation_config_path,
        transformation_config_path=transformation_config_path,
        inspection_config_path=inspection_config_path,
        baseline_file_path=baseline_file_path,
        cache_size=cache_size,
    )
    typer.echo(str(result))


if __name__ == "__main__":
    app()
