"""CLI entry point for scdm-prepare."""

from enum import Enum
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(
    name="scdm-prepare",
    help="Combine SynPUF subsamples into standardised SCDM tables.",
)


class OutputFormat(str, Enum):
    parquet = "parquet"
    csv = "csv"
    json = "json"


@app.command()
def main(
    input_dir: Path = typer.Option(
        ...,
        "--input",
        help="Directory containing SynPUF SAS7BDAT subsample files.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output",
        help="Directory for output files. Created if it does not exist.",
        resolve_path=True,
    ),
    fmt: OutputFormat = typer.Option(
        ...,
        "--format",
        help="Output format.",
    ),
    first: Optional[int] = typer.Option(
        None,
        "--first",
        help="First subsample number to process. Omit to start from the lowest detected.",
    ),
    last: Optional[int] = typer.Option(
        None,
        "--last",
        help="Last subsample number to process. Omit to process through the highest detected.",
    ),
) -> None:
    """Combine SynPUF subsamples into 9 standardised SCDM tables."""
    if not input_dir.is_dir():
        typer.echo(f"Error: Input directory does not exist: {input_dir}", err=True)
        raise typer.Exit(code=1)

    output_dir.mkdir(parents=True, exist_ok=True)

    typer.echo(f"Input:  {input_dir}")
    typer.echo(f"Output: {output_dir}")
    typer.echo(f"Format: {fmt.value}")
    if first is not None:
        typer.echo(f"First subsample: {first}")
    if last is not None:
        typer.echo(f"Last subsample:  {last}")
