"""CLI entry point for scdm-prepare."""

import shutil
from enum import Enum
from pathlib import Path

import duckdb
import typer

from scdm_prepare.ingest import discover_subsamples, ingest_all
from scdm_prepare.progress import PipelineProgress
from scdm_prepare.schema import TABLES
from scdm_prepare.transform import assemble_tables, build_crosswalks
from scdm_prepare.export import export_all

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
    input_dir: Path | None = typer.Option(
        None,
        "--input",
        help="Directory containing SynPUF SAS7BDAT subsample files.",
        exists=False,
        file_okay=False,
        resolve_path=True,
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output",
        help="Directory for output files. Created if it does not exist.",
        resolve_path=True,
    ),
    fmt: OutputFormat | None = typer.Option(
        None,
        "--format",
        help="Output format.",
    ),
    first: int | None = typer.Option(
        None,
        "--first",
        help="First subsample number to process. Omit to start from the lowest detected.",
    ),
    last: int | None = typer.Option(
        None,
        "--last",
        help="Last subsample number to process. Omit to process through the highest detected.",
    ),
    clean_temp: bool = typer.Option(
        False,
        "--clean-temp",
        help="Remove leftover temp files and exit.",
    ),
    file_ext: str = typer.Option(
        ".sas7bdat",
        "--file-ext",
        hidden=True,
        help="File extension for input files (hidden option for testing).",
    ),
) -> None:
    """Combine SynPUF subsamples into 9 standardised SCDM tables."""
    temp_dir = output_dir / "_temp"

    # Handle --clean-temp
    if clean_temp:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            typer.echo(f"Cleaned temp directory: {temp_dir}")
        else:
            typer.echo("No temp directory to clean.")
        raise typer.Exit()

    # Validate required arguments for normal operation
    if input_dir is None:
        typer.echo("Error: --input is required", err=True)
        raise typer.Exit(code=1)
    if fmt is None:
        typer.echo("Error: --format is required", err=True)
        raise typer.Exit(code=1)

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

    progress = PipelineProgress()

    try:
        # 1. Discover subsamples
        subsamples = discover_subsamples(input_dir, first, last, file_ext)
        typer.echo(f"Found subsamples: {subsamples}")

        # 2. Ingest (with per-table progress)
        with progress.ingestion_tracker(total_files=len(TABLES)) as tracker:
            ingest_all(input_dir, subsamples, output_dir, file_ext, progress=tracker)

        # 3. Transform (with per-table progress)
        con = duckdb.connect()
        try:
            build_crosswalks(con, str(temp_dir))
            with progress.transform_tracker(total_tables=len(TABLES)) as tracker:
                assemble_tables(con, str(temp_dir), progress=tracker)

            # 4. Export (with per-table progress)
            with progress.export_tracker(total_tables=len(TABLES)) as tracker:
                export_all(con, list(TABLES.keys()), str(output_dir), fmt.value, progress=tracker)
        finally:
            con.close()

        # 5. Cleanup temp on success
        shutil.rmtree(temp_dir)
        typer.echo("Done. Temp files cleaned up.")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        typer.echo(f"Temp files preserved at: {temp_dir}", err=True)
        raise typer.Exit(code=1)
