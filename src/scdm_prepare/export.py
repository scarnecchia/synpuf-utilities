"""Export assembled SCDM tables from DuckDB to parquet, CSV, or NDJSON format."""

from pathlib import Path

import duckdb
import polars as pl


def export_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    output_dir: str | Path,
    fmt: str,
) -> None:
    """Export a single DuckDB table to the specified format.

    Args:
        con: DuckDB connection
        table_name: Name of the table to export
        output_dir: Output directory path
        fmt: Output format ("parquet", "csv", or "json")

    Raises:
        ValueError: If format is not supported
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if fmt == "parquet":
        _export_parquet(con, table_name, output_dir)
    elif fmt == "csv":
        _export_csv(con, table_name, output_dir)
    elif fmt == "json":
        _export_ndjson(con, table_name, output_dir)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def _export_parquet(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    output_dir: Path,
) -> None:
    """Export table to parquet format with zstd compression."""
    output_path = output_dir / f"{table_name}.parquet"
    con.execute(f"""
        COPY {table_name}
        TO '{output_path}'
        (FORMAT parquet, COMPRESSION zstd)
    """)


def _export_csv(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    output_dir: Path,
) -> None:
    """Export table to CSV format with headers."""
    output_path = output_dir / f"{table_name}.csv"
    con.execute(f"""
        COPY {table_name}
        TO '{output_path}'
        (FORMAT csv, HEADER true)
    """)


def _export_ndjson(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    output_dir: Path,
    batch_size: int = 100_000,
) -> None:
    """Export table to NDJSON format using batched reading.

    polars write_ndjson() does not support append mode, so we use DuckDB's
    fetch_arrow_reader() to stream data in batches and write manually.

    Args:
        con: DuckDB connection
        table_name: Name of the table to export
        output_dir: Output directory path
        batch_size: Number of rows per batch
    """
    output_path = output_dir / f"{table_name}.json"

    result = con.execute(f"SELECT * FROM {table_name}")
    reader = result.fetch_arrow_reader(batch_size=batch_size)

    with open(output_path, "w") as f:
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            df = pl.from_arrow(batch)
            f.write(df.write_ndjson())


def export_all(
    con: duckdb.DuckDBPyConnection,
    table_names: list[str],
    output_dir: str | Path,
    fmt: str,
) -> None:
    """Export multiple tables to the specified format.

    Args:
        con: DuckDB connection
        table_names: List of table names to export
        output_dir: Output directory path
        fmt: Output format ("parquet", "csv", or "json")
    """
    for table_name in table_names:
        export_table(con, table_name, output_dir, fmt)
