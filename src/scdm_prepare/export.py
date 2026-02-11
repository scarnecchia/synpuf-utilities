"""Export assembled SCDM tables from DuckDB to parquet, CSV, or NDJSON format."""

from pathlib import Path

import duckdb


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
) -> None:
    """Export table to NDJSON format using DuckDB COPY TO.

    DuckDB's COPY TO with FORMAT json produces NDJSON (newline-delimited JSON)
    format where each line is a valid JSON object.

    Args:
        con: DuckDB connection
        table_name: Name of the table to export
        output_dir: Output directory path
    """
    output_path = output_dir / f"{table_name}.json"
    con.execute(f"""
        COPY {table_name}
        TO '{output_path}'
        (FORMAT json)
    """)


def export_all(
    con: duckdb.DuckDBPyConnection,
    table_names: list[str],
    output_dir: str | Path,
    fmt: str,
    progress=None,
) -> None:
    """Export multiple tables to the specified format.

    Args:
        con: DuckDB connection
        table_names: List of table names to export
        output_dir: Output directory path
        fmt: Output format ("parquet", "csv", or "json")
        progress: Optional progress tracker with update_description() and advance()
    """
    for table_name in table_names:
        if progress:
            progress.update_description(f"Exporting {table_name}")
        export_table(con, table_name, output_dir, fmt)
        if progress:
            progress.advance()
