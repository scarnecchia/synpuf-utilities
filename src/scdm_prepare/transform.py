from pathlib import Path

import duckdb
import polars as pl

from scdm_prepare.schema import CROSSWALKS


def build_crosswalks(con: duckdb.DuckDBPyConnection, temp_dir: Path | str) -> None:
    """Build crosswalk tables for PatID, EncounterID, ProviderID, and FacilityID.

    For each crosswalk defined in CROSSWALKS:
    1. Extract distinct (original_id, samplenum) pairs from source table(s)
    2. Filter out NULL original IDs
    3. Assign sequential new IDs via ROW_NUMBER() ordered by samplenum and original_id
    4. Create the crosswalk as a DuckDB table

    NULL original IDs are excluded. When tables LEFT JOIN on the crosswalk,
    NULL originals produce NULL new IDs naturally (absent from crosswalk).

    Same original ID in different subsamples gets different ROW_NUMBER values
    because samplenum is part of the DISTINCT key.

    Args:
        con: DuckDB connection
        temp_dir: Directory containing ingested parquet files (output of Phase 2)
    """
    temp_dir = Path(temp_dir)

    for crosswalk_name, crosswalk_def in CROSSWALKS.items():
        id_column = crosswalk_def.id_column
        source_tables = crosswalk_def.source_tables
        table_name = crosswalk_def.crosswalk_name

        # Build glob pattern for source table parquet files
        source_table = source_tables[0]
        glob_pattern = str(temp_dir / f"{source_table}_*.parquet")

        # Build SQL to extract distinct IDs, filter NULLs, and assign sequential IDs
        sql = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            orig_{id_column},
            samplenum,
            ROW_NUMBER() OVER (ORDER BY samplenum, orig_{id_column}) AS {id_column}
        FROM (
            SELECT DISTINCT
                {id_column} AS orig_{id_column},
                samplenum
            FROM read_parquet('{glob_pattern}')
            WHERE {id_column} IS NOT NULL
        )
        """

        con.execute(sql)


def get_crosswalk(
    con: duckdb.DuckDBPyConnection, crosswalk_name: str
) -> pl.DataFrame:
    """Retrieve a crosswalk table as a polars DataFrame.

    Args:
        con: DuckDB connection
        crosswalk_name: Name of the crosswalk table in DuckDB

    Returns:
        Polars DataFrame containing the crosswalk
    """
    result = con.execute(f"SELECT * FROM {crosswalk_name}")
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    return pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
