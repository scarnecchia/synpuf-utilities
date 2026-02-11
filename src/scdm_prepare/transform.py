from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

from scdm_prepare.progress import ProgressTracker
from scdm_prepare.schema import CROSSWALKS, TABLES


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

    Raises:
        ValueError: If crosswalk_name is not a known crosswalk
    """
    valid_names = {cw.crosswalk_name for cw in CROSSWALKS.values()}
    if crosswalk_name not in valid_names:
        raise ValueError(f"unknown crosswalk: {crosswalk_name}")

    return con.sql(f"SELECT * FROM {crosswalk_name}").pl()


def assemble_tables(con: duckdb.DuckDBPyConnection, temp_dir: Path | str, progress: Optional[ProgressTracker] = None) -> None:
    """Assemble all 9 SCDM output tables from ingested data and crosswalks.

    For each of the 7 data-derived tables (enrollment, demographic, dispensing,
    encounter, diagnosis, procedure, death):
    1. SELECT specified columns from source data and crosswalks
    2. INNER JOIN patid_crosswalk (required for all tables)
    3. LEFT JOIN other crosswalks as needed (EncounterID, ProviderID, FacilityID)
    4. ORDER BY the table's sort keys

    Also synthesises Provider and Facility tables by calling synthesise_tables()
    internally, which derives them from the providerid_crosswalk and facilityid_crosswalk.

    Args:
        con: DuckDB connection
        temp_dir: Directory containing ingested parquet files
        progress: Optional progress tracker with update_description() and advance()
    """
    temp_dir = Path(temp_dir)

    # Define data-derived tables (exclude provider and facility which are synthesised)
    data_derived_tables = {
        name: table_def
        for name, table_def in TABLES.items()
        if name not in ("provider", "facility")
    }

    for table_name, table_def in data_derived_tables.items():
        if progress:
            progress.update_description(f"Transforming {table_name}")
        # Check if the source table exists
        matching_files = list(Path(temp_dir).glob(f"{table_name}_*.parquet"))
        if not matching_files:
            # Skip this table if no source files exist
            continue
        # Build the SELECT clause with proper column selections
        select_parts = []
        join_clauses = []
        join_aliases = {}

        # Track which alias to use for each crosswalk
        alias_counter = {"b": ord("b")}

        for col in table_def.columns:
            if col in table_def.crosswalk_ids:
                # This column comes from a crosswalk
                crosswalk_name = _get_crosswalk_name(col)
                alias = _get_or_create_alias(join_aliases, crosswalk_name, alias_counter)
                select_parts.append(f"{alias}.{col}")
            else:
                # This column comes from the source data
                select_parts.append(f"a.{col}")

        select_clause = ", ".join(select_parts)

        # Build JOIN clauses based on crosswalk_ids
        for id_col, join_type in table_def.crosswalk_ids.items():
            crosswalk_name = _get_crosswalk_name(id_col)
            alias = _get_or_create_alias(join_aliases, crosswalk_name, alias_counter)

            # For source data, we need to determine the original column name
            orig_col = f"a.{id_col}"

            if join_type.upper() == "INNER":
                join_clauses.append(
                    f"INNER JOIN {crosswalk_name} AS {alias}\n"
                    f"  ON {orig_col} = {alias}.orig_{id_col} AND a.samplenum = {alias}.samplenum"
                )
            else:  # LEFT
                join_clauses.append(
                    f"LEFT JOIN {crosswalk_name} AS {alias}\n"
                    f"  ON {orig_col} = {alias}.orig_{id_col} AND a.samplenum = {alias}.samplenum"
                )

        # Build FROM clause with glob pattern
        glob_pattern = str(temp_dir / f"{table_name}_*.parquet")
        from_clause = f"read_parquet('{glob_pattern}') AS a"

        # Build ORDER BY clause
        order_parts = []
        for sort_key in table_def.sort_keys:
            if sort_key in table_def.crosswalk_ids:
                # Sort key comes from a crosswalk
                crosswalk_name = _get_crosswalk_name(sort_key)
                alias = join_aliases.get(crosswalk_name, "")
                if alias:
                    order_parts.append(f"{alias}.{sort_key}")
                else:
                    order_parts.append(f"{sort_key}")
            else:
                # Sort key comes from source data
                order_parts.append(f"a.{sort_key}")

        order_by_clause = ", ".join(order_parts)

        # Build final SQL
        sql = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT {select_clause}
        FROM {from_clause}
        {chr(10).join(join_clauses)}
        ORDER BY {order_by_clause}
        """

        con.execute(sql)
        if progress:
            progress.advance()

    # Synthesise provider and facility tables (always done, progress handled above)
    synthesise_tables(con)


def synthesise_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Synthesise Provider and Facility tables from crosswalks.

    Provider table:
    - Columns: ProviderID, Specialty, Specialty_CodeType
    - Built from providerid_crosswalk
    - Hardcoded: Specialty='99', Specialty_CodeType='2'
    - Excludes rows where original ProviderID was NULL

    Facility table:
    - Columns: FacilityID, Facility_Location
    - Built from facilityid_crosswalk
    - Empty Facility_Location string
    - Excludes rows where original FacilityID was NULL

    Args:
        con: DuckDB connection
    """
    # Provider table
    con.execute("""
        CREATE OR REPLACE TABLE provider AS
        SELECT
            ProviderID,
            '99' AS Specialty,
            '2' AS Specialty_CodeType
        FROM providerid_crosswalk
        WHERE orig_ProviderID IS NOT NULL
        ORDER BY ProviderID
    """)

    # Facility table
    con.execute("""
        CREATE OR REPLACE TABLE facility AS
        SELECT
            FacilityID,
            '' AS Facility_Location
        FROM facilityid_crosswalk
        WHERE orig_FacilityID IS NOT NULL
        ORDER BY FacilityID
    """)


def _get_crosswalk_name(id_column: str) -> str:
    """Map an ID column name to its corresponding crosswalk name.

    Args:
        id_column: Column name (e.g., "PatID", "EncounterID")

    Returns:
        Crosswalk table name (e.g., "patid_crosswalk", "encounterid_crosswalk")

    Raises:
        ValueError: If no crosswalk is defined for the given column
    """
    mapping = {cw.id_column: cw.crosswalk_name for cw in CROSSWALKS.values()}
    crosswalk_name = mapping.get(id_column)
    if crosswalk_name is None:
        raise ValueError(f"no crosswalk defined for column: {id_column}")
    return crosswalk_name


def _get_or_create_alias(
    join_aliases: dict[str, str], crosswalk_name: str, alias_counter: dict[str, int]
) -> str:
    """Get or create an alias for a crosswalk in JOIN clauses.

    Args:
        join_aliases: Dictionary mapping crosswalk names to aliases
        crosswalk_name: Name of the crosswalk table
        alias_counter: Counter for generating new aliases

    Returns:
        Single-character alias (b, c, d, etc.)
    """
    if crosswalk_name not in join_aliases:
        # Create new alias
        next_ord = alias_counter["b"]
        alias = chr(next_ord)
        join_aliases[crosswalk_name] = alias
        alias_counter["b"] = next_ord + 1

    return join_aliases[crosswalk_name]
