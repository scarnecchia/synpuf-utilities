import re
from pathlib import Path

import polars as pl
import pyreadstat

from scdm_prepare.schema import TABLES


def source_file_path(
    input_dir: Path | str,
    table_name: str,
    samplenum: int,
    file_ext: str = ".sas7bdat",
) -> Path:
    """Return the expected path for a given table/subsample combination.

    Args:
        input_dir: Directory containing source files
        table_name: Name of the table (e.g., "enrollment")
        samplenum: Subsample number
        file_ext: File extension (default: ".sas7bdat")

    Returns:
        Path object for the expected source file
    """
    input_dir = Path(input_dir)
    return input_dir / f"{table_name}_{samplenum}{file_ext}"


def discover_subsamples(
    input_dir: Path | str,
    first: int | None = None,
    last: int | None = None,
    file_ext: str = ".sas7bdat",
) -> list[int]:
    """Discover subsample numbers from source files in input directory.

    Scans input_dir for files matching *_{N}{file_ext} pattern, extracts
    subsample numbers, applies first/last filtering, and validates that
    all 9 table types exist for each subsample in range.

    Args:
        input_dir: Directory containing source files
        first: First subsample number to process (None = lowest detected)
        last: Last subsample number to process (None = highest detected)
        file_ext: File extension to match (default: ".sas7bdat")

    Returns:
        List of validated subsample numbers in ascending order

    Raises:
        ValueError: If no files found, if range has missing files,
                   or if subsamples contain missing table types
    """
    input_dir = Path(input_dir)

    # Scan for files matching pattern *_{N}{file_ext}
    pattern = re.compile(rf"^(.+)_(\d+){re.escape(file_ext)}$")
    found_files = {}

    for file_path in input_dir.glob(f"*{file_ext}"):
        match = pattern.match(file_path.name)
        if match:
            table_type = match.group(1)
            samplenum = int(match.group(2))

            if samplenum not in found_files:
                found_files[samplenum] = set()
            found_files[samplenum].add(table_type)

    # AC1.5: Empty directory or no matching files
    if not found_files:
        raise ValueError(
            f"No files matching pattern '*_<N>{file_ext}' found in {input_dir}"
        )

    # Determine range
    all_subsamples = sorted(found_files.keys())
    start = first if first is not None else all_subsamples[0]
    end = last if last is not None else all_subsamples[-1]

    # Collect validated subsamples and check for missing files
    validated = []
    missing_files = []

    for samplenum in range(start, end + 1):
        # Get all 9 table types from schema
        all_table_types = set(TABLES.keys())

        # Check what we have for this subsample
        have_tables = found_files.get(samplenum, set())
        missing_tables = all_table_types - have_tables

        # AC1.4: Missing file within range
        if missing_tables:
            for table_type in sorted(missing_tables):
                missing_files.append(source_file_path(input_dir, table_type, samplenum, file_ext))
        else:
            validated.append(samplenum)

    # Raise if any missing files
    if missing_files:
        raise ValueError(
            f"Missing files for subsamples in range [{start}, {end}]:\n"
            + "\n".join(str(f) for f in missing_files)
        )

    return validated


def ingest_table(
    input_dir: Path | str,
    table_name: str,
    subsamples: list[int],
    output_dir: Path | str,
    file_ext: str = ".sas7bdat",
    chunk_size: int = 10000,
) -> None:
    """Read source file in chunks and write temp parquet with samplenum column.

    For SAS7BDAT files: uses pyreadstat chunked reading which auto-converts
    SAS date columns to Python datetime.date. For parquet test files: reads
    entire file at once (no chunking needed for small test files).

    Args:
        input_dir: Directory containing source files
        table_name: Name of the table (e.g., "enrollment")
        subsamples: List of subsample numbers to process
        output_dir: Directory where temp parquet files will be written
        file_ext: File extension (default: ".sas7bdat")
        chunk_size: Chunk size for SAS7BDAT reading (default: 10000)

    Raises:
        ValueError: If source file not found or if writing fails
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    temp_dir = output_dir / "_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    for samplenum in subsamples:
        source_path = source_file_path(input_dir, table_name, samplenum, file_ext)

        if not source_path.exists():
            raise ValueError(f"Source file not found: {source_path}")

        # Read based on file extension
        if file_ext == ".parquet":
            # For parquet: read entire file and inject samplenum
            df = pl.read_parquet(str(source_path))
            df = df.with_columns(pl.lit(samplenum).alias("samplenum"))
        else:
            # For SAS7BDAT: read in chunks
            chunks = []
            for chunk_df in pyreadstat.read_file_in_chunks(
                pyreadstat.read_sas7bdat, str(source_path), chunksize=chunk_size
            ):
                # chunk_df is a pandas DataFrame
                # Convert to polars and inject samplenum
                chunk_pl = pl.from_pandas(chunk_df)
                chunk_pl = chunk_pl.with_columns(pl.lit(samplenum).alias("samplenum"))
                chunks.append(chunk_pl)

            # Concatenate all chunks
            if chunks:
                df = pl.concat(chunks)
            else:
                # Empty file - create empty dataframe with correct schema
                df = pl.DataFrame({col: [] for col in TABLES[table_name].columns})
                df = df.with_columns(pl.lit(samplenum).alias("samplenum"))

        # Write to temp parquet
        output_path = temp_dir / f"{table_name}_{samplenum}.parquet"
        df.write_parquet(str(output_path))


def ingest_all(
    input_dir: Path | str,
    subsamples: list[int],
    output_dir: Path | str,
    file_ext: str = ".sas7bdat",
    chunk_size: int = 10000,
) -> None:
    """Ingest all 9 table types for given subsamples to temp parquet.

    Args:
        input_dir: Directory containing source files
        subsamples: List of subsample numbers to process
        output_dir: Directory where temp parquet files will be written
        file_ext: File extension (default: ".sas7bdat")
        chunk_size: Chunk size for SAS7BDAT reading (default: 10000)
    """
    for table_name in TABLES.keys():
        ingest_table(input_dir, table_name, subsamples, output_dir, file_ext, chunk_size)
