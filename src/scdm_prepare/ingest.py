import re
from pathlib import Path
from typing import Optional

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
    first: Optional[int] = None,
    last: Optional[int] = None,
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
            f"No files matching pattern '*_{{}}{file_ext}' found in {input_dir}"
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
