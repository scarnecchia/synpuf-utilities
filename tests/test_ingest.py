import tempfile
from pathlib import Path

import polars as pl
import pytest

from scdm_prepare.ingest import discover_subsamples, ingest_all, ingest_table, source_file_path
from scdm_prepare.schema import TABLES


class TestSourceFilePath:
    """Tests for source_file_path helper."""

    def test_returns_correct_path(self):
        """Verify source_file_path returns expected path format."""
        path = source_file_path("/data", "enrollment", 5)
        assert str(path) == "/data/enrollment_5.sas7bdat"

    def test_custom_extension(self):
        """Verify custom file extension is used."""
        path = source_file_path("/data", "demographic", 3, file_ext=".parquet")
        assert str(path) == "/data/demographic_3.parquet"

    def test_pathlib_input(self):
        """Verify pathlib.Path input works."""
        path = source_file_path(Path("/data"), "encounter", 7)
        assert path == Path("/data/encounter_7.sas7bdat")


class TestDiscoverSubsamplesSuccess:
    """AC1.1, AC1.2, AC1.3: Successful subsample discovery tests."""

    def test_ac11_discover_all_subsamples(self):
        """AC1.1: Auto-detect all subsample numbers from filenames."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create files for subsamples 1, 2, 3
            for samplenum in [1, 2, 3]:
                for table_name in TABLES.keys():
                    (tmpdir / f"{table_name}_{samplenum}.parquet").touch()

            result = discover_subsamples(tmpdir, file_ext=".parquet")
            assert result == [1, 2, 3]

    def test_ac12_range_filtering_first_and_last(self):
        """AC1.2: --first 5 --last 10 processes only subsamples 5-10."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create files for subsamples 1-10
            for samplenum in range(1, 11):
                for table_name in TABLES.keys():
                    (tmpdir / f"{table_name}_{samplenum}.parquet").touch()

            result = discover_subsamples(tmpdir, first=5, last=10, file_ext=".parquet")
            assert result == [5, 6, 7, 8, 9, 10]

    def test_ac13_omit_last_processes_through_highest(self):
        """AC1.3: Omitting --last processes from --first through highest detected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create files for subsamples 1-5
            for samplenum in range(1, 6):
                for table_name in TABLES.keys():
                    (tmpdir / f"{table_name}_{samplenum}.parquet").touch()

            # Ask for first=2, no last
            result = discover_subsamples(tmpdir, first=2, last=None, file_ext=".parquet")
            assert result == [2, 3, 4, 5]

    def test_omit_first_processes_from_lowest(self):
        """Variant: Omitting --first processes from lowest detected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create files for subsamples 3-7
            for samplenum in range(3, 8):
                for table_name in TABLES.keys():
                    (tmpdir / f"{table_name}_{samplenum}.parquet").touch()

            # Ask for last=5, no first
            result = discover_subsamples(tmpdir, first=None, last=5, file_ext=".parquet")
            assert result == [3, 4, 5]

    def test_neither_first_nor_last_uses_detected_range(self):
        """When neither first nor last provided, use detected range."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create files for subsamples 2-5
            for samplenum in [2, 3, 4, 5]:
                for table_name in TABLES.keys():
                    (tmpdir / f"{table_name}_{samplenum}.parquet").touch()

            result = discover_subsamples(tmpdir, file_ext=".parquet")
            assert result == [2, 3, 4, 5]


class TestDiscoverSubsamplesFailure:
    """AC1.4, AC1.5: Error handling tests."""

    def test_ac14_missing_file_raises_with_list(self):
        """AC1.4: Missing file within range raises with listing of all missing files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create files for subsamples 1-3 but remove demographic_2
            for samplenum in [1, 2, 3]:
                for table_name in TABLES.keys():
                    (tmpdir / f"{table_name}_{samplenum}.parquet").touch()

            # Remove one file
            (tmpdir / "demographic_2.parquet").unlink()

            with pytest.raises(ValueError) as exc_info:
                discover_subsamples(tmpdir, file_ext=".parquet")

            error_msg = str(exc_info.value)
            assert "Missing files" in error_msg
            assert "demographic_2.parquet" in error_msg

    def test_ac14_multiple_missing_files_all_listed(self):
        """AC1.4: Multiple missing files are all listed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create files for subsamples 1-3
            for samplenum in [1, 2, 3]:
                for table_name in TABLES.keys():
                    (tmpdir / f"{table_name}_{samplenum}.parquet").touch()

            # Remove multiple files
            (tmpdir / "demographic_2.parquet").unlink()
            (tmpdir / "encounter_3.parquet").unlink()

            with pytest.raises(ValueError) as exc_info:
                discover_subsamples(tmpdir, file_ext=".parquet")

            error_msg = str(exc_info.value)
            assert "demographic_2.parquet" in error_msg
            assert "encounter_3.parquet" in error_msg

    def test_ac15_empty_directory_raises(self):
        """AC1.5: Empty directory raises clear error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError) as exc_info:
                discover_subsamples(tmpdir, file_ext=".parquet")

            error_msg = str(exc_info.value)
            assert "No files matching pattern" in error_msg or "found in" in error_msg

    def test_ac15_no_matching_files_raises(self):
        """AC1.5: No matching files raises clear error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create files with wrong extension
            (tmpdir / "enrollment_1.txt").touch()

            with pytest.raises(ValueError) as exc_info:
                discover_subsamples(tmpdir, file_ext=".parquet")

            error_msg = str(exc_info.value)
            assert "No files matching pattern" in error_msg or "found in" in error_msg

    def test_discovers_9_tables_correctly(self):
        """Verify that all 9 tables are required for discovery."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create only 8 of 9 tables for subsample 1
            all_tables = list(TABLES.keys())
            for table_name in all_tables[:-1]:
                (tmpdir / f"{table_name}_1.parquet").touch()

            with pytest.raises(ValueError) as exc_info:
                discover_subsamples(tmpdir, file_ext=".parquet")

            error_msg = str(exc_info.value)
            assert "Missing files" in error_msg


class TestIngestTable:
    """AC2.1, AC2.2, AC2.3: Ingestion tests."""

    def test_ac21_samplenum_column_injected(self):
        """AC2.1: samplenum column is injected into output parquet."""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                input_dir = Path(input_dir)
                output_dir = Path(output_dir)

                # Create test parquet file
                table_def = TABLES["enrollment"]
                test_data = {col: [1, 2, 3] for col in table_def.columns}
                df = pl.DataFrame(test_data)
                df.write_parquet(str(input_dir / "enrollment_5.parquet"))

                # Ingest
                ingest_table(input_dir, "enrollment", [5], output_dir, file_ext=".parquet")

                # Check output
                output_path = output_dir / "_temp" / "enrollment_5.parquet"
                assert output_path.exists()

                result_df = pl.read_parquet(str(output_path))
                assert "samplenum" in result_df.columns
                assert result_df["samplenum"][0] == 5

    def test_ac21_samplenum_value_correct_for_each_subsample(self):
        """AC2.1: samplenum has correct value for each subsample."""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                input_dir = Path(input_dir)
                output_dir = Path(output_dir)

                # Create test parquet files for multiple subsamples
                table_def = TABLES["enrollment"]
                for samplenum in [1, 2, 3]:
                    test_data = {col: [samplenum] for col in table_def.columns}
                    df = pl.DataFrame(test_data)
                    df.write_parquet(str(input_dir / f"enrollment_{samplenum}.parquet"))

                # Ingest all
                ingest_table(input_dir, "enrollment", [1, 2, 3], output_dir, file_ext=".parquet")

                # Check each output
                for samplenum in [1, 2, 3]:
                    output_path = output_dir / "_temp" / f"enrollment_{samplenum}.parquet"
                    result_df = pl.read_parquet(str(output_path))
                    assert result_df["samplenum"][0] == samplenum

    def test_ac22_all_9_tables_ingested(self):
        """AC2.2: All 9 table types produce temp parquet files."""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                input_dir = Path(input_dir)
                output_dir = Path(output_dir)

                # Create test parquet files for all tables
                for table_name, table_def in TABLES.items():
                    test_data = {col: [1] for col in table_def.columns}
                    df = pl.DataFrame(test_data)
                    df.write_parquet(str(input_dir / f"{table_name}_1.parquet"))

                # Ingest all
                ingest_all(input_dir, [1], output_dir, file_ext=".parquet")

                # Check all 9 files exist
                temp_dir = output_dir / "_temp"
                for table_name in TABLES.keys():
                    output_path = temp_dir / f"{table_name}_1.parquet"
                    assert output_path.exists(), f"Missing {output_path}"

    def test_ac23_date_columns_preserved_as_date_type(self):
        """AC2.3: Date columns in output parquet are date type (not int64)."""
        import datetime
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                input_dir = Path(input_dir)
                output_dir = Path(output_dir)

                # Create test parquet with date columns
                table_def = TABLES["demographic"]
                test_data = {
                    "PatID": [1, 2],
                    "Birth_Date": [datetime.date(1990, 1, 15), datetime.date(1985, 3, 20)],
                    "Sex": ["M", "F"],
                    "Hispanic": ["Y", "N"],
                    "Race": ["W", "B"],
                    "PostalCode": ["12345", "54321"],
                    "PostalCode_Date": [datetime.date(2020, 1, 1), datetime.date(2020, 2, 1)],
                    "ImputedRace": ["N", "Y"],
                    "ImputedHispanic": ["N", "N"],
                }
                df = pl.DataFrame(test_data)
                df.write_parquet(str(input_dir / "demographic_1.parquet"))

                # Ingest
                ingest_table(input_dir, "demographic", [1], output_dir, file_ext=".parquet")

                # Check output schema
                output_path = output_dir / "_temp" / "demographic_1.parquet"
                result_df = pl.read_parquet(str(output_path))

                # Check date columns are Date type
                assert result_df.schema["Birth_Date"] == pl.Date
                assert result_df.schema["PostalCode_Date"] == pl.Date

    def test_temp_dir_created_automatically(self):
        """Temp directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                input_dir = Path(input_dir)
                output_dir = Path(output_dir)

                # Create test parquet
                table_def = TABLES["enrollment"]
                test_data = {col: [1] for col in table_def.columns}
                df = pl.DataFrame(test_data)
                df.write_parquet(str(input_dir / "enrollment_1.parquet"))

                # Ingest
                temp_dir = output_dir / "_temp"
                assert not temp_dir.exists()

                ingest_table(input_dir, "enrollment", [1], output_dir, file_ext=".parquet")

                # Temp dir should be created
                assert temp_dir.exists()
                assert (temp_dir / "enrollment_1.parquet").exists()

    def test_missing_source_file_raises(self):
        """Raises ValueError if source file not found."""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                input_dir = Path(input_dir)
                output_dir = Path(output_dir)

                with pytest.raises(ValueError) as exc_info:
                    ingest_table(input_dir, "enrollment", [999], output_dir, file_ext=".parquet")

                assert "Source file not found" in str(exc_info.value)
