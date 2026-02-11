import tempfile
from pathlib import Path

import pytest

from scdm_prepare.ingest import discover_subsamples, source_file_path
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
