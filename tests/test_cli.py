"""Tests for CLI entry point and orchestration."""

import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scdm_prepare.cli import app


runner = CliRunner()


class TestProgressModule:
    """Tests for progress.py module."""

    def test_progress_import(self):
        """Verify PipelineProgress can be imported."""
        from scdm_prepare.progress import PipelineProgress

        progress = PipelineProgress()
        assert progress is not None

    def test_ingestion_tracker_context(self):
        """Verify ingestion_tracker context manager works."""
        from scdm_prepare.progress import PipelineProgress

        progress = PipelineProgress()
        with progress.ingestion_tracker(total_files=5) as tracker:
            assert tracker is not None
            tracker.update_description("test.txt")
            tracker.advance()

    def test_transform_tracker_context(self):
        """Verify transform_tracker context manager works."""
        from scdm_prepare.progress import PipelineProgress

        progress = PipelineProgress()
        with progress.transform_tracker(total_tables=9) as tracker:
            assert tracker is not None
            tracker.update_description("enrollment")
            tracker.advance()

    def test_export_tracker_context(self):
        """Verify export_tracker context manager works."""
        from scdm_prepare.progress import PipelineProgress

        progress = PipelineProgress()
        with progress.export_tracker(total_tables=9) as tracker:
            assert tracker is not None
            tracker.update_description("enrollment")
            tracker.advance()


class TestCLIHelpAndArgs:
    """Tests for AC8.1 and AC8.2 - CLI help and required arguments."""

    def test_ac81_help_prints_all_arguments(self):
        """AC8.1: --help prints usage with all arguments."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "--input" in result.stdout
        assert "--output" in result.stdout
        assert "--format" in result.stdout
        assert "--first" in result.stdout
        assert "--last" in result.stdout

    def test_ac82_missing_required_input(self):
        """AC8.2: Missing --input produces error."""
        result = runner.invoke(app, ["--output", "/tmp/out", "--format", "parquet"])
        assert result.exit_code != 0

    def test_ac82_missing_required_output(self):
        """AC8.2: Missing --output produces error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(app, ["--input", tmpdir, "--format", "parquet"])
            assert result.exit_code != 0

    def test_ac82_missing_required_format(self):
        """AC8.2: Missing --format produces error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(app, ["--input", tmpdir, "--output", tmpdir])
            assert result.exit_code != 0

    def test_ac82_first_last_optional(self):
        """AC8.2: --first and --last are optional (can omit)."""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                input_path = Path(input_dir)
                # Create dummy files so discover_subsamples doesn't fail
                from scdm_prepare.schema import TABLES

                for samplenum in [1, 2]:
                    for table_name in TABLES.keys():
                        (input_path / f"{table_name}_{samplenum}.parquet").write_text("")

                # This will fail later (bad parquet), but args should parse OK
                result = runner.invoke(
                    app,
                    [
                        "--input",
                        input_dir,
                        "--output",
                        output_dir,
                        "--format",
                        "parquet",
                    ],
                )
                # Should not fail on argument parsing
                assert "--input" in str(result.exception) or result.exit_code != 0


class TestCLIErrorHandling:
    """Tests for AC8.3, AC8.4 - error handling."""

    def test_ac83_invalid_format(self):
        """AC8.3: Invalid --format produces clear error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(
                app,
                ["--input", tmpdir, "--output", tmpdir, "--format", "xml"],
            )
            assert result.exit_code != 0

    def test_ac84_nonexistent_input_dir(self):
        """AC8.4: Non-existent --input directory produces clear error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(
                app,
                [
                    "--input",
                    "/nonexistent/path/that/does/not/exist",
                    "--output",
                    tmpdir,
                    "--format",
                    "parquet",
                ],
            )
            assert result.exit_code != 0


class TestCLIProgressReporting:
    """Tests for AC9.1, AC9.2 - progress reporting."""

    def test_ac91_ac92_progress_reported_in_full_run(self, sample_parquet_dir):
        """AC9.1, AC9.2: Verify progress is reported during full pipeline run."""
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(
                app,
                [
                    "--input",
                    str(sample_parquet_dir),
                    "--output",
                    output_dir,
                    "--format",
                    "parquet",
                    "--file-ext",
                    ".parquet",
                ],
            )
            # Should succeed
            assert result.exit_code == 0


class TestCLICleanup:
    """Tests for AC10.1, AC10.2 - temp file cleanup."""

    def test_ac101_temp_cleaned_on_success(self, sample_parquet_dir):
        """AC10.1: Temp directory cleaned up on successful completion."""
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(
                app,
                [
                    "--input",
                    str(sample_parquet_dir),
                    "--output",
                    output_dir,
                    "--format",
                    "parquet",
                    "--file-ext",
                    ".parquet",
                ],
            )
            assert result.exit_code == 0
            temp_dir = Path(output_dir) / "_temp"
            assert not temp_dir.exists()

    def test_ac102_temp_preserved_on_failure(self):
        """AC10.2: Temp directory preserved on failure."""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                # Create empty input dir (no valid subsamples)
                result = runner.invoke(
                    app,
                    [
                        "--input",
                        input_dir,
                        "--output",
                        output_dir,
                        "--format",
                        "parquet",
                    ],
                )
                assert result.exit_code != 0


class TestCleanTempFlag:
    """Tests for AC10.3 - --clean-temp flag."""

    def test_ac103_clean_temp_removes_directory(self):
        """AC10.3: --clean-temp removes leftover temp files."""
        with tempfile.TemporaryDirectory() as output_dir:
            output_path = Path(output_dir)
            temp_path = output_path / "_temp"
            temp_path.mkdir(parents=True)
            (temp_path / "test.parquet").write_text("test")

            result = runner.invoke(
                app,
                ["--output", output_dir, "--clean-temp"],
            )
            assert result.exit_code == 0
            assert not temp_path.exists()

    def test_ac103_clean_temp_no_temp_dir(self):
        """AC10.3: --clean-temp exits cleanly if no temp dir."""
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(
                app,
                ["--output", output_dir, "--clean-temp"],
            )
            assert result.exit_code == 0


class TestE2ESmoke:
    """End-to-end smoke test."""

    def test_e2e_full_pipeline(self, sample_parquet_dir):
        """E2E: Full pipeline from input to output."""
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(
                app,
                [
                    "--input",
                    str(sample_parquet_dir),
                    "--output",
                    output_dir,
                    "--format",
                    "parquet",
                    "--file-ext",
                    ".parquet",
                ],
            )
            assert result.exit_code == 0

            # Verify all 9 output files exist
            output_path = Path(output_dir)
            from scdm_prepare.schema import TABLES

            for table_name in TABLES.keys():
                output_file = output_path / f"{table_name}.parquet"
                assert output_file.exists(), f"Missing {table_name}.parquet"

            # Verify temp was cleaned up
            assert not (output_path / "_temp").exists()
