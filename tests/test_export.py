"""Tests for SCDM table export functionality."""

import datetime
import json
import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest

from scdm_prepare.export import export_all, export_table


@pytest.fixture
def duckdb_con():
    """Create an in-memory DuckDB connection."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


class TestExportParquet:
    """Tests for parquet export (AC7.1, AC7.4)."""

    def test_ac71_parquet_export_creates_readable_file(self, duckdb_con):
        """AC7.1: Exported parquet file is readable by polars."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create a simple table in DuckDB
            data = {
                "PatID": [1, 2, 3],
                "Name": ["Alice", "Bob", "Charlie"],
                "Amount": [100.5, 200.75, 300.25],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("test_table", df)

            # Export to parquet
            export_table(duckdb_con, "test_table", tmpdir, "parquet")

            # Verify file exists and is readable
            output_file = tmpdir / "test_table.parquet"
            assert output_file.exists()

            # Read back and verify data matches
            result_df = pl.read_parquet(str(output_file))
            assert len(result_df) == 3
            assert set(result_df.columns) == {"PatID", "Name", "Amount"}
            assert result_df["PatID"].to_list() == [1, 2, 3]
            assert result_df["Name"].to_list() == ["Alice", "Bob", "Charlie"]

    def test_ac71_parquet_export_with_multiple_types(self, duckdb_con):
        """AC7.1: Parquet export handles mixed column types correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create table with various types
            data = {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "birth_date": [
                    datetime.date(1990, 1, 15),
                    datetime.date(1985, 3, 20),
                    datetime.date(1992, 6, 10),
                ],
                "amount": [100.5, 200.75, 300.25],
                "active": [True, False, True],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("mixed_table", df)

            # Export to parquet
            export_table(duckdb_con, "mixed_table", tmpdir, "parquet")

            # Verify file exists and data types are preserved
            output_file = tmpdir / "mixed_table.parquet"
            result_df = pl.read_parquet(str(output_file))

            assert result_df["id"].dtype == pl.Int64
            assert result_df["name"].dtype == pl.Utf8
            assert result_df["birth_date"].dtype == pl.Date
            assert result_df["amount"].dtype == pl.Float64
            assert result_df["active"].dtype == pl.Boolean

    def test_ac74_parquet_file_naming(self, duckdb_con):
        """AC7.4: Parquet file is named {table}.parquet."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            df = pl.DataFrame({"col1": [1, 2]})
            duckdb_con.register("my_table", df)

            export_table(duckdb_con, "my_table", tmpdir, "parquet")

            output_file = tmpdir / "my_table.parquet"
            assert output_file.exists()
            assert output_file.name == "my_table.parquet"

    def test_parquet_export_with_nulls(self, duckdb_con):
        """Parquet export handles NULL values correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            data = {
                "id": [1, 2, None],
                "name": ["Alice", None, "Charlie"],
                "amount": [100.5, None, 300.25],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("null_table", df)

            export_table(duckdb_con, "null_table", tmpdir, "parquet")

            result_df = pl.read_parquet(str(tmpdir / "null_table.parquet"))
            assert result_df["id"][2] is None
            assert result_df["name"][1] is None
            assert result_df["amount"][1] is None


class TestExportCSV:
    """Tests for CSV export (AC7.2, AC7.4)."""

    def test_ac72_csv_export_creates_readable_file(self, duckdb_con):
        """AC7.2: Exported CSV file is readable by polars with headers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create a simple table in DuckDB
            data = {
                "PatID": [1, 2, 3],
                "Name": ["Alice", "Bob", "Charlie"],
                "Amount": [100, 200, 300],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("csv_table", df)

            # Export to CSV
            export_table(duckdb_con, "csv_table", tmpdir, "csv")

            # Verify file exists and is readable
            output_file = tmpdir / "csv_table.csv"
            assert output_file.exists()

            # Read back and verify data matches
            result_df = pl.read_csv(str(output_file))
            assert len(result_df) == 3
            assert set(result_df.columns) == {"PatID", "Name", "Amount"}
            assert result_df["PatID"].to_list() == [1, 2, 3]
            assert result_df["Name"].to_list() == ["Alice", "Bob", "Charlie"]

    def test_ac72_csv_has_header_row(self, duckdb_con):
        """AC7.2: CSV output contains header row."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            data = {
                "col_a": [10, 20],
                "col_b": ["x", "y"],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("header_table", df)

            export_table(duckdb_con, "header_table", tmpdir, "csv")

            output_file = tmpdir / "header_table.csv"

            # Read raw CSV content to verify header
            with open(output_file) as f:
                first_line = f.readline().strip()

            assert first_line == "col_a,col_b"

    def test_ac74_csv_file_naming(self, duckdb_con):
        """AC7.4: CSV file is named {table}.csv."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            df = pl.DataFrame({"col1": [1, 2]})
            duckdb_con.register("my_csv_table", df)

            export_table(duckdb_con, "my_csv_table", tmpdir, "csv")

            output_file = tmpdir / "my_csv_table.csv"
            assert output_file.exists()
            assert output_file.name == "my_csv_table.csv"

    def test_csv_export_with_nulls(self, duckdb_con):
        """CSV export handles NULL values correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            data = {
                "id": [1, 2, None],
                "name": ["Alice", None, "Charlie"],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("csv_null_table", df)

            export_table(duckdb_con, "csv_null_table", tmpdir, "csv")

            result_df = pl.read_csv(str(tmpdir / "csv_null_table.csv"))
            assert len(result_df) == 3

    def test_csv_export_with_dates(self, duckdb_con):
        """CSV export preserves date values correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            data = {
                "id": [1, 2, 3],
                "date": [
                    datetime.date(2020, 1, 15),
                    datetime.date(2021, 6, 20),
                    datetime.date(2022, 12, 25),
                ],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("csv_date_table", df)

            export_table(duckdb_con, "csv_date_table", tmpdir, "csv")

            result_df = pl.read_csv(str(tmpdir / "csv_date_table.csv"))
            assert len(result_df) == 3
            # Dates are read as strings from CSV
            assert result_df["date"][0] == "2020-01-15"
