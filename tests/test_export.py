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


class TestExportNDJSON:
    """Tests for NDJSON export (AC7.3, AC7.4)."""

    def test_ac73_ndjson_export_creates_readable_file(self, duckdb_con):
        """AC7.3: Exported NDJSON file is readable with valid JSON per line."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create a simple table in DuckDB
            data = {
                "PatID": [1, 2, 3],
                "Name": ["Alice", "Bob", "Charlie"],
                "Amount": [100, 200, 300],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("ndjson_table", df)

            # Export to NDJSON
            export_table(duckdb_con, "ndjson_table", tmpdir, "json")

            # Verify file exists
            output_file = tmpdir / "ndjson_table.json"
            assert output_file.exists()

            # Verify each line is valid JSON
            lines = output_file.read_text().strip().split("\n")
            assert len(lines) == 3

            records = []
            for line in lines:
                record = json.loads(line)
                records.append(record)

            # Verify data matches
            assert records[0]["PatID"] == 1
            assert records[1]["PatID"] == 2
            assert records[2]["PatID"] == 3
            assert records[0]["Name"] == "Alice"
            assert records[1]["Name"] == "Bob"

    def test_ac73_ndjson_with_mixed_types(self, duckdb_con):
        """AC7.3: NDJSON export handles mixed column types correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create table with various types
            data = {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "birth_date": [
                    datetime.date(1990, 1, 15),
                    datetime.date(1985, 3, 20),
                ],
                "amount": [100.5, 200.75],
                "active": [True, False],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("ndjson_mixed", df)

            # Export to NDJSON
            export_table(duckdb_con, "ndjson_mixed", tmpdir, "json")

            # Verify each line is valid JSON
            output_file = tmpdir / "ndjson_mixed.json"
            lines = output_file.read_text().strip().split("\n")

            record = json.loads(lines[0])
            assert record["id"] == 1
            assert record["name"] == "Alice"
            assert record["amount"] == 100.5
            assert record["active"] is True

    def test_ac74_ndjson_file_naming(self, duckdb_con):
        """AC7.4: NDJSON file is named {table}.json."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            df = pl.DataFrame({"col1": [1, 2]})
            duckdb_con.register("my_ndjson_table", df)

            export_table(duckdb_con, "my_ndjson_table", tmpdir, "json")

            output_file = tmpdir / "my_ndjson_table.json"
            assert output_file.exists()
            assert output_file.name == "my_ndjson_table.json"

    def test_ndjson_export_with_nulls(self, duckdb_con):
        """NDJSON export handles NULL values correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            data = {
                "id": [1, 2, None],
                "name": ["Alice", None, "Charlie"],
                "amount": [100.5, None, 300.25],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("ndjson_null", df)

            export_table(duckdb_con, "ndjson_null", tmpdir, "json")

            output_file = tmpdir / "ndjson_null.json"
            lines = output_file.read_text().strip().split("\n")

            records = [json.loads(line) for line in lines]

            # Verify NULL values are represented as null in JSON
            assert records[2]["id"] is None
            assert records[1]["name"] is None
            assert records[1]["amount"] is None

    def test_ndjson_large_batch_handling(self, duckdb_con):
        """NDJSON export correctly handles multiple batches."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create a larger dataset to exercise the export path
            data = {
                "id": list(range(250)),
                "value": [f"val_{i}" for i in range(250)],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("ndjson_large", df)

            export_table(duckdb_con, "ndjson_large", tmpdir, "json")

            output_file = tmpdir / "ndjson_large.json"
            lines = output_file.read_text().strip().split("\n")

            assert len(lines) == 250

            # Verify all lines are valid JSON and data matches
            records = [json.loads(line) for line in lines]
            for i, record in enumerate(records):
                assert record["id"] == i
                assert record["value"] == f"val_{i}"

    def test_ndjson_roundtrip_with_polars(self, duckdb_con):
        """NDJSON export can be read back with polars.read_ndjson()."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create original data
            original_data = {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [95.5, 87.3, 92.1],
            }
            df = pl.DataFrame(original_data)
            duckdb_con.register("roundtrip_table", df)

            # Export to NDJSON
            export_table(duckdb_con, "roundtrip_table", tmpdir, "json")

            # Read back with polars
            output_file = tmpdir / "roundtrip_table.json"
            result_df = pl.read_ndjson(str(output_file))

            # Verify data matches
            assert len(result_df) == 3
            assert result_df["id"].to_list() == [1, 2, 3]
            assert result_df["name"].to_list() == ["Alice", "Bob", "Charlie"]


class TestExportIntegration:
    """Integration tests for export_table() and export_all() (AC7.1-AC7.4)."""

    def test_roundtrip_parquet(self, duckdb_con):
        """Round-trip test: export to parquet and read back."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create table with mixed types
            original_data = {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "date": [
                    datetime.date(2020, 1, 15),
                    datetime.date(2021, 6, 20),
                    datetime.date(2022, 12, 25),
                    datetime.date(2023, 3, 10),
                    None,
                ],
                "amount": [100.5, 200.75, 300.0, None, 450.25],
            }
            df = pl.DataFrame(original_data)
            duckdb_con.register("roundtrip_parquet", df)

            # Export to parquet
            export_table(duckdb_con, "roundtrip_parquet", tmpdir, "parquet")

            # Read back
            result_df = pl.read_parquet(str(tmpdir / "roundtrip_parquet.parquet"))

            # Verify data matches
            assert len(result_df) == 5
            assert result_df["id"].to_list() == [1, 2, 3, 4, 5]
            assert result_df["name"].to_list() == ["Alice", "Bob", "Charlie", "David", "Eve"]

    def test_roundtrip_csv(self, duckdb_con):
        """Round-trip test: export to CSV and read back."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create table with mixed types
            original_data = {
                "id": [10, 20, 30],
                "value": ["X", "Y", "Z"],
                "score": [95.5, 87.3, 92.1],
            }
            df = pl.DataFrame(original_data)
            duckdb_con.register("roundtrip_csv", df)

            # Export to CSV
            export_table(duckdb_con, "roundtrip_csv", tmpdir, "csv")

            # Read back
            result_df = pl.read_csv(str(tmpdir / "roundtrip_csv.csv"))

            # Verify data matches
            assert len(result_df) == 3
            assert result_df["id"].to_list() == [10, 20, 30]
            assert result_df["value"].to_list() == ["X", "Y", "Z"]

    def test_roundtrip_ndjson(self, duckdb_con):
        """Round-trip test: export to NDJSON and read back."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create table with mixed types
            original_data = {
                "user_id": [100, 101, 102],
                "username": ["alice", "bob", "charlie"],
                "active": [True, False, True],
            }
            df = pl.DataFrame(original_data)
            duckdb_con.register("roundtrip_ndjson", df)

            # Export to NDJSON
            export_table(duckdb_con, "roundtrip_ndjson", tmpdir, "json")

            # Read back
            result_df = pl.read_ndjson(str(tmpdir / "roundtrip_ndjson.json"))

            # Verify data matches
            assert len(result_df) == 3
            assert result_df["user_id"].to_list() == [100, 101, 102]
            assert result_df["username"].to_list() == ["alice", "bob", "charlie"]
            assert result_df["active"].to_list() == [True, False, True]

    def test_export_all_creates_all_files(self, duckdb_con):
        """export_all() creates files for all table names."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create multiple tables in DuckDB
            table_names = ["table1", "table2", "table3", "table4", "table5"]
            for table_name in table_names:
                data = {"id": [1, 2, 3], "name": [f"{table_name}_a", f"{table_name}_b", f"{table_name}_c"]}
                df = pl.DataFrame(data)
                duckdb_con.register(table_name, df)

            # Export all to parquet
            export_all(duckdb_con, table_names, tmpdir, "parquet")

            # Verify all files exist with correct naming
            for table_name in table_names:
                output_file = tmpdir / f"{table_name}.parquet"
                assert output_file.exists(), f"Missing {output_file}"
                # Verify content is readable
                df_read = pl.read_parquet(str(output_file))
                assert len(df_read) == 3

    def test_export_all_with_9_tables(self, duckdb_con):
        """export_all() works with all 9 SCDM table names."""
        from scdm_prepare.schema import TABLES

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create all 9 tables with minimal data
            table_names = list(TABLES.keys())
            for table_name in table_names:
                # Create table with at least one row and basic columns
                data = {
                    col: [f"{col}_{i}" for i in range(3)]
                    for col in TABLES[table_name].columns[:3]  # Use first 3 columns
                }
                df = pl.DataFrame(data)
                duckdb_con.register(table_name, df)

            # Export all to parquet
            export_all(duckdb_con, table_names, tmpdir, "parquet")

            # Verify all 9 files exist
            for table_name in table_names:
                output_file = tmpdir / f"{table_name}.parquet"
                assert output_file.exists(), f"Missing {table_name}.parquet"

    def test_export_all_csv_format(self, duckdb_con):
        """export_all() creates CSV files with headers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create 3 tables
            for i in range(1, 4):
                table_name = f"csv_table_{i}"
                data = {"col_a": [1, 2], "col_b": ["x", "y"]}
                df = pl.DataFrame(data)
                duckdb_con.register(table_name, df)

            table_names = ["csv_table_1", "csv_table_2", "csv_table_3"]
            export_all(duckdb_con, table_names, tmpdir, "csv")

            # Verify all files exist and have headers
            for table_name in table_names:
                output_file = tmpdir / f"{table_name}.csv"
                assert output_file.exists()
                with open(output_file) as f:
                    first_line = f.readline().strip()
                assert first_line == "col_a,col_b"

    def test_export_all_json_format(self, duckdb_con):
        """export_all() creates NDJSON files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create 2 tables
            for i in range(1, 3):
                table_name = f"json_table_{i}"
                data = {"id": [i * 10, i * 20], "type": ["A", "B"]}
                df = pl.DataFrame(data)
                duckdb_con.register(table_name, df)

            table_names = ["json_table_1", "json_table_2"]
            export_all(duckdb_con, table_names, tmpdir, "json")

            # Verify all files exist and are valid NDJSON
            for table_name in table_names:
                output_file = tmpdir / f"{table_name}.json"
                assert output_file.exists()
                lines = output_file.read_text().strip().split("\n")
                assert len(lines) == 2
                for line in lines:
                    record = json.loads(line)  # Should not raise
                    assert "id" in record and "type" in record

    def test_export_unsupported_format_raises(self, duckdb_con):
        """export_table() raises ValueError for unsupported format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            data = {"col1": [1, 2]}
            df = pl.DataFrame(data)
            duckdb_con.register("test_table", df)

            with pytest.raises(ValueError) as exc_info:
                export_table(duckdb_con, "test_table", tmpdir, "unsupported")

            assert "Unsupported format" in str(exc_info.value)

    def test_export_creates_output_dir_if_not_exists(self, duckdb_con):
        """export_table() creates output directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            output_dir = tmpdir / "subdir" / "nested" / "output"

            data = {"col1": [1, 2]}
            df = pl.DataFrame(data)
            duckdb_con.register("test_table", df)

            # Export to non-existent directory
            export_table(duckdb_con, "test_table", output_dir, "parquet")

            # Verify output directory was created
            assert output_dir.exists()
            assert (output_dir / "test_table.parquet").exists()

    def test_export_large_table(self, duckdb_con):
        """export_table() handles large tables correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create large table with 10,000 rows
            data = {
                "id": list(range(10000)),
                "value": [f"val_{i}" for i in range(10000)],
                "amount": [float(i * 1.5) for i in range(10000)],
            }
            df = pl.DataFrame(data)
            duckdb_con.register("large_table", df)

            # Export to all formats
            export_table(duckdb_con, "large_table", tmpdir, "parquet")
            export_table(duckdb_con, "large_table", tmpdir, "csv")
            export_table(duckdb_con, "large_table", tmpdir, "json")

            # Verify all files exist and are readable
            parquet_df = pl.read_parquet(str(tmpdir / "large_table.parquet"))
            csv_df = pl.read_csv(str(tmpdir / "large_table.csv"))
            json_df = pl.read_ndjson(str(tmpdir / "large_table.json"))

            assert len(parquet_df) == 10000
            assert len(csv_df) == 10000
            assert len(json_df) == 10000
