import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest

from scdm_prepare.transform import build_crosswalks, get_crosswalk


class TestCrosswalkGeneration:
    """Tests for crosswalk generation functions."""

    def test_ac31_unique_ids_across_subsamples(self):
        """AC3.1: Same original ID in different subsamples gets different new IDs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create demographic table with PatID "ABC" in both samplenum 1 and 2
            data_sample1 = {
                "PatID": ["ABC", "DEF", "GHI"],
                "Birth_Date": [None, None, None],
                "Sex": ["M", "F", "M"],
                "Hispanic": ["N", "Y", "N"],
                "Race": ["W", "B", "A"],
                "PostalCode": ["12345", "54321", "11111"],
                "PostalCode_Date": [None, None, None],
                "ImputedRace": ["N", "N", "N"],
                "ImputedHispanic": ["N", "N", "N"],
                "samplenum": [1, 1, 1],
            }
            df1 = pl.DataFrame(data_sample1)
            df1.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            data_sample2 = {
                "PatID": ["ABC", "XYZ", "PQR"],
                "Birth_Date": [None, None, None],
                "Sex": ["F", "M", "F"],
                "Hispanic": ["Y", "N", "Y"],
                "Race": ["B", "A", "W"],
                "PostalCode": ["54321", "11111", "12345"],
                "PostalCode_Date": [None, None, None],
                "ImputedRace": ["N", "N", "N"],
                "ImputedHispanic": ["N", "N", "N"],
                "samplenum": [2, 2, 2],
            }
            df2 = pl.DataFrame(data_sample2)
            df2.write_parquet(str(tmpdir_path / "demographic_2.parquet"))

            # Create DuckDB connection and build patid crosswalk only
            con = duckdb.connect(":memory:")
            source_table = "demographic"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE patid_crosswalk AS
            SELECT
                orig_PatID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_PatID) AS PatID
            FROM (
                SELECT DISTINCT
                    PatID AS orig_PatID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE PatID IS NOT NULL
            )
            """
            con.execute(sql)

            # Retrieve patid_crosswalk
            crosswalk = get_crosswalk(con, "patid_crosswalk")

            # Check that PatID "ABC" in samplenum 1 and 2 have different new IDs
            abc_rows = crosswalk.filter(pl.col("orig_PatID") == "ABC")
            assert len(abc_rows) == 2, "Should have two rows for PatID ABC"

            abc_ids = sorted(abc_rows["PatID"].to_list())
            assert abc_ids[0] != abc_ids[1], (
                "Same original ID in different subsamples should get different new IDs"
            )

            con.close()

    def test_ac32_null_original_ids_excluded(self):
        """AC3.2: NULL original IDs are excluded from crosswalk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create demographic table with NULL PatID
            data = {
                "PatID": ["ABC", None, "DEF"],
                "Birth_Date": [None, None, None],
                "Sex": ["M", "F", "F"],
                "Hispanic": ["N", "Y", "N"],
                "Race": ["W", "B", "A"],
                "PostalCode": ["12345", "54321", "11111"],
                "PostalCode_Date": [None, None, None],
                "ImputedRace": ["N", "N", "N"],
                "ImputedHispanic": ["N", "N", "N"],
                "samplenum": [1, 1, 1],
            }
            df = pl.DataFrame(data)
            df.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            # Build patid crosswalk
            con = duckdb.connect(":memory:")
            source_table = "demographic"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE patid_crosswalk AS
            SELECT
                orig_PatID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_PatID) AS PatID
            FROM (
                SELECT DISTINCT
                    PatID AS orig_PatID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE PatID IS NOT NULL
            )
            """
            con.execute(sql)

            # Retrieve crosswalk
            crosswalk = get_crosswalk(con, "patid_crosswalk")

            # Check that NULL is not in the crosswalk
            null_rows = crosswalk.filter(pl.col("orig_PatID").is_null())
            assert len(null_rows) == 0, "NULL original IDs should not be in crosswalk"

            # Check that non-NULL values are present
            assert len(crosswalk) == 2, "Should have 2 rows for 2 non-NULL IDs"

            con.close()

    def test_ac33_four_crosswalks_created(self):
        """AC3.3: All four crosswalks (patid, encounterid, providerid, facilityid) are created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create minimal test data for each source table
            for table_name, id_col in [
                ("demographic", "PatID"),
                ("encounter", "EncounterID"),
                ("provider", "ProviderID"),
                ("facility", "FacilityID"),
            ]:
                if table_name == "demographic":
                    data = {
                        "PatID": ["P1", "P2"],
                        "Birth_Date": [None, None],
                        "Sex": ["M", "F"],
                        "Hispanic": ["N", "Y"],
                        "Race": ["W", "B"],
                        "PostalCode": ["12345", "54321"],
                        "PostalCode_Date": [None, None],
                        "ImputedRace": ["N", "N"],
                        "ImputedHispanic": ["N", "N"],
                        "samplenum": [1, 1],
                    }
                elif table_name == "encounter":
                    data = {
                        "PatID": ["P1", "P2"],
                        "EncounterID": ["E1", "E2"],
                        "ADate": [None, None],
                        "DDate": [None, None],
                        "EncType": ["I", "O"],
                        "FacilityID": ["F1", "F2"],
                        "Discharge_Disposition": ["home", "home"],
                        "Discharge_Status": [1, 1],
                        "DRG": [1, 2],
                        "DRG_Type": ["I", "I"],
                        "Admitting_Source": ["ER", "OP"],
                        "samplenum": [1, 1],
                    }
                elif table_name == "provider":
                    data = {
                        "ProviderID": ["Pr1", "Pr2"],
                        "Specialty": ["MD", "RN"],
                        "Specialty_CodeType": ["code1", "code1"],
                        "samplenum": [1, 1],
                    }
                else:  # facility
                    data = {
                        "FacilityID": ["F1", "F2"],
                        "Facility_Location": ["loc1", "loc2"],
                        "samplenum": [1, 1],
                    }

                df = pl.DataFrame(data)
                df.write_parquet(str(tmpdir_path / f"{table_name}_1.parquet"))

            # Build crosswalks
            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

            # Verify all four crosswalks exist
            tables = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
            table_names = {row[0] for row in tables}

            assert "patid_crosswalk" in table_names, "patid_crosswalk not created"
            assert "encounterid_crosswalk" in table_names, "encounterid_crosswalk not created"
            assert "providerid_crosswalk" in table_names, "providerid_crosswalk not created"
            assert "facilityid_crosswalk" in table_names, "facilityid_crosswalk not created"

            con.close()


class TestCrosswalkEdgeCases:
    """Edge case tests for crosswalk generation."""

    def test_sequential_ids_start_from_one(self):
        """Crosswalk IDs are sequential starting from 1 with no gaps."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create demographic with 5 distinct PatIDs
            data = {
                "PatID": ["A", "B", "C", "D", "E"],
                "Birth_Date": [None] * 5,
                "Sex": ["M"] * 5,
                "Hispanic": ["N"] * 5,
                "Race": ["W"] * 5,
                "PostalCode": ["12345"] * 5,
                "PostalCode_Date": [None] * 5,
                "ImputedRace": ["N"] * 5,
                "ImputedHispanic": ["N"] * 5,
                "samplenum": [1] * 5,
            }
            df = pl.DataFrame(data)
            df.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            con = duckdb.connect(":memory:")
            source_table = "demographic"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE patid_crosswalk AS
            SELECT
                orig_PatID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_PatID) AS PatID
            FROM (
                SELECT DISTINCT
                    PatID AS orig_PatID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE PatID IS NOT NULL
            )
            """
            con.execute(sql)

            crosswalk = get_crosswalk(con, "patid_crosswalk")
            ids = sorted(crosswalk["PatID"].to_list())

            # Check sequential from 1 with no gaps
            assert ids == list(range(1, len(ids) + 1)), (
                f"IDs should be sequential from 1, got {ids}"
            )

            con.close()

    def test_single_subsample_correct_crosswalk(self):
        """Single subsample produces correct crosswalk without deduplication issues."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create demographic with one subsample
            data = {
                "PatID": ["P1", "P2", "P3"],
                "Birth_Date": [None] * 3,
                "Sex": ["M", "F", "M"],
                "Hispanic": ["N", "Y", "N"],
                "Race": ["W", "B", "A"],
                "PostalCode": ["12345", "54321", "11111"],
                "PostalCode_Date": [None] * 3,
                "ImputedRace": ["N"] * 3,
                "ImputedHispanic": ["N"] * 3,
                "samplenum": [1] * 3,
            }
            df = pl.DataFrame(data)
            df.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            con = duckdb.connect(":memory:")
            source_table = "demographic"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE patid_crosswalk AS
            SELECT
                orig_PatID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_PatID) AS PatID
            FROM (
                SELECT DISTINCT
                    PatID AS orig_PatID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE PatID IS NOT NULL
            )
            """
            con.execute(sql)

            crosswalk = get_crosswalk(con, "patid_crosswalk")

            # Verify 3 rows, IDs 1-3
            assert len(crosswalk) == 3
            assert sorted(crosswalk["PatID"].to_list()) == [1, 2, 3]

            con.close()

    def test_empty_source_table_produces_empty_crosswalk(self):
        """Empty source table produces empty crosswalk without errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create empty demographic table
            data = {
                "PatID": [],
                "Birth_Date": [],
                "Sex": [],
                "Hispanic": [],
                "Race": [],
                "PostalCode": [],
                "PostalCode_Date": [],
                "ImputedRace": [],
                "ImputedHispanic": [],
                "samplenum": [],
            }
            df = pl.DataFrame(data)
            df.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            con = duckdb.connect(":memory:")
            source_table = "demographic"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE patid_crosswalk AS
            SELECT
                orig_PatID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_PatID) AS PatID
            FROM (
                SELECT DISTINCT
                    PatID AS orig_PatID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE PatID IS NOT NULL
            )
            """
            con.execute(sql)

            crosswalk = get_crosswalk(con, "patid_crosswalk")

            # Empty crosswalk should exist but have 0 rows
            assert len(crosswalk) == 0

            con.close()

    def test_duplicate_ids_within_table_deduplicated(self):
        """Duplicate IDs within a single source table are deduplicated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create provider table with duplicate ProviderIDs
            data = {
                "ProviderID": ["Pr1", "Pr2", "Pr1", "Pr3", "Pr2"],
                "Specialty": ["MD", "RN", "MD", "PA", "RN"],
                "Specialty_CodeType": ["code1"] * 5,
                "samplenum": [1] * 5,
            }
            df = pl.DataFrame(data)
            df.write_parquet(str(tmpdir_path / "provider_1.parquet"))

            con = duckdb.connect(":memory:")
            source_table = "provider"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE providerid_crosswalk AS
            SELECT
                orig_ProviderID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_ProviderID) AS ProviderID
            FROM (
                SELECT DISTINCT
                    ProviderID AS orig_ProviderID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE ProviderID IS NOT NULL
            )
            """
            con.execute(sql)

            crosswalk = get_crosswalk(con, "providerid_crosswalk")

            # Should have only 3 distinct ProviderIDs (Pr1, Pr2, Pr3)
            assert len(crosswalk) == 3
            orig_ids = set(crosswalk["orig_ProviderID"].to_list())
            assert orig_ids == {"Pr1", "Pr2", "Pr3"}

            con.close()

    def test_large_id_values_handled_correctly(self):
        """Large ID values (strings and large integers) are handled correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create demographic with large string and numeric-looking IDs
            data = {
                "PatID": ["ID_99999999", "ID_88888888", "ID_77777777"],
                "Birth_Date": [None] * 3,
                "Sex": ["M"] * 3,
                "Hispanic": ["N"] * 3,
                "Race": ["W"] * 3,
                "PostalCode": ["12345"] * 3,
                "PostalCode_Date": [None] * 3,
                "ImputedRace": ["N"] * 3,
                "ImputedHispanic": ["N"] * 3,
                "samplenum": [1] * 3,
            }
            df = pl.DataFrame(data)
            df.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            con = duckdb.connect(":memory:")
            source_table = "demographic"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE patid_crosswalk AS
            SELECT
                orig_PatID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_PatID) AS PatID
            FROM (
                SELECT DISTINCT
                    PatID AS orig_PatID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE PatID IS NOT NULL
            )
            """
            con.execute(sql)

            crosswalk = get_crosswalk(con, "patid_crosswalk")

            # All 3 IDs should be present and mapped
            assert len(crosswalk) == 3
            assert sorted(crosswalk["PatID"].to_list()) == [1, 2, 3]

            con.close()

    def test_multiple_subsamples_combined(self):
        """Multiple subsamples are correctly combined in one crosswalk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create demographic for subsamples 1 and 2
            data1 = {
                "PatID": ["P1", "P2"],
                "Birth_Date": [None] * 2,
                "Sex": ["M"] * 2,
                "Hispanic": ["N"] * 2,
                "Race": ["W"] * 2,
                "PostalCode": ["12345"] * 2,
                "PostalCode_Date": [None] * 2,
                "ImputedRace": ["N"] * 2,
                "ImputedHispanic": ["N"] * 2,
                "samplenum": [1] * 2,
            }
            df1 = pl.DataFrame(data1)
            df1.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            data2 = {
                "PatID": ["P3", "P4"],
                "Birth_Date": [None] * 2,
                "Sex": ["F"] * 2,
                "Hispanic": ["Y"] * 2,
                "Race": ["B"] * 2,
                "PostalCode": ["54321"] * 2,
                "PostalCode_Date": [None] * 2,
                "ImputedRace": ["N"] * 2,
                "ImputedHispanic": ["N"] * 2,
                "samplenum": [2] * 2,
            }
            df2 = pl.DataFrame(data2)
            df2.write_parquet(str(tmpdir_path / "demographic_2.parquet"))

            con = duckdb.connect(":memory:")
            source_table = "demographic"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE patid_crosswalk AS
            SELECT
                orig_PatID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_PatID) AS PatID
            FROM (
                SELECT DISTINCT
                    PatID AS orig_PatID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE PatID IS NOT NULL
            )
            """
            con.execute(sql)

            crosswalk = get_crosswalk(con, "patid_crosswalk")

            # Should have 4 total rows (2 from each subsample)
            assert len(crosswalk) == 4
            # All should get sequential IDs 1-4
            assert sorted(crosswalk["PatID"].to_list()) == [1, 2, 3, 4]

            con.close()

    def test_crosswalk_column_names_correct(self):
        """Crosswalk has correct column names: orig_{id}, samplenum, {id}."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            data = {
                "PatID": ["P1"],
                "Birth_Date": [None],
                "Sex": ["M"],
                "Hispanic": ["N"],
                "Race": ["W"],
                "PostalCode": ["12345"],
                "PostalCode_Date": [None],
                "ImputedRace": ["N"],
                "ImputedHispanic": ["N"],
                "samplenum": [1],
            }
            df = pl.DataFrame(data)
            df.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            con = duckdb.connect(":memory:")
            source_table = "demographic"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE patid_crosswalk AS
            SELECT
                orig_PatID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_PatID) AS PatID
            FROM (
                SELECT DISTINCT
                    PatID AS orig_PatID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE PatID IS NOT NULL
            )
            """
            con.execute(sql)

            crosswalk = get_crosswalk(con, "patid_crosswalk")

            expected_cols = {"orig_PatID", "samplenum", "PatID"}
            assert set(crosswalk.columns) == expected_cols

            con.close()

    def test_crosswalk_ordering_by_samplenum_then_id(self):
        """Crosswalk IDs are assigned in order of (samplenum, orig_id)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create data with specific ordering: samplenum 2 before samplenum 1
            data = {
                "PatID": ["Z", "A"],
                "Birth_Date": [None] * 2,
                "Sex": ["M"] * 2,
                "Hispanic": ["N"] * 2,
                "Race": ["W"] * 2,
                "PostalCode": ["12345"] * 2,
                "PostalCode_Date": [None] * 2,
                "ImputedRace": ["N"] * 2,
                "ImputedHispanic": ["N"] * 2,
                "samplenum": [2, 1],  # samplenum 2, then samplenum 1
            }
            df = pl.DataFrame(data)
            df.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            con = duckdb.connect(":memory:")
            source_table = "demographic"
            glob_pattern = str(tmpdir_path / f"{source_table}_*.parquet")
            sql = f"""
            CREATE OR REPLACE TABLE patid_crosswalk AS
            SELECT
                orig_PatID,
                samplenum,
                ROW_NUMBER() OVER (ORDER BY samplenum, orig_PatID) AS PatID
            FROM (
                SELECT DISTINCT
                    PatID AS orig_PatID,
                    samplenum
                FROM read_parquet('{glob_pattern}')
                WHERE PatID IS NOT NULL
            )
            """
            con.execute(sql)

            crosswalk = get_crosswalk(con, "patid_crosswalk").sort("PatID")

            # Samplenum 1 should come before samplenum 2 in assignment order
            rows = crosswalk.to_dicts()
            # PatID 1 should be for samplenum 1 (orig A)
            # PatID 2 should be for samplenum 2 (orig Z)
            assert rows[0]["samplenum"] == 1
            assert rows[1]["samplenum"] == 2

            con.close()
