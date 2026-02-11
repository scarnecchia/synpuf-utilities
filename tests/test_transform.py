import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest

from scdm_prepare.schema import TABLES
from scdm_prepare.transform import assemble_tables, build_crosswalks, get_crosswalk, synthesise_tables


def _create_minimal_fixtures(tmpdir_path: Path) -> None:
    """Create minimal parquet files for all four source tables.

    Each table has a single row with the required columns. Only creates files
    if they don't already exist in the directory (doesn't overwrite test data).

    Args:
        tmpdir_path: Directory to write parquet files to
    """
    tables_data = {
        "demographic": {
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
        },
        "encounter": {
            "PatID": ["P1"],
            "EncounterID": ["E1"],
            "ADate": [None],
            "DDate": [None],
            "EncType": ["I"],
            "FacilityID": ["F1"],
            "Discharge_Disposition": ["home"],
            "Discharge_Status": [1],
            "DRG": [1],
            "DRG_Type": ["I"],
            "Admitting_Source": ["ER"],
            "samplenum": [1],
        },
        "provider": {
            "ProviderID": ["Pr1"],
            "Specialty": ["MD"],
            "Specialty_CodeType": ["code1"],
            "samplenum": [1],
        },
        "facility": {
            "FacilityID": ["F1"],
            "Facility_Location": ["loc1"],
            "samplenum": [1],
        },
    }

    for table_name, data in tables_data.items():
        # Check if any file for this table already exists
        existing_files = list(tmpdir_path.glob(f"{table_name}_*.parquet"))
        if not existing_files:
            df = pl.DataFrame(data)
            df.write_parquet(str(tmpdir_path / f"{table_name}_1.parquet"))


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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            # Create DuckDB connection and build all crosswalks
            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            # Build all crosswalks
            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

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


class TestGetCrosswalkValidation:
    """Tests for get_crosswalk() input validation."""

    def test_get_crosswalk_rejects_unknown_name(self):
        """ValueError raised when crosswalk_name is not a known crosswalk."""
        con = duckdb.connect(":memory:")
        with pytest.raises(ValueError, match="unknown crosswalk"):
            get_crosswalk(con, "not_a_real_crosswalk")
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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

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

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)

            crosswalk = get_crosswalk(con, "patid_crosswalk").sort("PatID")

            # Samplenum 1 should come before samplenum 2 in assignment order
            rows = crosswalk.to_dicts()
            # PatID 1 should be for samplenum 1 (orig A)
            # PatID 2 should be for samplenum 2 (orig Z)
            assert rows[0]["samplenum"] == 1
            assert rows[1]["samplenum"] == 2

            con.close()


class TestTableAssembly:
    """Tests for table assembly with crosswalk joins."""

    def test_ac41_enrollment_columns(self):
        """AC4.1: Enrollment table contains exactly the expected columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create minimal enrollment data
            enrollment_data = {
                "PatID": ["P1", "P2"],
                "Enr_Start": [None, None],
                "Enr_End": [None, None],
                "MedCov": ["Y", "N"],
                "DrugCov": ["Y", "Y"],
                "Chart": ["Y", "N"],
                "PlanType": ["HMO", "PPO"],
                "PayerType": ["Medicaid", "Medicare"],
                "samplenum": [1, 1],
            }
            df = pl.DataFrame(enrollment_data)
            df.write_parquet(str(tmpdir_path / "enrollment_1.parquet"))

            # Create minimal fixtures for other tables
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            # Retrieve enrollment table
            enrollment = con.sql("SELECT * FROM enrollment").pl()

            # Check columns match expected
            expected_cols = set(TABLES["enrollment"].columns)
            actual_cols = set(enrollment.columns)
            assert actual_cols == expected_cols, f"Expected {expected_cols}, got {actual_cols}"

            con.close()

    def test_ac42_enrollment_column_order(self):
        """AC4.2: Enrollment column order matches schema specification."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            enrollment_data = {
                "PatID": ["P1"],
                "Enr_Start": [None],
                "Enr_End": [None],
                "MedCov": ["Y"],
                "DrugCov": ["Y"],
                "Chart": ["Y"],
                "PlanType": ["HMO"],
                "PayerType": ["Medicaid"],
                "samplenum": [1],
            }
            df = pl.DataFrame(enrollment_data)
            df.write_parquet(str(tmpdir_path / "enrollment_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            enrollment = con.sql("SELECT * FROM enrollment").pl()

            # Check column order
            expected_order = list(TABLES["enrollment"].columns)
            actual_order = enrollment.columns
            assert actual_order == expected_order, (
                f"Column order mismatch. Expected {expected_order}, got {actual_order}"
            )

            con.close()

    def test_ac43_inner_join_patid_excludes_nulls(self):
        """AC4.3: INNER JOIN on PatID excludes rows with NULL PatID in source."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create demographic with a NULL PatID
            demographic_data = {
                "PatID": ["P1", None, "P3"],
                "Birth_Date": [None] * 3,
                "Sex": ["M", "F", "M"],
                "Hispanic": ["N", "Y", "N"],
                "Race": ["W", "B", "A"],
                "PostalCode": ["12345", "54321", "11111"],
                "PostalCode_Date": [None] * 3,
                "ImputedRace": ["N"] * 3,
                "ImputedHispanic": ["N"] * 3,
                "samplenum": [1, 1, 1],
            }
            df = pl.DataFrame(demographic_data)
            df.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            demographic = con.sql("SELECT * FROM demographic").pl()

            # Should only have 2 rows (P1 and P3, not the NULL one)
            assert len(demographic) == 2, f"Expected 2 rows, got {len(demographic)}"

            con.close()

    def test_ac43_left_join_encounterid_includes_nulls(self):
        """AC4.3: LEFT JOIN on EncounterID includes rows with NULL new ID."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create encounter with a NULL EncounterID
            encounter_data = {
                "PatID": ["P1", "P2"],
                "EncounterID": ["E1", None],
                "ADate": [None] * 2,
                "DDate": [None] * 2,
                "EncType": ["I", "O"],
                "FacilityID": ["F1", "F2"],
                "Discharge_Disposition": ["home", "home"],
                "Discharge_Status": [1, 1],
                "DRG": [1, 2],
                "DRG_Type": ["I", "I"],
                "Admitting_Source": ["ER", "OP"],
                "samplenum": [1, 1],
            }
            df = pl.DataFrame(encounter_data)
            df.write_parquet(str(tmpdir_path / "encounter_1.parquet"))

            # Create demographic with both P1 and P2 so both pass INNER JOIN
            demographic_data = {
                "PatID": ["P1", "P2"],
                "Birth_Date": [None] * 2,
                "Sex": ["M", "F"],
                "Hispanic": ["N", "Y"],
                "Race": ["W", "B"],
                "PostalCode": ["12345", "54321"],
                "PostalCode_Date": [None] * 2,
                "ImputedRace": ["N"] * 2,
                "ImputedHispanic": ["N"] * 2,
                "samplenum": [1, 1],
            }
            df_demo = pl.DataFrame(demographic_data)
            df_demo.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            encounter = con.sql("SELECT * FROM encounter").pl()

            # Should have 2 rows: one with EncounterID mapped, one with NULL
            assert len(encounter) == 2, f"Expected 2 rows, got {len(encounter)}"

            # Find the row with NULL source EncounterID
            null_encounter_rows = encounter.filter(pl.col("EncounterID").is_null())
            assert len(null_encounter_rows) == 1, "Expected 1 row with NULL EncounterID"

            con.close()

    def test_ac51_enrollment_sort_order(self):
        """AC5.1: Enrollment table is sorted by PatID, Enr_Start, Enr_End, MedCov, DrugCov, Chart."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create enrollment with multiple rows in non-sorted order
            enrollment_data = {
                "PatID": ["P2", "P1", "P2", "P1"],
                "Enr_Start": [None, None, None, None],
                "Enr_End": [None, None, None, None],
                "MedCov": ["N", "Y", "Y", "N"],
                "DrugCov": ["Y", "Y", "N", "N"],
                "Chart": ["Y", "N", "Y", "N"],
                "PlanType": ["PPO", "HMO", "HMO", "PPO"],
                "PayerType": ["Medicare", "Medicaid", "Medicaid", "Medicare"],
                "samplenum": [1, 1, 1, 1],
            }
            df = pl.DataFrame(enrollment_data)
            df.write_parquet(str(tmpdir_path / "enrollment_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            enrollment = con.sql("SELECT * FROM enrollment").pl()

            # Check that PatID is sorted
            patient_ids = enrollment["PatID"].to_list()
            assert patient_ids == sorted(patient_ids), "PatID not sorted correctly"

            con.close()

    def test_ac51_demographic_sort_order(self):
        """AC5.1: Demographic table is sorted by PatID."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            demographic_data = {
                "PatID": ["P3", "P1", "P2"],
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
            df = pl.DataFrame(demographic_data)
            df.write_parquet(str(tmpdir_path / "demographic_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            demographic = con.sql("SELECT * FROM demographic").pl()

            # Check that PatID column is sorted
            patient_ids = demographic["PatID"].to_list()
            sorted_ids = sorted(patient_ids)
            # Map to integer keys for comparison
            id_to_order = {pid: idx for idx, pid in enumerate(sorted_ids)}
            actual_order = [id_to_order[pid] for pid in patient_ids]
            expected_order = list(range(len(patient_ids)))

            assert actual_order == expected_order, f"PatID not sorted: {patient_ids}"

            con.close()

    def test_dispensing_passthrough_providerid(self):
        """Dispensing: ProviderID is passed through directly (not crosswalked)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create dispensing with specific ProviderID values
            dispensing_data = {
                "PatID": ["P1", "P1"],
                "ProviderID": ["ORIG_PR1", "ORIG_PR2"],
                "RxDate": [None, None],
                "Rx": ["RX001", "RX002"],
                "Rx_CodeType": ["code1", "code1"],
                "RxSup": [30, 60],
                "RxAmt": [100, 200],
                "samplenum": [1, 1],
            }
            df = pl.DataFrame(dispensing_data)
            df.write_parquet(str(tmpdir_path / "dispensing_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            dispensing = con.sql("SELECT * FROM dispensing").pl()

            # ProviderID should be the original values, not mapped
            provider_ids = set(dispensing["ProviderID"].to_list())
            assert "ORIG_PR1" in provider_ids, "ProviderID not passed through"
            assert "ORIG_PR2" in provider_ids, "ProviderID not passed through"

            con.close()

    def test_diagnosis_crosswalks_providerid(self):
        """Diagnosis: ProviderID is crosswalked via providerid_crosswalk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create diagnosis with ProviderID matching the fixture's provider ID
            diagnosis_data = {
                "PatID": ["P1"],
                "EncounterID": ["E1"],
                "ADate": [None],
                "ProviderID": ["Pr1"],
                "EncType": ["I"],
                "DX": ["DX001"],
                "Dx_Codetype": ["ICD9CM"],
                "OrigDX": ["DX001"],
                "PDX": ["Y"],
                "PAdmit": ["Y"],
                "samplenum": [1],
            }
            df = pl.DataFrame(diagnosis_data)
            df.write_parquet(str(tmpdir_path / "diagnosis_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            diagnosis = con.sql("SELECT * FROM diagnosis").pl()

            # ProviderID should be mapped to an integer via the crosswalk
            provider_ids = diagnosis["ProviderID"].to_list()
            # First row should have the mapped ID (an integer, not the original string)
            assert isinstance(provider_ids[0], int), f"Expected int, got {type(provider_ids[0])}"

            con.close()

    def test_ac41_encounter_columns(self):
        """AC4.1: Encounter table contains exactly the expected columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create minimal encounter data with all required columns
            encounter_data = {
                "PatID": ["P1"],
                "EncounterID": ["E1"],
                "ADate": [None],
                "DDate": [None],
                "EncType": ["I"],
                "FacilityID": ["F1"],
                "Discharge_Disposition": ["home"],
                "Discharge_Status": [1],
                "DRG": [1],
                "DRG_Type": ["I"],
                "Admitting_Source": ["ER"],
                "samplenum": [1],
            }
            df = pl.DataFrame(encounter_data)
            df.write_parquet(str(tmpdir_path / "encounter_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            # Retrieve encounter table
            encounter = con.sql("SELECT * FROM encounter").pl()

            # Check columns match expected
            expected_cols = set(TABLES["encounter"].columns)
            actual_cols = set(encounter.columns)
            assert actual_cols == expected_cols, f"Expected {expected_cols}, got {actual_cols}"

            con.close()

    def test_ac42_encounter_column_order(self):
        """AC4.2: Encounter column order matches schema specification."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            encounter_data = {
                "PatID": ["P1"],
                "EncounterID": ["E1"],
                "ADate": [None],
                "DDate": [None],
                "EncType": ["I"],
                "FacilityID": ["F1"],
                "Discharge_Disposition": ["home"],
                "Discharge_Status": [1],
                "DRG": [1],
                "DRG_Type": ["I"],
                "Admitting_Source": ["ER"],
                "samplenum": [1],
            }
            df = pl.DataFrame(encounter_data)
            df.write_parquet(str(tmpdir_path / "encounter_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            encounter = con.sql("SELECT * FROM encounter").pl()

            # Check column order
            expected_order = list(TABLES["encounter"].columns)
            actual_order = encounter.columns
            assert actual_order == expected_order, (
                f"Column order mismatch. Expected {expected_order}, got {actual_order}"
            )

            con.close()

    def test_ac41_diagnosis_columns(self):
        """AC4.1: Diagnosis table contains exactly the expected columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create minimal diagnosis data with all required columns
            diagnosis_data = {
                "PatID": ["P1"],
                "EncounterID": ["E1"],
                "ADate": [None],
                "ProviderID": ["Pr1"],
                "EncType": ["I"],
                "DX": ["DX001"],
                "Dx_Codetype": ["ICD9CM"],
                "OrigDX": ["DX001"],
                "PDX": ["Y"],
                "PAdmit": ["Y"],
                "samplenum": [1],
            }
            df = pl.DataFrame(diagnosis_data)
            df.write_parquet(str(tmpdir_path / "diagnosis_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            # Retrieve diagnosis table
            diagnosis = con.sql("SELECT * FROM diagnosis").pl()

            # Check columns match expected
            expected_cols = set(TABLES["diagnosis"].columns)
            actual_cols = set(diagnosis.columns)
            assert actual_cols == expected_cols, f"Expected {expected_cols}, got {actual_cols}"

            con.close()

    def test_ac42_diagnosis_column_order(self):
        """AC4.2: Diagnosis column order matches schema specification."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            diagnosis_data = {
                "PatID": ["P1"],
                "EncounterID": ["E1"],
                "ADate": [None],
                "ProviderID": ["Pr1"],
                "EncType": ["I"],
                "DX": ["DX001"],
                "Dx_Codetype": ["ICD9CM"],
                "OrigDX": ["DX001"],
                "PDX": ["Y"],
                "PAdmit": ["Y"],
                "samplenum": [1],
            }
            df = pl.DataFrame(diagnosis_data)
            df.write_parquet(str(tmpdir_path / "diagnosis_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            assemble_tables(con, tmpdir_path)

            diagnosis = con.sql("SELECT * FROM diagnosis").pl()

            # Check column order
            expected_order = list(TABLES["diagnosis"].columns)
            actual_order = diagnosis.columns
            assert actual_order == expected_order, (
                f"Column order mismatch. Expected {expected_order}, got {actual_order}"
            )

            con.close()


class TestTableSynthesis:
    """Tests for Provider and Facility table synthesis."""

    def test_ac61_provider_table_structure(self):
        """AC6.1: Provider table has correct columns and hardcoded values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create minimal fixtures
            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            synthesise_tables(con)

            provider = con.sql("SELECT * FROM provider").pl()

            # Check columns
            expected_cols = {"ProviderID", "Specialty", "Specialty_CodeType"}
            actual_cols = set(provider.columns)
            assert actual_cols == expected_cols, f"Expected {expected_cols}, got {actual_cols}"

            # Check hardcoded values
            assert all(
                specialty == "99" for specialty in provider["Specialty"]
            ), "All Specialty values should be '99'"
            assert all(
                codetype == "2" for codetype in provider["Specialty_CodeType"]
            ), "All Specialty_CodeType values should be '2'"

            con.close()

    def test_ac62_facility_table_structure(self):
        """AC6.2: Facility table has correct columns and empty Facility_Location."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            synthesise_tables(con)

            facility = con.sql("SELECT * FROM facility").pl()

            # Check columns
            expected_cols = {"FacilityID", "Facility_Location"}
            actual_cols = set(facility.columns)
            assert actual_cols == expected_cols, f"Expected {expected_cols}, got {actual_cols}"

            # Check that Facility_Location is empty string
            assert all(
                loc == "" for loc in facility["Facility_Location"]
            ), "All Facility_Location values should be empty strings"

            con.close()

    def test_ac63_provider_excludes_null_originals(self):
        """AC6.3: Provider table excludes rows where original ProviderID was NULL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create provider with a NULL ProviderID
            provider_data = {
                "ProviderID": ["PR1", None, "PR3"],
                "Specialty": ["MD", "RN", "PA"],
                "Specialty_CodeType": ["code1", "code2", "code3"],
                "samplenum": [1, 1, 1],
            }
            df = pl.DataFrame(provider_data)
            df.write_parquet(str(tmpdir_path / "provider_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            synthesise_tables(con)

            provider = con.sql("SELECT * FROM provider").pl()
            provider_cw = get_crosswalk(con, "providerid_crosswalk")

            # Crosswalk itself excludes NULL originals by design (phase 3)
            # So we verify that synthesised provider table matches crosswalk count
            # and that all provider IDs are non-NULL
            assert len(provider) == len(provider_cw), (
                "Provider table should have same count as crosswalk (both exclude NULLs)"
            )

            # Verify no NULL ProviderID in synthesised table
            null_rows = provider.filter(pl.col("ProviderID").is_null())
            assert len(null_rows) == 0, "No NULL ProviderID should be in synthesised provider table"

            con.close()

    def test_ac63_facility_excludes_null_originals(self):
        """AC6.3: Facility table excludes rows where original FacilityID was NULL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create facility with a NULL FacilityID
            facility_data = {
                "FacilityID": ["F1", None, "F3"],
                "Facility_Location": ["loc1", "loc2", "loc3"],
                "samplenum": [1, 1, 1],
            }
            df = pl.DataFrame(facility_data)
            df.write_parquet(str(tmpdir_path / "facility_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            synthesise_tables(con)

            facility = con.sql("SELECT * FROM facility").pl()
            facility_cw = get_crosswalk(con, "facilityid_crosswalk")

            # Crosswalk itself excludes NULL originals by design (phase 3)
            # So we verify that synthesised facility table matches crosswalk count
            # and that all facility IDs are non-NULL
            assert len(facility) == len(facility_cw), (
                "Facility table should have same count as crosswalk (both exclude NULLs)"
            )

            # Verify no NULL FacilityID in synthesised table
            null_rows = facility.filter(pl.col("FacilityID").is_null())
            assert len(null_rows) == 0, "No NULL FacilityID should be in synthesised facility table"

            con.close()

    def test_provider_sort_order(self):
        """Provider table is sorted by ProviderID."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            provider_data = {
                "ProviderID": ["PR3", "PR1", "PR2"],
                "Specialty": ["MD", "RN", "PA"],
                "Specialty_CodeType": ["code1"] * 3,
                "samplenum": [1] * 3,
            }
            df = pl.DataFrame(provider_data)
            df.write_parquet(str(tmpdir_path / "provider_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            synthesise_tables(con)

            provider = con.sql("SELECT * FROM provider").pl()

            # Check that ProviderID is sorted by numeric value
            provider_ids = provider["ProviderID"].to_list()
            assert all(
                provider_ids[i] <= provider_ids[i + 1] for i in range(len(provider_ids) - 1)
            ), "ProviderID not sorted"

            con.close()

    def test_facility_sort_order(self):
        """Facility table is sorted by FacilityID."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            facility_data = {
                "FacilityID": ["F3", "F1", "F2"],
                "Facility_Location": ["loc3", "loc1", "loc2"],
                "samplenum": [1] * 3,
            }
            df = pl.DataFrame(facility_data)
            df.write_parquet(str(tmpdir_path / "facility_1.parquet"))

            _create_minimal_fixtures(tmpdir_path)

            con = duckdb.connect(":memory:")
            build_crosswalks(con, tmpdir_path)
            synthesise_tables(con)

            facility = con.sql("SELECT * FROM facility").pl()

            # Check that FacilityID is sorted by numeric value
            facility_ids = facility["FacilityID"].to_list()
            assert all(
                facility_ids[i] <= facility_ids[i + 1] for i in range(len(facility_ids) - 1)
            ), "FacilityID not sorted"

            con.close()
