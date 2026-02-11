import tempfile
from pathlib import Path

import polars as pl
import pytest

from scdm_prepare.schema import TABLES


@pytest.fixture
def sample_parquet_dir():
    """Create synthetic test parquet files for 3 subsamples."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create parquet files for subsamples 1, 2, 3 with realistic data
        for samplenum in [1, 2, 3]:
            for table_name, table_def in TABLES.items():
                # Create test data with correct columns
                data = {}
                for col in table_def.columns:
                    if col.endswith("Date") or col == "Birth_Date" or col == "Enr_Start" or col == "Enr_End" or col == "ADate" or col == "DDate" or col == "RxDate" or col == "PostalCode_Date" or col == "DeathDt":
                        # Date columns
                        data[col] = [
                            "2020-01-15",
                            "2020-02-20",
                            None,
                            "2020-06-10",
                            None,
                            "2020-12-31",
                            "2021-03-15",
                            "2021-05-20",
                            "2021-08-10",
                            "2021-11-05",
                            "2020-04-15",
                            "2020-07-20",
                            "2020-09-10",
                            "2020-10-05",
                            "2021-02-15",
                            "2021-06-20",
                            "2021-09-10",
                            "2021-10-05",
                            "2020-01-01",
                            "2021-12-31",
                        ][:20]
                    elif col in ["PatID", "ProviderID", "EncounterID", "FacilityID"]:
                        # ID columns (may have nulls for some)
                        base_id = 100 + samplenum * 1000 + hash(col) % 100
                        values = [
                            base_id + i if i % 5 != 0 else None
                            for i in range(20)
                        ]
                        data[col] = values
                    elif col in ["Sex", "MedCov", "DrugCov", "Chart", "EncType"]:
                        # Categorical single character
                        data[col] = ["M" if i % 2 == 0 else "F" for i in range(20)] if col == "Sex" else ["Y" if i % 2 == 0 else "N" for i in range(20)]
                    elif col in ["Hispanic", "ImputedHispanic", "ImputedRace"]:
                        # Binary flags
                        data[col] = ["Y" if i % 3 == 0 else "N" for i in range(20)]
                    elif col in ["Race"]:
                        # Race codes
                        races = ["W", "B", "A", "I", "M"]
                        data[col] = [races[i % len(races)] for i in range(20)]
                    elif col in ["PostalCode"]:
                        # Postal codes
                        data[col] = [f"{10000 + i * 100}" for i in range(20)]
                    elif col in ["DtImpute", "Specialty_CodeType", "Rx_CodeType", "Dx_Codetype", "PX_CodeType", "DRG_Type"]:
                        # Code type columns
                        data[col] = ["ICD9CM" if i % 2 == 0 else "ICD10CM" for i in range(20)]
                    elif col in ["Rx", "DX", "OrigDX", "PX", "OrigPX"]:
                        # Diagnosis/procedure/rx codes
                        data[col] = [f"CODE{i % 5:02d}" for i in range(20)]
                    elif col in ["RxSup", "RxAmt", "DRG", "Discharge_Status"]:
                        # Numeric columns
                        data[col] = [i * 10 for i in range(20)]
                    elif col in ["Discharge_Disposition", "Source", "Confidence", "Specialty"]:
                        # Text fields
                        data[col] = [f"Value_{i % 3}" for i in range(20)]
                    elif col in ["Admitting_Source"]:
                        # Another text field
                        data[col] = [f"Source_{i % 4}" for i in range(20)]
                    elif col in ["PayerType", "PlanType", "Facility_Location"]:
                        # Plan/facility type
                        data[col] = [f"Type_{i % 3}" for i in range(20)]
                    elif col in ["PDX", "PAdmit"]:
                        # Flags
                        data[col] = ["Y" if i % 4 == 0 else "N" for i in range(20)]
                    else:
                        # Default: string values
                        data[col] = [f"{col}_{i}" for i in range(20)]

                # Create DataFrame with correct types for dates
                df_dict = {}
                for col, values in data.items():
                    if col.endswith("Date") or col == "Birth_Date" or col == "Enr_Start" or col == "Enr_End" or col == "ADate" or col == "DDate" or col == "RxDate" or col == "PostalCode_Date" or col == "DeathDt":
                        # Parse dates
                        df_dict[col] = pl.Series(
                            col,
                            [
                                pl.datetime(2020, 1, 15) if v == "2020-01-15" else
                                pl.datetime(2020, 2, 20) if v == "2020-02-20" else
                                pl.datetime(2020, 6, 10) if v == "2020-06-10" else
                                pl.datetime(2020, 12, 31) if v == "2020-12-31" else
                                pl.datetime(2021, 3, 15) if v == "2021-03-15" else
                                pl.datetime(2021, 5, 20) if v == "2021-05-20" else
                                pl.datetime(2021, 8, 10) if v == "2021-08-10" else
                                pl.datetime(2021, 11, 5) if v == "2021-11-05" else
                                pl.datetime(2020, 4, 15) if v == "2020-04-15" else
                                pl.datetime(2020, 7, 20) if v == "2020-07-20" else
                                pl.datetime(2020, 9, 10) if v == "2020-09-10" else
                                pl.datetime(2020, 10, 5) if v == "2020-10-05" else
                                pl.datetime(2021, 2, 15) if v == "2021-02-15" else
                                pl.datetime(2021, 6, 20) if v == "2021-06-20" else
                                pl.datetime(2021, 9, 10) if v == "2021-09-10" else
                                pl.datetime(2021, 10, 5) if v == "2021-10-05" else
                                pl.datetime(2020, 1, 1) if v == "2020-01-01" else
                                pl.datetime(2021, 12, 31) if v == "2021-12-31" else
                                None
                                for v in values
                            ],
                        ).cast(pl.Date)
                    else:
                        df_dict[col] = pl.Series(col, values)

                df = pl.DataFrame(df_dict)

                # Write to parquet
                output_path = tmpdir_path / f"{table_name}_{samplenum}.parquet"
                df.write_parquet(str(output_path))

        yield tmpdir_path
