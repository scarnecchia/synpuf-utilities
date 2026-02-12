import datetime
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
                    if col.endswith("Date") or col in ["Birth_Date", "Enr_Start", "Enr_End", "ADate", "DDate", "RxDate", "PostalCode_Date", "DeathDt"]:
                        # Date columns
                        data[col] = [
                            datetime.date(2020, 1, 15),
                            datetime.date(2020, 2, 20),
                            None,
                            datetime.date(2020, 6, 10),
                            None,
                            datetime.date(2020, 12, 31),
                            datetime.date(2021, 3, 15),
                            datetime.date(2021, 5, 20),
                            datetime.date(2021, 8, 10),
                            datetime.date(2021, 11, 5),
                            datetime.date(2020, 4, 15),
                            datetime.date(2020, 7, 20),
                            datetime.date(2020, 9, 10),
                            datetime.date(2020, 10, 5),
                            datetime.date(2021, 2, 15),
                            datetime.date(2021, 6, 20),
                            datetime.date(2021, 9, 10),
                            datetime.date(2021, 10, 5),
                            datetime.date(2020, 1, 1),
                            datetime.date(2021, 12, 31),
                        ]
                    elif col in ["PatID", "ProviderID", "EncounterID", "FacilityID"]:
                        # ID columns (may have nulls for some)
                        base_id = 100 + samplenum * 1000 + hash(col) % 100
                        values = [
                            base_id + i if i % 5 != 0 else None
                            for i in range(20)
                        ]
                        data[col] = values
                    elif col in ["Sex"]:
                        # Sex
                        data[col] = ["M" if i % 2 == 0 else "F" for i in range(20)]
                    elif col in ["MedCov", "DrugCov", "Chart", "EncType", "Hispanic", "ImputedHispanic", "ImputedRace", "PDX", "PAdmit"]:
                        # Binary/categorical single character
                        data[col] = ["Y" if i % 2 == 0 else "N" for i in range(20)]
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
                    else:
                        # Default: string values
                        data[col] = [f"{col}_{i}" for i in range(20)]

                # Create DataFrame
                df = pl.DataFrame(data)

                # Write to parquet
                output_path = tmpdir_path / f"{table_name}_{samplenum}.parquet"
                df.write_parquet(str(output_path))

        yield tmpdir_path
