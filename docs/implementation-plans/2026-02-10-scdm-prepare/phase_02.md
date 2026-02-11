# SCDM Prepare Implementation Plan — Phase 2: Schema Definitions + Ingestion

**Goal:** Define the 9 SCDM table schemas and build the ingestion layer that reads SAS7BDAT subsample files into temp parquet with samplenum tracking.

**Architecture:** `schema.py` defines static table metadata (column names, sort keys, crosswalk requirements) derived from `tables_documentation.json`. `ingest.py` uses pyreadstat chunked reading to convert SAS7BDAT files to temp parquet, injecting a `samplenum` column. Test fixtures use XPT format (pyreadstat can write XPT but not SAS7BDAT).

**Tech Stack:** Python 3.13.5, pyreadstat (SAS reading + XPT writing for tests), polars (DataFrame conversion + parquet writing)

**Scope:** 6 phases from original design (phase 2 of 6)

**Codebase verified:** 2026-02-10 — Phase 1 outputs expected (pyproject.toml, src/scdm_prepare/ package, CLI skeleton). Source data in `data/` directory follows pattern `{table_type}_{subsample_number}.sas7bdat` with 9 table types × 20 subsamples. tables_documentation.json confirmed with exact column definitions and sort orders for all 9 tables.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### scdm-prepare.AC1: Subsample Discovery
- **scdm-prepare.AC1.1 Success:** Tool auto-detects all subsample numbers from filenames in the input directory
- **scdm-prepare.AC1.2 Success:** `--first 5 --last 10` processes only subsamples 5-10
- **scdm-prepare.AC1.3 Success:** Omitting `--last` processes from `--first` through the highest detected subsample
- **scdm-prepare.AC1.4 Failure:** Missing subsample file within range fails fast with clear listing of all missing files
- **scdm-prepare.AC1.5 Failure:** Empty input directory or no matching SAS7BDAT files produces clear error

### scdm-prepare.AC2: Ingestion
- **scdm-prepare.AC2.1 Success:** Each SAS7BDAT file is read in chunks and written to temp parquet with samplenum column
- **scdm-prepare.AC2.2 Success:** Ingestion completes for all 9 table types across selected subsamples
- **scdm-prepare.AC2.3 Edge:** SAS date columns are preserved as date types (not raw numeric epoch values)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Create schema.py with SCDM table definitions

**Files:**
- Create: `src/scdm_prepare/schema.py`

**Implementation:**

Define a `TableDef` dataclass and a dictionary of all 9 SCDM table definitions. Each definition captures:
- `name`: lowercase table name (used for file naming)
- `columns`: ordered list of column names matching `tables_documentation.json`
- `sort_keys`: columns from `sort_order` in `tables_documentation.json`
- `crosswalk_ids`: dict mapping column name → join type (`"inner"` or `"left"`)

The exact column definitions per table (from `tables_documentation.json`):

| Table | Columns (in order) | Sort Keys | Crosswalk Joins |
|-------|-------------------|-----------|-----------------|
| enrollment | PatID, Enr_Start, Enr_End, MedCov, DrugCov, Chart, PlanType, PayerType | PatID, Enr_Start, Enr_End, MedCov, DrugCov, Chart | PatID=inner |
| demographic | PatID, Birth_Date, Sex, Hispanic, Race, PostalCode, PostalCode_Date, ImputedRace, ImputedHispanic | PatID | PatID=inner |
| dispensing | PatID, ProviderID, RxDate, Rx, Rx_CodeType, RxSup, RxAmt | PatID, RxDate | PatID=inner, ProviderID=left |
| encounter | PatID, EncounterID, ADate, DDate, EncType, FacilityID, Discharge_Disposition, Discharge_Status, DRG, DRG_Type, Admitting_Source | PatID, ADate | PatID=inner, EncounterID=left, FacilityID=left |
| diagnosis | PatID, EncounterID, ADate, ProviderID, EncType, DX, Dx_Codetype, OrigDX, PDX, PAdmit | PatID, ADate | PatID=inner, EncounterID=left, ProviderID=left |
| procedure | PatID, EncounterID, ADate, ProviderID, EncType, PX, PX_CodeType, OrigPX | PatID, ADate | PatID=inner, EncounterID=left, ProviderID=left |
| death | PatID, DeathDt, DtImpute, Source, Confidence | PatID | PatID=inner |
| provider | ProviderID, Specialty, Specialty_CodeType | ProviderID | (synthesised from crosswalk) |
| facility | FacilityID, Facility_Location | FacilityID | (synthesised from crosswalk) |

Also define a constant for the 9 source file table types that map to filenames in the data directory. The source files use the exact same lowercase table names: `enrollment`, `demographic`, `dispensing`, `encounter`, `diagnosis`, `procedure`, `death`, `provider`, `facility`.

**Note on source vs output columns:** The `columns` list in each `TableDef` defines the OUTPUT table schema. Source files for `provider` and `facility` have the same columns as the output, but the output tables are synthesised from crosswalks with hardcoded values (Phase 4), not copied from source data. The source files are still ingested to extract IDs for crosswalk generation.

Define the file extension constant (`.sas7bdat` for production, configurable for tests).

**Verification:**

Run: `uv run python -c "from scdm_prepare.schema import TABLES; print([t.name for t in TABLES.values()])"`
Expected: Prints all 9 table names.

**Commit:** `feat: add SCDM table schema definitions`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Unit test schema definitions

**Verifies:** None (schema is static configuration, but tests catch typos and structural issues)

**Files:**
- Create: `tests/test_schema.py`

**Testing:**
- Verify exactly 9 tables are defined
- Verify each table has non-empty columns and sort_keys
- Verify all sort_keys are a subset of columns for each table
- Verify all crosswalk_ids keys are present in the table's columns
- Verify column counts match tables_documentation.json (enrollment=8, demographic=9, dispensing=7, encounter=11, diagnosis=10, procedure=8, facility=2, provider=3, death=5)

**Verification:**

Run: `uv run pytest tests/test_schema.py -v`
Expected: All tests pass.

**Commit:** `test: add schema definition tests`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-6) -->
<!-- START_TASK_3 -->
### Task 3: Create test fixtures in conftest.py

**Files:**
- Create: `tests/conftest.py`

**Implementation:**

Write test fixtures as **parquet files** using polars directly. pyreadstat cannot write SAS7BDAT format and XPT format limits column names to 8 characters (too short for SCDM columns like `EncounterID`, `Discharge_Disposition`). Parquet fixtures bypass format limitations and test the ingestion logic (samplenum injection → temp parquet) cleanly.

Create a `sample_parquet_dir` fixture that:
1. Creates parquet files named `{table_type}_{samplenum}.parquet` for 3 subsamples
2. Uses correct SCDM column names from `schema.py`
3. Contains 10-20 rows per file with realistic data
4. Includes date columns, NULL IDs, and varying data types

**Verification:**

Run: `uv run pytest tests/conftest.py --co`
Expected: Fixtures are collected without errors.

**Commit:** `test: add synthetic test data fixtures`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Create ingest.py — subsample discovery

**Verifies:** scdm-prepare.AC1.1, scdm-prepare.AC1.2, scdm-prepare.AC1.3, scdm-prepare.AC1.4, scdm-prepare.AC1.5

**Files:**
- Create: `src/scdm_prepare/ingest.py`

**Implementation:**

Create a `discover_subsamples(input_dir, first, last, file_ext)` function that:
1. Scans `input_dir` for files matching `*_{N}.{ext}` pattern (where ext defaults to `.sas7bdat`)
2. Extracts all unique subsample numbers from filenames
3. Applies `first`/`last` range filtering:
   - If `first` is None, start from lowest detected subsample
   - If `last` is None, process through highest detected subsample
4. For each subsample in range, verifies ALL 9 table types have files present
5. Returns the list of validated subsample numbers

Error handling:
- Empty directory or no matching files → raise with clear error message (AC1.5)
- Missing file within range → raise with listing of ALL missing files (AC1.4)

The function takes `file_ext` parameter (defaults to `".sas7bdat"`) so tests can pass `".parquet"`.

Also create a helper `source_file_path(input_dir, table_name, samplenum, file_ext)` that returns the expected path for a given table/subsample combination.

**Testing:**
Tests must verify each AC listed above:
- scdm-prepare.AC1.1: Given a directory with files for subsamples 1-3, function returns [1, 2, 3]
- scdm-prepare.AC1.2: Given `first=2, last=3`, function returns [2, 3] only
- scdm-prepare.AC1.3: Given `first=2, last=None`, function returns [2, 3] (through highest)
- scdm-prepare.AC1.4: Remove one file, verify error lists the specific missing file(s)
- scdm-prepare.AC1.5: Empty directory raises clear error

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_ingest.py -v -k discovery`
Expected: All discovery tests pass.

**Commit:** `feat: add subsample discovery with range filtering`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Create ingest.py — chunked file reading and temp parquet writing

**Verifies:** scdm-prepare.AC2.1, scdm-prepare.AC2.2, scdm-prepare.AC2.3

**Files:**
- Modify: `src/scdm_prepare/ingest.py`

**Implementation:**

Add an `ingest_table(input_dir, table_name, subsamples, output_dir, file_ext, chunk_size)` function that:
1. For each subsample number, reads the source file in chunks
2. For SAS7BDAT files: uses `pyreadstat.read_file_in_chunks(pyreadstat.read_sas7bdat, path, chunksize=chunk_size)` — pyreadstat auto-converts SAS dates to Python datetime.date by default (AC2.3)
3. For parquet test files: reads with `polars.read_parquet()` (no chunking needed for small test files)
4. Injects a `samplenum` integer column into each chunk
5. Converts pandas DataFrame to polars DataFrame via `pl.from_pandas()`
6. Writes each chunk to temp parquet at `{output_dir}/_temp/{table_name}_{samplenum}.parquet`

Add an `ingest_all(input_dir, subsamples, output_dir, file_ext, chunk_size)` function that calls `ingest_table` for all 9 table types (AC2.2).

The `file_ext` parameter selects the reader:
- `".sas7bdat"` → pyreadstat chunked read (production)
- `".parquet"` → polars read_parquet (tests)

Temp parquet output goes to `{output_dir}/_temp/` directory, created automatically.

**Testing:**
Tests must verify each AC listed above:
- scdm-prepare.AC2.1: After ingestion, temp parquet files exist with `samplenum` column and correct values
- scdm-prepare.AC2.2: All 9 table types produce temp parquet files for each subsample
- scdm-prepare.AC2.3: Date columns in output parquet are date type (not int64 epoch values)

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_ingest.py -v`
Expected: All ingestion tests pass.

**Commit:** `feat: add chunked SAS7BDAT ingestion to temp parquet`
<!-- END_TASK_5 -->

<!-- START_TASK_6 -->
### Task 6: Integration smoke test for full ingestion pipeline

**Verifies:** scdm-prepare.AC2.1, scdm-prepare.AC2.2

**Files:**
- Create or modify: `tests/test_ingest.py`

**Testing:**
End-to-end test combining discovery + ingestion:
- Given the `sample_parquet_dir` fixture with 3 subsamples, run `discover_subsamples()` → `ingest_all()`
- Verify all 27 temp parquet files are created (9 tables × 3 subsamples)
- Verify each temp parquet file has the correct `samplenum` value
- Verify row counts match input fixtures
- Verify column names match schema definitions

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_ingest.py -v`
Expected: All tests pass including integration test.

**Commit:** `test: add ingestion integration smoke test`
<!-- END_TASK_6 -->
<!-- END_SUBCOMPONENT_B -->
