# SCDM Prepare CLI Design

## Summary

**scdm-prepare** is a Python CLI tool that combines SynPUF subsamples into 9 standardised SCDM (Sentinel Common Data Model) tables for research use. The SynPUF dataset ships as ~180 SAS files split across 20 subsamples — each subsample contains the same 9 table types (enrollment, demographic, etc.), and ID values are scoped per-subsample rather than globally unique. This creates a problem: combining subsamples naively would result in ID collisions across patient, encounter, provider, and facility identifiers.

The tool solves this via a three-stage pipeline: **Ingest** reads SAS7BDAT files in memory-efficient chunks and writes temporary parquet files; **Transform** uses DuckDB SQL to union subsamples, build ID crosswalk tables that assign globally unique sequential IDs, and assemble the 9 SCDM output tables with correct columns, joins, and sort orders; **Export** writes the final tables in parquet, CSV, or JSON format. The design prioritises low memory footprint (DuckDB spills to disk, pyreadstat chunks), disk-based intermediate storage (temp parquet files), and fidelity to the existing SAS reference implementation — column selections, join types, and sort orders match the SAS code exactly.

## Definition of Done

1. **A pip/uv-installable Python CLI** (`pyproject.toml` + Typer entry point) that reads SynPUF SAS7BDAT subsample files from a data directory.
2. **Combines 1-N subsamples** into 9 SCDM tables (enrollment, demographic, dispensing, encounter, diagnosis, procedure, death, provider, facility) with reassigned unique IDs via crosswalk logic. Auto-detects how many subsamples exist.
3. **Outputs in user-chosen format** (parquet, CSV, or JSON).
4. **Handles 31GB+ without blowing memory** via DuckDB as the processing engine.
5. **CLI args:** input dir, output dir, format, first/last subsample range.
6. **Output data is SCDM-compliant** per tables_documentation.json (correct columns, sort orders, types) but the tool does not validate/enforce rules.

**Out of scope:** Source file cleanup/deletion, tables not produced by the SAS code (Prescribing, Lab, Vital Signs, etc.), data validation/enforcement, GUI.

## Acceptance Criteria

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

### scdm-prepare.AC3: ID Uniqueness
- **scdm-prepare.AC3.1 Success:** Crosswalks assign globally unique sequential IDs across subsamples (same original ID in different subsamples gets different new IDs)
- **scdm-prepare.AC3.2 Success:** NULL/missing original IDs map to NULL in crosswalk output
- **scdm-prepare.AC3.3 Success:** Four crosswalks produced: patid, encounterid, providerid, facilityid

### scdm-prepare.AC4: Table Structure
- **scdm-prepare.AC4.1 Success:** Each of the 9 output tables contains exactly the columns specified by the SAS code's SELECT statements
- **scdm-prepare.AC4.2 Success:** Column order matches the SAS code's SELECT order
- **scdm-prepare.AC4.3 Success:** Join types match SAS code (INNER for PatID, LEFT for EncounterID/ProviderID/FacilityID)

### scdm-prepare.AC5: Sort Orders
- **scdm-prepare.AC5.1 Success:** Each output table is sorted per tables_documentation.json sort_order specification

### scdm-prepare.AC6: Synthesised Tables
- **scdm-prepare.AC6.1 Success:** Provider table built from providerid_crosswalk with Specialty='99' and Specialty_CodeType='2'
- **scdm-prepare.AC6.2 Success:** Facility table built from facilityid_crosswalk with empty Facility_Location
- **scdm-prepare.AC6.3 Success:** Provider/Facility exclude rows where original ID was NULL

### scdm-prepare.AC7: Output Formats
- **scdm-prepare.AC7.1 Success:** `--format parquet` produces readable .parquet files
- **scdm-prepare.AC7.2 Success:** `--format csv` produces CSV with headers
- **scdm-prepare.AC7.3 Success:** `--format json` produces valid NDJSON files
- **scdm-prepare.AC7.4 Success:** Output files are named `{table}.{ext}` (e.g., enrollment.parquet)

### scdm-prepare.AC8: CLI Interface
- **scdm-prepare.AC8.1 Success:** `scdm-prepare --help` prints usage with all arguments
- **scdm-prepare.AC8.2 Success:** `--input`, `--output`, `--format` are required; `--first`, `--last` are optional
- **scdm-prepare.AC8.3 Failure:** Invalid `--format` value produces clear error
- **scdm-prepare.AC8.4 Failure:** Non-existent `--input` directory produces clear error

### scdm-prepare.AC9: Progress Reporting
- **scdm-prepare.AC9.1 Success:** Ingestion reports per-file progress
- **scdm-prepare.AC9.2 Success:** Transform and export report per-table progress

### scdm-prepare.AC10: Error Handling & Cleanup
- **scdm-prepare.AC10.1 Success:** Temp parquet files cleaned up on successful completion
- **scdm-prepare.AC10.2 Success:** Temp files left in place on failure for debugging
- **scdm-prepare.AC10.3 Success:** `--clean-temp` flag removes leftover temp files
- **scdm-prepare.AC10.4 Failure:** Corrupt SAS7BDAT file reports which specific file failed

## Glossary

- **SCDM (Sentinel Common Data Model)**: A standardised schema for healthcare claims and EHR data used by the FDA Sentinel System. Defines table structures, column names, and sort orders for interoperability across research institutions.
- **SynPUF (Synthetic Public Use Files)**: De-identified, synthetic Medicare claims dataset created by CMS for public research use. The dataset ships in 20 subsamples to mimic real-world data distribution patterns.
- **Subsample**: A numbered partition of the SynPUF dataset (1-20). Each subsample contains ~100,000 unique beneficiaries and all their associated records across 9 table types. Subsamples are independent — combining them requires ID reassignment to avoid collisions.
- **Crosswalk table**: A lookup table mapping original ID values (scoped per-subsample) to new globally unique sequential IDs. Four crosswalks are required: PatID, EncounterID, ProviderID, FacilityID.
- **SAS7BDAT**: Binary file format used by SAS statistical software. Contains tabular data with typed columns and metadata. Requires specialised libraries (pyreadstat) to read from Python.
- **DuckDB**: In-process analytical SQL database optimised for large-scale data transformations. Runs queries directly against parquet files without loading entire datasets into memory.
- **Polars**: Modern DataFrame library for Python. Used here as an intermediate format between pyreadstat and DuckDB, which can query Polars DataFrames zero-copy via Arrow.
- **Parquet**: Columnar binary file format designed for analytical workloads. Compresses significantly better than CSV and preserves data types. Used for temp storage and optionally as output format.
- **NDJSON (Newline-Delimited JSON)**: Text format where each line is a valid JSON object. Allows streaming writes without holding entire datasets in memory.
- **Typer**: Modern Python CLI framework built on top of Click. Uses type hints to auto-generate argument parsing and help text.
- **pyreadstat**: Python library for reading SAS, SPSS, and Stata files. Handles SAS date conversion and chunked reads to manage memory.
- **Chunked read**: Reading a file in fixed-size batches (e.g., 100,000 rows at a time) rather than loading the entire file into memory.

## Architecture

Three-stage pipeline: **Ingest → Transform → Export**. DuckDB is the processing engine for all heavy operations.

**Stage 1 — Ingest** (`src/scdm_prepare/ingest.py`): For each of the 9 table types, iterate over the selected subsample range. pyreadstat reads each SAS7BDAT file in configurable chunks (~100k rows), converts to Polars DataFrames, and writes to temporary parquet files partitioned by table name. A `samplenum` column is injected into each chunk to track subsample origin. After ingestion, DuckDB registers views over the temp parquet files via glob patterns — zero additional memory cost.

**Stage 2 — Transform** (`src/scdm_prepare/transform.py`): All transformation logic runs as DuckDB SQL. Three sub-steps:

1. **Union subsamples** — DuckDB reads all parquet files for a given table type using `read_parquet('_temp/{table}_*.parquet')`.
2. **Build crosswalks** — For each ID type (PatID, EncounterID, ProviderID, FacilityID): `SELECT DISTINCT` the original ID + samplenum, assign `ROW_NUMBER() OVER (ORDER BY samplenum, original_id)` as the new sequential ID. NULL original IDs map to NULL in the output (matching SAS `.U` special missing). This replaces the SAS `assign_idvar` macro.
3. **Join crosswalks + select columns** — Each output table is a SQL query joining unioned data with crosswalks, selecting only the SCDM-specified columns, and applying `ORDER BY` per the table's sort specification. Join types match the SAS code: INNER JOIN for PatID, LEFT JOIN for EncounterID/ProviderID/FacilityID.

Provider and Facility tables are special cases — synthesised from crosswalks rather than source data. Provider gets `Specialty='99'`, `Specialty_CodeType='2'`. Facility gets an empty `Facility_Location`.

**Stage 3 — Export** (`src/scdm_prepare/export.py`): Format-specific writers:
- **Parquet/CSV:** DuckDB `COPY` command — native streaming write, no memory spike.
- **JSON:** Query results fetched in batches → Polars → `write_ndjson()` in append mode. NDJSON (newline-delimited) is used instead of standard JSON arrays to avoid holding entire tables in memory.

**Temp storage:** Parquet intermediates land in `{output_dir}/_temp/`. Cleaned up on success. Left in place on failure for debugging. A `--clean-temp` flag allows manual cleanup.

**Data flow diagram:**

```
SAS7BDAT files (data/)
    │
    ▼ pyreadstat (chunked read + samplenum injection)
Temp Parquet (_temp/)
    │
    ▼ DuckDB SQL (union → crosswalks → joins → sort)
DuckDB Tables (in-process)
    │
    ▼ COPY / Polars write
Output files (parquet/csv/json)
```

## Existing Patterns

This is a greenfield project — no existing Python code in the repository. The SAS codebase (`translational_code/`) provides the reference implementation but no Python patterns to follow.

The SAS code's structure informed the pipeline design:
- `prepare_scdm.sas` → maps to `cli.py` (orchestration) + `transform.py` (SQL logic)
- `assign_idvar.sas` macro → maps to crosswalk generation in `transform.py`
- `assign_max_varlength.sas` macro → not needed (Polars/DuckDB handle variable-length strings natively)
- `assign_sort_order.sas` macro → maps to `ORDER BY` clauses in `transform.py`

The `tables_documentation.json` in the repository root serves as the schema reference. Column selections, sort orders, and join patterns in `transform.py` must match both the documentation and the SAS code's actual `proc sql` SELECT statements.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Project Scaffolding

**Goal:** Installable package with working CLI entry point.

**Components:**
- `pyproject.toml` — uv-managed, Python 3.13.5, dependencies (duckdb, polars, pyreadstat, typer), `[project.scripts]` entry point
- `src/scdm_prepare/__init__.py` — package init
- `src/scdm_prepare/cli.py` — Typer app skeleton with `--input`, `--output`, `--format`, `--first`, `--last` arguments
- `.python-version` — pins 3.13.5

**Dependencies:** None (first phase)

**Done when:** `uv sync` succeeds, `scdm-prepare --help` prints usage, arguments are parsed and validated (input dir exists, format is one of parquet/csv/json)
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Schema Definitions + Ingestion

**Goal:** SAS7BDAT files can be read and converted to temp parquet with correct table awareness.

**Components:**
- `src/scdm_prepare/schema.py` — 9 SCDM table definitions: column names, column order, sort keys, ID crosswalk requirements. Derived from `tables_documentation.json` and the SAS code's SELECT statements. Static configuration, not runtime validation.
- `src/scdm_prepare/ingest.py` — pyreadstat chunked reader. For each table type in the selected subsample range: read SAS7BDAT in chunks, inject `samplenum`, write temp parquet files to `{output_dir}/_temp/{table}_{samplenum}.parquet`.
- `tests/conftest.py` — Fixtures that create small synthetic SAS7BDAT files (2-3 subsamples, ~100 rows each) using pyreadstat
- `tests/test_ingest.py` — Verifies chunked read, samplenum injection, temp parquet output

**Dependencies:** Phase 1 (project setup)

**Covers:** scdm-prepare.AC1 (subsample discovery), scdm-prepare.AC2 (ingestion)

**Done when:** Given synthetic SAS7BDAT fixtures, ingestion produces correctly structured temp parquet files with samplenum column. Tests pass.
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Crosswalk Generation

**Goal:** DuckDB produces correct ID crosswalk tables from ingested data.

**Components:**
- `src/scdm_prepare/transform.py` — Crosswalk generation: DuckDB SQL that reads temp parquet, builds 4 crosswalk tables (patid_crosswalk, encounterid_crosswalk, providerid_crosswalk, facilityid_crosswalk). Each crosswalk contains `orig_{id}`, `samplenum`, and the new sequential `{id}`.
- `tests/test_transform.py` — Verifies crosswalks assign unique sequential IDs, handle NULL IDs correctly, preserve samplenum scoping (same original ID in different subsamples gets different new IDs)

**Dependencies:** Phase 2 (ingestion produces temp parquet)

**Covers:** scdm-prepare.AC3 (ID uniqueness)

**Done when:** Crosswalk tables produce correct sequential IDs across subsamples. NULL original IDs map to NULL. Tests pass.
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Table Assembly

**Goal:** All 9 SCDM output tables are assembled with correct columns, joined IDs, and sort orders.

**Components:**
- `src/scdm_prepare/transform.py` — Table assembly SQL for all 9 tables. Each table query joins unioned subsample data with relevant crosswalks, selects SCDM-specified columns in correct order, applies ORDER BY per schema. Provider and Facility tables synthesised from crosswalks with hardcoded values.
- `tests/test_transform.py` — Verifies each output table has correct columns, correct column order, correct join behaviour (inner vs left), correct sort order, and correct synthetic values for Provider/Facility

**Dependencies:** Phase 3 (crosswalks exist)

**Covers:** scdm-prepare.AC4 (table structure), scdm-prepare.AC5 (sort orders), scdm-prepare.AC6 (Provider/Facility synthesis)

**Done when:** All 9 tables assemble correctly from synthetic fixtures. Column selection, join types, sort orders, and synthetic values match the SAS code's behaviour. Tests pass.
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Export Layer

**Goal:** Assembled tables can be written to all supported output formats.

**Components:**
- `src/scdm_prepare/export.py` — Three export strategies: DuckDB `COPY` for parquet and CSV, batched Polars `write_ndjson()` for JSON. Each writer takes a DuckDB connection and table name, writes to `{output_dir}/{table}.{ext}`.
- `tests/test_export.py` — Verifies each format produces valid, readable output. Parquet readable by Polars. CSV has headers. JSON is valid NDJSON.

**Dependencies:** Phase 4 (assembled tables to export)

**Covers:** scdm-prepare.AC7 (output formats)

**Done when:** All three formats produce valid output files that can be read back and contain the expected data. Tests pass.
<!-- END_PHASE_5 -->

<!-- START_PHASE_6 -->
### Phase 6: CLI Integration, Progress Reporting, and Error Handling

**Goal:** Full end-to-end orchestration with user-facing progress and robust error handling.

**Components:**
- `src/scdm_prepare/cli.py` — Full orchestration: validate inputs → ingest → transform → export → cleanup temp. Wires all stages together.
- `src/scdm_prepare/progress.py` — Progress reporting: per-file during ingestion, per-table during transform and export. Uses Typer/rich if available, plain text fallback.
- Error handling across all stages: missing files fail fast with clear listing, corrupt SAS7BDAT reports which file failed, disk space warning at startup, `--clean-temp` flag for manual cleanup of failed runs.
- `tests/test_cli.py` — End-to-end smoke tests: full pipeline from synthetic SAS7BDAT fixtures to output files in each format. Error case tests: missing subsample file, invalid format argument.

**Dependencies:** Phases 1-5 (all components)

**Covers:** scdm-prepare.AC8 (CLI interface), scdm-prepare.AC9 (progress), scdm-prepare.AC10 (error handling)

**Done when:** `scdm-prepare` runs end-to-end on synthetic fixtures, produces correct output in all formats, reports progress, and handles error cases gracefully. Tests pass.
<!-- END_PHASE_6 -->

## Additional Considerations

**Memory budget:** The largest single table (diagnosis) is ~17GB across 20 subsamples (~850MB per file). pyreadstat chunks at ~100k rows keep ingestion memory under ~200MB. DuckDB's query engine spills to disk when memory pressure is high — its default memory limit is 80% of system RAM, which is configurable via `SET memory_limit`.

**Temp disk usage:** Parquet intermediates compress significantly vs SAS7BDAT. Expect temp storage to be roughly 30-50% of input size. The tool should warn at startup if the output directory has less free space than the input directory's total size.

**SAS date handling:** SAS stores dates as days since January 1, 1960. pyreadstat converts these to Python `datetime.date` objects by default. DuckDB and Polars handle date types natively — no manual epoch conversion needed.

**NDJSON for JSON output:** Standard JSON arrays require the entire dataset in memory to write the closing bracket. NDJSON (one JSON object per line) streams naturally and is widely supported by data tools (jq, Polars, pandas, DuckDB itself).
