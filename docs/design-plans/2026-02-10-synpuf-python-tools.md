# SynPUF Python Tools Design

## Summary

This design introduces two Python CLI tools to replace existing SAS workflows for processing the CMS SynPUF dataset into SCDM format. The first tool (`synpuf prepare`) takes already-translated SCDM subsample files stored as SAS7BDAT, combines a user-specified range of those subsamples, generates sequential numeric IDs via crosswalk tables, and writes 9 SCDM-compliant parquet files. The second tool (`synpuf translate`) replicates the full translation pipeline from raw CMS SynPUF data — it processes beneficiary demographics, prescription drug events, and three claim types (outpatient, carrier, inpatient) through a 6-step transformation cascade that classifies diagnosis/procedure codes, generates enrollment timelines, and produces the same 9 SCDM output tables.

Both tools share IO utilities for reading SAS7BDAT files into polars DataFrames and a crosswalk module that maps original string identifiers to sequential integers. The architecture uses a modular pipeline design where each SAS macro from the original implementation maps to a dedicated Python module (e.g., `step_a_bene.py` corresponds to `step_a_process_bene_file.sas`), preserving traceability between the authoritative SAS logic and the new Python equivalents. Memory management is handled by sequential per-table processing — crosswalks stay in memory while individual tables are read, transformed, and written before moving to the next, avoiding the need to hold the entire 31GB dataset simultaneously.

## Definition of Done

1. A Python CLI tool (uv-managed) that replicates `prepare_scdm.sas` — reads SCDM subsample SAS7BDAT files from an input directory, combines a specified range of subsamples, generates SCDM-compliant numeric IDs via crosswalk, and writes 9 output parquet files (enrollment, demographic, dispensing, encounter, diagnosis, procedure, death, provider, facility).

2. A secondary Python CLI tool that replicates `translate_synpufs_to_scdm.sas` — translates raw CMS SynPUFs into SCDM format (for future use when raw data is available).

3. Both tools produce SCDM-spec-compliant output (correct column names, types, sort orders per the SAS originals).

## Acceptance Criteria

### synpuf-python-tools.AC1: Shared modules produce correct crosswalks and handle IO

- **synpuf-python-tools.AC1.1 Success:** `build_crosswalk` generates sequential integer IDs (1, 2, 3, ...) from sorted unique values of the source column, preserving group column associations.
- **synpuf-python-tools.AC1.2 Success:** `apply_crosswalk` replaces original IDs with crosswalk-mapped numeric IDs via left join, dropping the original column.
- **synpuf-python-tools.AC1.3 Success:** `read_sas_table` reads a SAS7BDAT file and returns a polars DataFrame with correct column names and types.
- **synpuf-python-tools.AC1.4 Success:** `read_sas_tables` concatenates multiple SAS7BDAT files into a single polars DataFrame.
- **synpuf-python-tools.AC1.5 Edge:** Crosswalk handles source values that appear in data but not in crosswalk (should not occur with correct pipeline, but should raise if it does).

### synpuf-python-tools.AC2: Prepare command replicates prepare_scdm.sas

- **synpuf-python-tools.AC2.1 Success:** `synpuf prepare` reads SAS7BDAT subsample files from the specified input directory for the given range (--first/--last).
- **synpuf-python-tools.AC2.2 Success:** Produces 9 output parquet files: enrollment, demographic, dispensing, encounter, diagnosis, procedure, death, provider, facility.
- **synpuf-python-tools.AC2.3 Success:** PatID crosswalk generates sequential numeric IDs unique across all combined subsamples.
- **synpuf-python-tools.AC2.4 Success:** EncounterID, ProviderID, and FacilityID crosswalks similarly generate unique sequential numeric IDs.
- **synpuf-python-tools.AC2.5 Success:** All 9 output tables have crosswalk-mapped numeric IDs replacing original string identifiers.
- **synpuf-python-tools.AC2.6 Failure:** Missing input directory or no matching subsample files produces a clear error message and non-zero exit code.
- **synpuf-python-tools.AC2.7 Failure:** --first greater than --last produces a clear error message.
- **synpuf-python-tools.AC2.8 Edge:** Running with --first 1 --last 1 (single subsample) produces valid output.

### synpuf-python-tools.AC3: Output is SCDM-spec-compliant

- **synpuf-python-tools.AC3.1 Success:** Output parquet files have column names matching SCDM spec (as defined in `tables_documentation.json`).
- **synpuf-python-tools.AC3.2 Success:** Output parquet files are sorted in SCDM-specified sort order per table.
- **synpuf-python-tools.AC3.3 Success:** Numeric ID columns (PatID, EncounterID, etc.) are integer type, not string.

### synpuf-python-tools.AC4: Translate command replicates translate_synpufs_to_scdm.sas

- **synpuf-python-tools.AC4.1 Success:** Code type classification algorithm implements all 6 cascade steps matching `codetype_algorithm.sas` logic (1:0, MANY:0, 1:1, MANY:1, 1:MANY, MANY:MANY).
- **synpuf-python-tools.AC4.2 Success:** Enrollment span algorithm produces correct coverage timelines from beneficiary month indicators, merging overlapping spans and combining MedCov/DrugCov via Rhoads status-change-date method.
- **synpuf-python-tools.AC4.3 Success:** All 6 translate steps (A through F) execute sequentially per subsample, producing intermediate and final SCDM tables.
- **synpuf-python-tools.AC4.4 Success:** `synpuf translate` processes the specified subsample range and writes 9 SCDM parquet files per subsample.
- **synpuf-python-tools.AC4.5 Failure:** Missing lookup table files produces a clear error message.

### synpuf-python-tools.AC5: Cross-Cutting Behaviours

- **synpuf-python-tools.AC5.1 Success:** Python prepare output matches SAS originals: same row counts per table, same non-ID column values.
- **synpuf-python-tools.AC5.2 Success:** Sort order of output matches SAS originals.
- **synpuf-python-tools.AC5.3 Success:** Progress feedback printed to stdout during processing (table names, subsample numbers being processed).

## Glossary

- **CMS SynPUF**: Centers for Medicare & Medicaid Services Synthetic Public Use Files — a de-identified dataset of Medicare claims used for research and testing.
- **SCDM**: Sentinel Common Data Model — a standardized schema for healthcare claims data that specifies table structures, column names, data types, and relationships.
- **Crosswalk**: A mapping table that translates original identifiers (often strings or non-sequential integers) into sequential numeric IDs for standardization.
- **Subsample**: One of multiple data partitions in the SynPUF dataset (numbered 1–20), each representing a 5% sample of the synthetic beneficiary population.
- **polars**: A high-performance DataFrame library for Python, used here for in-memory data transformations as an alternative to pandas.
- **pyreadstat**: A Python library for reading SAS7BDAT files, capable of outputting directly to polars DataFrames.
- **typer**: A Python library for building CLI applications with automatic help text generation and type validation.
- **uv**: A Python project and dependency manager that handles virtual environments and package installation.
- **SAS7BDAT**: The binary file format used by SAS to store datasets.
- **Code type classification**: An algorithm that determines the coding system (ICD-9, ICD-10, CPT, etc.) for diagnosis and procedure codes by matching against lookup tables through a 6-step cascade.
- **Enrollment span algorithm**: Logic that converts month-by-month coverage indicators (Part A, Part B, HMO, Drug) into continuous date ranges representing when a beneficiary had active coverage.
- **Rhoads status-change-date method**: A technique for merging medical and drug coverage timelines by detecting status changes and creating enrollment records at transition points.
- **EncounterID**: A unique numeric identifier for a healthcare encounter (visit) in SCDM.
- **PatID**: Patient identifier in SCDM.
- **ProviderID**: Healthcare provider identifier in SCDM.
- **FacilityID**: Healthcare facility identifier in SCDM.
- **PDE**: Part D Event — a prescription drug claim record in Medicare data.
- **Outpatient/Carrier/Inpatient Claims**: Three categories of Medicare claims data representing different healthcare service types (ambulatory visits, physician services, and hospital stays respectively).

## Architecture

Single uv-managed Python package with two typer subcommands: `synpuf prepare` and `synpuf translate`. Both share IO and crosswalk modules. Data flows through polars DataFrames, read from SAS7BDAT via pyreadstat, written as parquet.

Stack: polars (DataFrames), pyreadstat with `output_format='polars'` (SAS7BDAT reader), typer (CLI), pytest (testing). Managed by uv.

### Package Layout

```
src/synpuf/
    __init__.py
    cli.py              # typer app with prepare/translate subcommands
    io.py               # read_sas_table(), read_sas_tables(), write_parquet()
    crosswalk.py        # build_crosswalk(), apply_crosswalk()
    prepare.py          # prepare command logic
    translate/
        __init__.py
        step_a_bene.py      # Beneficiary → Demographic + Death + Enrollment
        step_b_pde.py       # PDE → Dispensing
        step_c_op.py        # Outpatient Claims → Encounter/Diagnosis/Procedure
        step_d_car.py       # Carrier Claims → Encounter/Diagnosis/Procedure
        step_e_ip.py        # Inpatient Claims → Encounter/Diagnosis/Procedure
        step_f_final.py     # Final processing, crosswalks, Provider/Facility
        codetype.py         # 6-step code type classification algorithm
        enrollment.py       # Enrollment span algorithm (coverage timelines)
tests/
    test_crosswalk.py
    test_codetype.py
    test_prepare.py         # integration test against real data
    test_enrollment.py
```

### Data Flow

**prepare command:** Input SCDM subsample SAS7BDAT files → discover files by glob pattern → build 4 crosswalks (PatID, EncounterID, ProviderID, FacilityID) → process 9 tables sequentially applying crosswalks → sort per SCDM spec → write parquet.

**translate command:** Raw CMS SynPUF SAS7BDAT files → 6-step pipeline per subsample (step_a through step_f) → intermediate polars DataFrames → 9 SCDM output parquet files per subsample.

Memory strategy: sequential per-table processing. Crosswalks stay in memory (small relative to data). Each table is read, transformed, and written before moving to the next. This avoids holding all 31GB simultaneously.

## Existing Patterns

Investigation found no existing Python infrastructure in this repository. The codebase is entirely SAS programs and SAS7BDAT data files, with a JSON schema (`tables_documentation.json`) defining SCDM table specifications.

This design introduces Python tooling as a new layer. Patterns adopted:

- **src layout** (`src/synpuf/`): standard Python packaging convention, avoids import ambiguity.
- **typer subcommands**: idiomatic CLI pattern for related tools sharing a namespace.
- **polars over pandas**: user-specified preference for performance and API clarity.
- **Module-per-SAS-macro mapping**: `step_a_bene.py` mirrors `step_a_process_bene_file.sas`, etc. This preserves traceability between SAS originals and Python equivalents.

The SAS code establishes the authoritative logic. Python modules replicate that logic faithfully — same column names, same sort orders, same ID generation strategy. The SAS programs remain the reference implementation.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Project Scaffolding

**Goal:** uv-managed Python package that installs and runs.

**Components:**
- `pyproject.toml` with dependencies (polars, pyreadstat, typer) and dev dependencies (pytest)
- `src/synpuf/__init__.py` entry point
- `src/synpuf/cli.py` with typer app skeleton (prepare and translate subcommands, no logic)
- `.python-version` file

**Dependencies:** None (first phase)

**Done when:** `uv sync` succeeds, `uv run synpuf --help` prints usage with prepare/translate subcommands.
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: IO and Crosswalk Modules

**Goal:** Shared IO utilities for reading SAS7BDAT into polars and writing parquet. Shared crosswalk module for building and applying sequential numeric ID mappings.

**Components:**
- `src/synpuf/io.py` — `read_sas_table(path) -> pl.DataFrame`, `read_sas_tables(paths) -> pl.DataFrame` (concat), `write_parquet(df, path)`
- `src/synpuf/crosswalk.py` — `build_crosswalk(df, old_col, group_cols) -> pl.DataFrame` (sorted unique values → sequential integer), `apply_crosswalk(df, crosswalk, old_col, new_col) -> pl.DataFrame` (left join + drop old)
- `tests/test_crosswalk.py` — unit tests for crosswalk build/apply with edge cases (duplicates, missing values, multiple group columns)

**ACs covered:** `synpuf-python-tools.AC1.1`, `synpuf-python-tools.AC1.2`

**Dependencies:** Phase 1 (project setup)

**Done when:** crosswalk unit tests pass, IO functions can round-trip a small SAS7BDAT file to parquet.
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Prepare Command

**Goal:** Fully functional `synpuf prepare` command replicating `prepare_scdm.sas`.

**Components:**
- `src/synpuf/prepare.py` — discovers subsample files, builds 4 crosswalks (PatID, EncounterID, ProviderID, FacilityID) from combined data, processes each of 9 SCDM tables by: reading all subsamples for that table, concatenating, applying relevant crosswalks, sorting per SCDM spec, writing parquet
- Wire `prepare.py` into `cli.py` subcommand with `--input-dir`, `--output-dir`, `--first`, `--last` options
- `tests/test_prepare.py` — integration test running prepare against real subsample data (small range), verifying output parquet files exist with correct columns and sort orders

**ACs covered:** `synpuf-python-tools.AC2.1` through `synpuf-python-tools.AC2.5`, `synpuf-python-tools.AC3.1`, `synpuf-python-tools.AC3.2`

**Dependencies:** Phase 2 (IO and crosswalk modules)

**Done when:** `uv run synpuf prepare --input-dir data/ --output-dir output/ --first 1 --last 1` produces 9 parquet files with SCDM-compliant columns and sequential numeric IDs. Integration tests pass.
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Code Type Classification

**Goal:** Python implementation of the 6-step code type classification algorithm from `codetype_algorithm.sas`.

**Components:**
- `src/synpuf/translate/codetype.py` — `classify_codes(temptable, codes_lookup) -> (dx_df, px_df)` implementing the 6-step cascade: (1) 1:0 match → source category, type=OT, (2) MANY:0 → same, (3) 1:1 → use lookup, (4) MANY:1 → use lookup, (5) 1:MANY → prefer matching source category, (6) MANY:MANY → prefer matching. Handles leftover codes.
- `tests/test_codetype.py` — unit tests covering each cascade step with known inputs/outputs

**ACs covered:** `synpuf-python-tools.AC4.1`

**Dependencies:** Phase 2 (IO module for reading lookup tables)

**Done when:** codetype unit tests pass covering all 6 cascade steps.
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Enrollment Span Algorithm

**Goal:** Python implementation of the enrollment span algorithm from `step_a_process_bene_file.sas` and `create_episodes_spans_2.sas`.

**Components:**
- `src/synpuf/translate/enrollment.py` — coverage span logic: summarize Part A/B/HMO/Drug month indicators per year → generate date ranges → merge overlapping spans (polars equivalent of `create_episodes_spans_2`) → combine MedCov/DrugCov timelines using Rhoads status-change-date method → produce SCDM enrollment records
- `tests/test_enrollment.py` — unit tests for span merging, coverage timeline generation, edge cases (gaps, overlapping periods, single-month coverage)

**ACs covered:** `synpuf-python-tools.AC4.2`

**Dependencies:** Phase 2 (IO module)

**Done when:** enrollment span unit tests pass, producing correct SCDM enrollment records from test beneficiary data.
<!-- END_PHASE_5 -->

<!-- START_PHASE_6 -->
### Phase 6: Translate Steps A–F

**Goal:** Complete translate pipeline replicating all 6 SAS macro steps.

**Components:**
- `src/synpuf/translate/step_a_bene.py` — Beneficiary → Demographic + Death + Enrollment (uses enrollment.py)
- `src/synpuf/translate/step_b_pde.py` — PDE → Dispensing (filter, group, sum)
- `src/synpuf/translate/step_c_op.py` — Outpatient Claims → Encounter/Diagnosis/Procedure (unpivot wide arrays, codetype classification, EncType via ED/Home/IS lookups)
- `src/synpuf/translate/step_d_car.py` — Carrier Claims → same pattern as OP with different source columns
- `src/synpuf/translate/step_e_ip.py` — Inpatient Claims → same pattern, includes DDate/DRG/Discharge, EncType via OA overlap + stay duration
- `src/synpuf/translate/step_f_final.py` — concatenate OP/CAR/IP temps, build crosswalks (PatID, EncounterID, FacilityID, ProviderID), create Provider/Facility tables, write 9 SCDM tables per subsample
- Wire all steps into `cli.py` translate subcommand with `--input-dir`, `--output-dir`, `--lookup-dir`, `--first`, `--last` options

**ACs covered:** `synpuf-python-tools.AC4.3`, `synpuf-python-tools.AC4.4`, `synpuf-python-tools.AC3.1`, `synpuf-python-tools.AC3.2`

**Dependencies:** Phase 4 (codetype), Phase 5 (enrollment), Phase 2 (IO/crosswalk)

**Done when:** `uv run synpuf translate` runs through all 6 steps for a single subsample without error. Output tables have correct SCDM columns and sort orders.
<!-- END_PHASE_6 -->

<!-- START_PHASE_7 -->
### Phase 7: Regression Testing

**Goal:** Verify Python output matches SAS originals for both tools.

**Components:**
- `tests/test_regression.py` — compare parquet output from `synpuf prepare` against SAS7BDAT originals in `data/`: same row counts, same column values (accounting for ID remapping), same sort orders
- Integration test for translate pipeline (if raw CMS data available)
- CI-friendly test markers (skip regression tests when data directory absent)

**ACs covered:** `synpuf-python-tools.AC5.1`, `synpuf-python-tools.AC5.2`

**Dependencies:** Phase 3 (prepare command), Phase 6 (translate steps)

**Done when:** regression tests pass comparing Python prepare output to SAS originals. Row counts and non-ID column values match.
<!-- END_PHASE_7 -->

## Additional Considerations

**Memory:** The 31GB dataset processes sequentially per table, not all at once. Crosswalks (4 mapping tables of unique IDs) remain in memory — these are small relative to the full dataset. If memory becomes an issue for very large tables, polars supports lazy evaluation and streaming, but this is unlikely to be needed for individual tables.

**SAS7BDAT output:** pyreadstat supports `write_sas7bdat()`. If downstream consumers need SAS-format output, this can be added as an output format option in `io.py` without architectural changes. The design keeps IO as a thin module specifically to enable this.

**Date handling:** SAS stores dates as days since 1960-01-01. pyreadstat auto-converts these to Python datetime objects when reading. polars handles datetime natively. No manual date conversion needed.

**Lookup tables:** The translate command requires 4 lookup tables (`clinical_codes.sas7bdat`, `ed_codes.sas7bdat`, `home_codes.sas7bdat`, `is_codes.sas7bdat`) from `translational_code/inputfiles/`. These are read once at pipeline start and passed to steps that need them.
