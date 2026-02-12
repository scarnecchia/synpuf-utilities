# SCDM Prepare Test Requirements

This document maps every acceptance criterion from the [design plan](../../../docs/design-plans/2026-02-10-scdm-prepare.md) to either an automated test or a documented human verification step. Criteria use the format `scdm-prepare.AC{N}.{M}`.

---

## Test Infrastructure

| Item | Detail |
|------|--------|
| Test runner | pytest (via `uv run pytest`) |
| Test fixtures | Parquet-based synthetic data in `tests/conftest.py` (3 subsamples, ~10-20 rows each, all 9 table types). SAS7BDAT writing is not possible via pyreadstat; XPT truncates column names. Parquet fixtures bypass format limitations. |
| Hidden CLI flag | `--file-ext` (hidden from `--help`) defaults to `.sas7bdat`; set to `.parquet` for test fixtures. Threads through discovery and ingestion. |
| DuckDB test pattern | Tests create in-memory DuckDB connections and temp parquet files directly via polars `write_parquet()`. |

---

## Automated Test Mapping

### scdm-prepare.AC1: Subsample Discovery

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC1.1 | Auto-detects all subsample numbers from filenames | Unit | `tests/test_ingest.py::test_discover_all_subsamples` | 2, Task 4 | Given files for subsamples 1-3, `discover_subsamples()` returns `[1, 2, 3]`. Uses parquet fixtures with `file_ext=".parquet"`. |
| scdm-prepare.AC1.2 | `--first 5 --last 10` processes only subsamples 5-10 | Unit | `tests/test_ingest.py::test_discover_subsamples_range` | 2, Task 4 | Pass `first=2, last=3` to fixture with subsamples 1-3; returns `[2, 3]`. |
| scdm-prepare.AC1.3 | Omitting `--last` processes from `--first` through highest | Unit | `tests/test_ingest.py::test_discover_subsamples_first_only` | 2, Task 4 | Pass `first=2, last=None` to fixture with subsamples 1-3; returns `[2, 3]`. |
| scdm-prepare.AC1.4 | Missing subsample file fails fast with clear listing | Unit | `tests/test_ingest.py::test_discover_subsamples_missing_file` | 2, Task 4 | Remove one file from fixture dir; verify raised error lists the specific missing file(s). |
| scdm-prepare.AC1.5 | Empty directory produces clear error | Unit | `tests/test_ingest.py::test_discover_subsamples_empty_dir` | 2, Task 4 | Empty `tmp_path`; verify error message references no matching files. |

### scdm-prepare.AC2: Ingestion

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC2.1 | SAS7BDAT read in chunks, written to temp parquet with samplenum | Unit | `tests/test_ingest.py::test_ingest_table_samplenum` | 2, Task 5 | After `ingest_table()`, read temp parquet and verify `samplenum` column exists with correct values. |
| scdm-prepare.AC2.2 | All 9 table types ingested across selected subsamples | Integration | `tests/test_ingest.py::test_ingest_all_tables` | 2, Task 6 | Run `discover_subsamples()` then `ingest_all()` on 3-subsample fixture; verify 27 temp parquet files (9 tables x 3 subsamples). Verify column names match schema. |
| scdm-prepare.AC2.3 | SAS date columns preserved as date types | Unit | `tests/test_ingest.py::test_date_column_types` | 2, Task 5 | Fixture includes date columns (e.g., `Enr_Start`, `Birth_Date`). After ingestion, verify polars reads them as `Date` dtype, not `Int64`. |

**Implementation decision rationale:** Test fixtures use parquet (not SAS7BDAT) because pyreadstat cannot write SAS7BDAT. The `file_ext` parameter selects the reader path: `.sas7bdat` triggers pyreadstat chunked read, `.parquet` triggers polars direct read. AC2.3 (date preservation) is tested via parquet fixtures that include `pl.Date` typed columns. The pyreadstat SAS date conversion path is covered only by human verification against real SynPUF data.

### scdm-prepare.AC3: ID Uniqueness

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC3.1 | Crosswalks assign globally unique sequential IDs; same orig ID in different subsamples gets different new IDs | Unit | `tests/test_transform.py::test_crosswalk_unique_ids_across_subsamples` | 3, Task 2 | Insert PatID "ABC" in samplenum 1 and "ABC" in samplenum 2. After crosswalk, verify different new PatID values. Also verify IDs are sequential starting from 1 with no gaps. |
| scdm-prepare.AC3.2 | NULL/missing original IDs map to NULL in crosswalk output | Unit | `tests/test_transform.py::test_crosswalk_null_ids` | 3, Task 2 | Insert row with NULL PatID. Crosswalk should not contain a row for it. When a table LEFT JOINs on the crosswalk, NULL originals produce NULL new IDs. |
| scdm-prepare.AC3.3 | Four crosswalks produced: patid, encounterid, providerid, facilityid | Unit | `tests/test_transform.py::test_all_four_crosswalks_created` | 3, Task 2 | After `build_crosswalks()`, verify all four crosswalk tables exist in the DuckDB connection. |

**Additional edge case tests (Phase 3, Task 3):**

| Test | Type | Test File | Notes |
|------|------|-----------|-------|
| Sequential IDs start from 1, no gaps | Unit | `tests/test_transform.py::test_crosswalk_sequential_ids` | Verify max ID equals row count. |
| Single subsample produces correct crosswalk | Unit | `tests/test_transform.py::test_crosswalk_single_subsample` | No samplenum deduplication edge cases. |
| Empty source table produces empty crosswalk | Unit | `tests/test_transform.py::test_crosswalk_empty_source` | No errors on zero rows. |
| Duplicate IDs within single subsample are deduplicated | Unit | `tests/test_transform.py::test_crosswalk_dedup_within_subsample` | Same ProviderID appearing multiple times gets one crosswalk entry. |

**Implementation decision rationale:** Crosswalk source tables are single-source: PatID from `demographic`, EncounterID from `encounter`, ProviderID from `provider`, FacilityID from `facility`. The original design considered multi-table UNION for crosswalk sources but Phase 3 planning confirmed each ID type has a single canonical source table. NULLs are filtered with `WHERE IS NOT NULL` in the crosswalk SQL, so NULL mapping to NULL happens naturally via LEFT JOIN absence in Phase 4.

### scdm-prepare.AC4: Table Structure

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC4.1 | Each output table contains exactly the columns from SAS SELECT | Unit | `tests/test_transform.py::test_table_columns[{table}]` | 4, Task 1 | Parametrized test over all 9 tables. After `assemble_tables()`, verify column set matches schema definition. |
| scdm-prepare.AC4.2 | Column order matches SAS SELECT order | Unit | `tests/test_transform.py::test_table_column_order[{table}]` | 4, Task 1 | Parametrized. Verify `df.columns` list equals schema `columns` list (order-sensitive comparison). |
| scdm-prepare.AC4.3 | Join types match SAS code (INNER for PatID, LEFT for others) | Unit | `tests/test_transform.py::test_inner_join_patid` | 4, Task 1 | Insert a row with PatID not in crosswalk; verify it is excluded from output (INNER JOIN). |
| scdm-prepare.AC4.3 | Join types match SAS code (LEFT for EncounterID/ProviderID/FacilityID) | Unit | `tests/test_transform.py::test_left_join_optional_ids` | 4, Task 1 | Insert rows with NULL EncounterID/ProviderID/FacilityID; verify rows are included with NULL new IDs (LEFT JOIN). |

**Implementation decision rationale:** The Phase 4 plan documents a key discrepancy: SAS enrollment SELECT omits PlanType/PayerType, and SAS demographic omits ImputedRace/ImputedHispanic. The implementation includes these columns from `tables_documentation.json` but they may contain NULLs in SynPUF data. Tests validate against the schema definition (which includes all SCDM columns), not the SAS subset. The dispensing table intentionally does NOT crosswalk ProviderID (matching SAS behaviour), unlike diagnosis/procedure which do.

### scdm-prepare.AC5: Sort Orders

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC5.1 | Each output table sorted per tables_documentation.json | Unit | `tests/test_transform.py::test_table_sort_order[{table}]` | 4, Task 1 | Parametrized over all 9 tables. Insert data that would sort differently if ORDER BY were missing. Read back and verify rows are in expected order per schema `sort_keys`. |

**Implementation decision rationale:** Phase 4 documents a sort order discrepancy for dispensing: SAS sorts by 5 keys (`PatID RxDate Rx_CodeType Rx ProviderID`) but `tables_documentation.json` specifies only `PatID, RxDate`. The implementation uses the `tables_documentation.json` sort order (2 keys). The test validates against the schema definition.

### scdm-prepare.AC6: Synthesised Tables

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC6.1 | Provider table: Specialty='99', Specialty_CodeType='2' | Unit | `tests/test_transform.py::test_provider_synthesis` | 4, Task 2 | After synthesis, verify all rows have `Specialty='99'` and `Specialty_CodeType='2'`. Verify columns are (ProviderID, Specialty, Specialty_CodeType). |
| scdm-prepare.AC6.2 | Facility table: empty Facility_Location | Unit | `tests/test_transform.py::test_facility_synthesis` | 4, Task 2 | After synthesis, verify all rows have `Facility_Location=''`. Verify columns are (FacilityID, Facility_Location). |
| scdm-prepare.AC6.3 | Provider/Facility exclude NULL original IDs | Unit | `tests/test_transform.py::test_synthesised_excludes_null_ids` | 4, Task 2 | Add a NULL original ID to the crosswalk fixture. Verify synthesised table does not include a row for it. |

**Implementation decision rationale:** Provider and Facility are synthesised entirely from crosswalk tables, not from source data. The source provider/facility files are still ingested in Phase 2 to populate the crosswalk, but the output tables are generated with hardcoded values. The `WHERE orig_ProviderID IS NOT NULL` filter in the synthesis SQL handles AC6.3.

### scdm-prepare.AC7: Output Formats

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC7.1 | `--format parquet` produces readable .parquet files | Unit | `tests/test_export.py::test_export_parquet` | 5, Task 1 | Create DuckDB table, export, read back with `polars.read_parquet()`, verify data matches. |
| scdm-prepare.AC7.2 | `--format csv` produces CSV with headers | Unit | `tests/test_export.py::test_export_csv` | 5, Task 1 | Export, read back with `polars.read_csv()`, verify header row and data match. |
| scdm-prepare.AC7.3 | `--format json` produces valid NDJSON | Unit | `tests/test_export.py::test_export_ndjson` | 5, Task 2 | Export, read each line as `json.loads()`, verify all lines are valid JSON and data matches. |
| scdm-prepare.AC7.4 | Files named `{table}.{ext}` | Integration | `tests/test_export.py::test_export_all_file_naming` | 5, Task 3 | `export_all()` for all 9 tables; verify file names match `{table}.parquet`, `{table}.csv`, or `{table}.json`. |

**Additional round-trip tests (Phase 5, Task 3):**

| Test | Type | Test File | Notes |
|------|------|-----------|-------|
| Parquet round-trip with multiple column types | Integration | `tests/test_export.py::test_parquet_round_trip` | Int, string, date, nullable columns all survive round-trip. |
| CSV round-trip with multiple column types | Integration | `tests/test_export.py::test_csv_round_trip` | Verify header presence and data fidelity. |
| NDJSON round-trip with multiple column types | Integration | `tests/test_export.py::test_ndjson_round_trip` | Each line valid JSON, data reconstructable. |

**Implementation decision rationale:** Parquet and CSV use DuckDB `COPY TO` for streaming writes (no memory spike). NDJSON uses `fetch_arrow_reader()` + polars `write_ndjson()` in batches because polars `write_ndjson()` does not support append mode. The batch_size parameter (100,000) is a performance tuning knob, not a correctness concern.

### scdm-prepare.AC8: CLI Interface

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC8.1 | `--help` prints usage with all arguments | Unit | `tests/test_cli.py::test_help_output` | 6, Task 2 | `CliRunner.invoke(app, ["--help"])` returns exit code 0. Output contains `--input`, `--output`, `--format`, `--first`, `--last`. |
| scdm-prepare.AC8.2 | `--input`, `--output`, `--format` required; `--first`, `--last` optional | Unit | `tests/test_cli.py::test_required_args_missing` | 6, Task 2 | Invoke without required args; verify non-zero exit code. Invoke with only required args (no `--first`/`--last`); verify success. |
| scdm-prepare.AC8.3 | Invalid `--format` produces clear error | Unit | `tests/test_cli.py::test_invalid_format` | 6, Task 3 | `--format xml` produces non-zero exit code and error output mentioning valid choices. Handled by Typer enum validation. |
| scdm-prepare.AC8.4 | Non-existent `--input` directory produces clear error | Unit | `tests/test_cli.py::test_nonexistent_input_dir` | 6, Task 3 | `--input /nonexistent` produces non-zero exit code and error output. Handled by Typer `exists=True`. |

### scdm-prepare.AC9: Progress Reporting

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC9.1 | Ingestion reports per-file progress | Integration | `tests/test_cli.py::test_ingestion_progress` | 6, Task 2 | Run CLI with fixtures, capture output. Verify output contains per-file progress indicators. |
| scdm-prepare.AC9.2 | Transform and export report per-table progress | Integration | `tests/test_cli.py::test_transform_export_progress` | 6, Task 2 | Run CLI with fixtures, capture output. Verify output contains per-table progress indicators. |

### scdm-prepare.AC10: Error Handling & Cleanup

| AC ID | Criterion | Type | Test File | Phase | Notes |
|-------|-----------|------|-----------|-------|-------|
| scdm-prepare.AC10.1 | Temp files cleaned up on success | Integration | `tests/test_cli.py::test_temp_cleanup_on_success` | 6, Task 2 | Run full CLI on fixtures. After successful exit, verify `{output_dir}/_temp/` does not exist. |
| scdm-prepare.AC10.2 | Temp files left in place on failure | Integration | `tests/test_cli.py::test_temp_preserved_on_failure` | 6, Task 2 | Trigger a failure mid-pipeline (e.g., missing subsample file partway through). Verify `_temp/` still exists. |
| scdm-prepare.AC10.3 | `--clean-temp` removes leftover temp files | Unit | `tests/test_cli.py::test_clean_temp_flag` | 6, Task 4 | Manually create `{output_dir}/_temp/` with files. Run CLI with `--clean-temp`. Verify directory removed and pipeline did not execute. |
| scdm-prepare.AC10.4 | Corrupt SAS7BDAT reports which file failed | Unit | `tests/test_cli.py::test_corrupt_file_error` | 6, Task 3 | Write random bytes to a `.sas7bdat` file. Attempt ingestion. Verify error message includes the specific filename. |

---

## Human Verification Requirements

The following criteria require human verification because they depend on conditions that cannot be reliably reproduced in automated tests.

### HV-1: SAS7BDAT Chunked Reading with Real Data

| Related ACs | scdm-prepare.AC2.1, scdm-prepare.AC2.3 |
|---|---|
| **Justification** | Test fixtures use parquet files (pyreadstat cannot write SAS7BDAT). The pyreadstat chunked-read code path and SAS date conversion are only exercised against real SynPUF SAS7BDAT files. Parquet fixtures verify the samplenum injection and temp-parquet writing logic, but not the pyreadstat reader branch. |
| **Verification approach** | Run `scdm-prepare --input data/ --output /tmp/scdm_out --format parquet --first 1 --last 1` against a single real subsample. Verify: (1) ingestion completes without error; (2) temp parquet files contain correct `samplenum` values; (3) date columns (e.g., `Enr_Start`, `Birth_Date`, `ADate`) are date types, not integer epoch values. Inspect with `polars.read_parquet('_temp/enrollment_1.parquet').dtypes`. |

### HV-2: Memory Behaviour at Scale

| Related ACs | Design DoD item 4 ("Handles 31GB+ without blowing memory") |
|---|---|
| **Justification** | Memory behaviour under load cannot be meaningfully tested with small fixtures. The design relies on pyreadstat chunk size (~100k rows) keeping ingestion memory under ~200MB, and DuckDB spilling to disk under pressure. These are runtime performance properties, not functional correctness. |
| **Verification approach** | Run the full pipeline against all 20 subsamples (`scdm-prepare --input data/ --output /tmp/scdm_out --format parquet`). Monitor peak RSS memory via `time -v` (Linux) or Activity Monitor (macOS). Verify peak memory stays well below total input size (~31GB). Observe DuckDB temp files on disk if memory pressure is high. |

### HV-3: Output Data Fidelity vs SAS Reference

| Related ACs | scdm-prepare.AC4.1, scdm-prepare.AC4.3, scdm-prepare.AC5.1 |
|---|---|
| **Justification** | Automated tests verify structural correctness (columns, order, join types) against the schema definition using synthetic data. They cannot verify that the output data *matches the SAS reference implementation's actual output* for real SynPUF data, because we lack a SAS runtime in CI to produce comparison data. |
| **Verification approach** | Run both the SAS code and `scdm-prepare` against the same subsample(s). Compare row counts per table. Sample 100 random rows per table and compare column values. Particular attention to: (1) enrollment row counts (SAS omits samplenum join condition -- the Python implementation includes it, which may produce fewer rows if the SAS behaviour was a bug); (2) dispensing ProviderID values (not crosswalked, passed through directly). |

### HV-4: Progress Reporting Visual Quality

| Related ACs | scdm-prepare.AC9.1, scdm-prepare.AC9.2 |
|---|---|
| **Justification** | Automated tests can verify that progress output *exists* (non-empty output captured by `CliRunner`), but cannot assess whether the progress bars render correctly in a real terminal, update smoothly, or provide a useful user experience. Rich progress bars depend on terminal capabilities and width. |
| **Verification approach** | Run the full pipeline in a real terminal (not piped/redirected). Observe: (1) per-file progress bar during ingestion shows current filename and advances; (2) per-table progress during transform/export shows table names and advances; (3) no visual glitches, overlapping output, or garbled text. |

### HV-5: Disk Space Warning

| Related ACs | Design additional consideration ("warn at startup if output dir has less free space than input dir total size") |
|---|---|
| **Justification** | This is mentioned in the design's Additional Considerations section but is not an explicit acceptance criterion. If implemented, reliably testing disk space conditions requires filesystem mocking or actual low-disk environments that are impractical in CI. |
| **Verification approach** | If implemented: run the CLI against a filesystem with limited free space (e.g., a small tmpfs mount). Verify the warning message appears and the pipeline continues (warning, not error). If not implemented: document as a known gap for future work. |

---

## Coverage Summary

| AC Group | Total ACs | Automated | Human Verification | Notes |
|----------|-----------|-----------|-------------------|-------|
| AC1: Subsample Discovery | 5 | 5 | 0 | Fully automated via parquet fixtures. |
| AC2: Ingestion | 3 | 3 | HV-1 supplements | Automated tests cover logic; HV-1 covers real SAS7BDAT path. |
| AC3: ID Uniqueness | 3 (+4 edge cases) | 7 | 0 | Fully automated. |
| AC4: Table Structure | 3 | 4 | HV-3 supplements | Automated tests cover structure; HV-3 covers data fidelity vs SAS. |
| AC5: Sort Orders | 1 | 1 | 0 | Fully automated (parametrized over 9 tables). |
| AC6: Synthesised Tables | 3 | 3 | 0 | Fully automated. |
| AC7: Output Formats | 4 (+3 round-trip) | 7 | 0 | Fully automated. |
| AC8: CLI Interface | 4 | 4 | 0 | Fully automated via Typer CliRunner. |
| AC9: Progress Reporting | 2 | 2 | HV-4 supplements | Automated tests verify output exists; HV-4 covers visual quality. |
| AC10: Error Handling | 4 | 4 | 0 | Fully automated. |
| **Totals** | **32 (+7 extra)** | **40** | **5 HV items** | Every AC has automated coverage. 5 HV items supplement areas where automation has known blind spots. |

---

## Test File Summary

| Test File | Phase | AC Coverage | Estimated Test Count |
|-----------|-------|-------------|---------------------|
| `tests/test_schema.py` | 2 | (infrastructure) | 5 |
| `tests/test_ingest.py` | 2 | AC1.1-AC1.5, AC2.1-AC2.3 | 8 |
| `tests/test_transform.py` | 3, 4 | AC3.1-AC3.3, AC4.1-AC4.3, AC5.1, AC6.1-AC6.3 | 18 |
| `tests/test_export.py` | 5 | AC7.1-AC7.4 | 7 |
| `tests/test_cli.py` | 6 | AC8.1-AC8.4, AC9.1-AC9.2, AC10.1-AC10.4 | 10 |
| `tests/conftest.py` | 2 | (shared fixtures) | 0 |
| **Total** | | | **~48** |
