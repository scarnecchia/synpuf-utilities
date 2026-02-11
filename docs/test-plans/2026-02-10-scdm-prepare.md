# SCDM Prepare Human Test Plan

Generated: 2026-02-11

## Prerequisites

- macOS or Linux environment with Python 3.13+ and `uv` installed
- Real SynPUF SAS7BDAT data available in `data/` directory (at minimum subsamples 1-5; ideally all 20)
- SAS runtime available for comparison output (HV-3 only)
- `uv sync` completed successfully
- `uv run pytest` passing (131 tests, 0 failures)
- At least 40GB free disk space for full 20-subsample run

---

## Phase 1: SAS7BDAT Ingestion with Real Data (HV-1)

**Purpose:** Verify the pyreadstat chunked-read code path works with actual SynPUF SAS7BDAT files, including SAS date column conversion -- a path not exercised by automated tests.

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Run `uv run scdm-prepare --input data/ --output /tmp/scdm_hv1 --format parquet --first 1 --last 1` | Command completes with exit code 0. Output shows "Found subsamples: [1]", per-file progress indicators, and "Done. Temp files cleaned up." |
| 1.2 | Verify output files exist: `ls /tmp/scdm_hv1/*.parquet` | Exactly 9 parquet files: enrollment.parquet, demographic.parquet, dispensing.parquet, encounter.parquet, diagnosis.parquet, procedure.parquet, death.parquet, provider.parquet, facility.parquet |
| 1.3 | Verify `_temp` directory was cleaned: `ls /tmp/scdm_hv1/_temp` | Directory does not exist (should get "No such file or directory") |
| 1.4 | Inspect date types in demographic output: `uv run python -c "import polars as pl; df = pl.read_parquet('/tmp/scdm_hv1/demographic.parquet'); print(df.schema)"` | `Birth_Date` is `Date` type, `PostalCode_Date` is `Date` type. Neither should be `Int64` (which would indicate raw SAS epoch values leaked through). |
| 1.5 | Inspect date types in enrollment output: `uv run python -c "import polars as pl; df = pl.read_parquet('/tmp/scdm_hv1/enrollment.parquet'); print(df.schema)"` | `Enr_Start` and `Enr_End` are both `Date` type. |
| 1.6 | Inspect date types in encounter output: `uv run python -c "import polars as pl; df = pl.read_parquet('/tmp/scdm_hv1/encounter.parquet'); print(df.schema)"` | `ADate` and `DDate` are both `Date` type. |
| 1.7 | Spot-check a date value: `uv run python -c "import polars as pl; df = pl.read_parquet('/tmp/scdm_hv1/demographic.parquet'); print(df.select('Birth_Date').head(5))"` | Dates are human-readable (e.g., `1950-03-15`), not epoch integers. |
| 1.8 | Clean up: `rm -rf /tmp/scdm_hv1` | Cleanup complete. |

---

## Phase 2: Memory Behaviour at Scale (HV-2)

**Purpose:** Verify the pipeline handles the full ~31GB dataset without excessive memory consumption.

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Run full pipeline: `/usr/bin/time -l uv run scdm-prepare --input data/ --output /tmp/scdm_hv2 --format parquet` (macOS) or wrap with `time -v` on Linux | Command completes successfully. |
| 2.2 | Check peak RSS from the `time` output | Peak RSS should be well below total input size. Target: under 4GB peak RSS for the full 20-subsample dataset. If above 8GB, investigate. |
| 2.3 | Monitor during execution with Activity Monitor (macOS) or `top -p $(pgrep -f scdm-prepare)` (Linux) | Memory usage should remain relatively stable, not continuously climbing. Spikes during crosswalk building / table assembly are acceptable if they come back down. |
| 2.4 | Verify all 9 output parquet files exist and have reasonable row counts: `uv run python -c "import polars as pl; [print(f'{t}: {len(pl.read_parquet(f\"/tmp/scdm_hv2/{t}.parquet\"))} rows') for t in ['enrollment','demographic','dispensing','encounter','diagnosis','procedure','death','provider','facility']]"` | All 9 tables have non-zero row counts. Row counts should be substantially larger than single-subsample run (roughly 20x). |
| 2.5 | Clean up: `rm -rf /tmp/scdm_hv2` | Cleanup complete. |

---

## Phase 3: Output Data Fidelity vs SAS Reference (HV-3)

**Purpose:** Verify that the Python implementation produces output equivalent to the SAS reference implementation for real SynPUF data. Requires a SAS runtime.

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Run the SAS reference code against subsample 1 and export as CSV or SAS7BDAT | SAS reference output available for comparison. |
| 3.2 | Run `uv run scdm-prepare --input data/ --output /tmp/scdm_hv3 --format parquet --first 1 --last 1` | Pipeline completes successfully. |
| 3.3 | Compare row counts per table between SAS output and Python output | Row counts should match. **Known exception:** Enrollment may differ if the SAS code has a samplenum join condition bug (documented in test-requirements.md). Document any discrepancy. |
| 3.4 | For each table, sample 100 random rows from both SAS and Python output. Compare all column values. | Column values should match. ID columns (PatID, EncounterID, etc.) will differ in absolute value (both use sequential IDs) but should have the same cardinality and join relationships. |
| 3.5 | Specifically check dispensing.ProviderID values | ProviderID in dispensing should be the original (un-crosswalked) values in both SAS and Python outputs. The Python implementation intentionally does NOT crosswalk ProviderID for dispensing (matching SAS behaviour). |
| 3.6 | Compare diagnosis.ProviderID values | ProviderID in diagnosis SHOULD be crosswalked (remapped to sequential integers) in the Python output. SAS behaviour may differ here -- document any discrepancy. |
| 3.7 | Clean up: `rm -rf /tmp/scdm_hv3` | Cleanup complete. |

---

## Phase 4: Progress Reporting Visual Quality (HV-4)

**Purpose:** Verify that Rich progress bars render correctly in a real terminal environment.

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Open a real terminal (not VS Code integrated terminal, not piped). Resize to standard width (80+ columns). | Terminal ready. |
| 4.2 | Run `uv run scdm-prepare --input data/ --output /tmp/scdm_hv4 --format parquet --first 1 --last 3` | Pipeline runs with visible progress. |
| 4.3 | During ingestion phase, observe progress output | Per-file progress indicator should be visible, showing current table name and advancing. No garbled text, no overlapping lines, no raw escape codes. |
| 4.4 | During transform/export phase, observe progress output | Per-table progress indicator should be visible, showing table names. Progress should advance smoothly. |
| 4.5 | After completion, verify final output says "Done. Temp files cleaned up." | Clean completion message visible. |
| 4.6 | Repeat step 4.2 with a narrow terminal (40 columns) | Progress output should degrade gracefully -- may truncate but should not crash or produce garbage. |
| 4.7 | Clean up: `rm -rf /tmp/scdm_hv4` | Cleanup complete. |

---

## Phase 5: Disk Space Warning (HV-5)

**Purpose:** Check whether the disk space warning (design additional consideration) was implemented.

| Step | Action | Expected |
|------|--------|----------|
| 5.1 | Review CLI source for disk space checking: `grep -r "free space\|disk\|shutil.disk_usage\|statvfs" src/scdm_prepare/` | If no results, this feature was not implemented. Document as a known gap for future work. |
| 5.2 | If implemented: create a small tmpfs or ramdisk with limited space, run the pipeline targeting it | A warning message should appear but the pipeline should continue (warning, not error). |

---

## End-to-End: Full Pipeline Smoke Test

**Purpose:** Validate the complete pipeline end-to-end against real data, exercising all stages from discovery through export.

| Step | Action | Expected |
|------|--------|----------|
| E2E.1 | Run: `uv run scdm-prepare --input data/ --output /tmp/scdm_e2e --format parquet --first 1 --last 5` | Exit code 0. Output shows discovery, progress indicators, "Done." |
| E2E.2 | Verify 9 output files: `ls /tmp/scdm_e2e/*.parquet \| wc -l` | Exactly 9 files. |
| E2E.3 | Verify PatID uniqueness across tables: `uv run python -c "import polars as pl; demo = pl.read_parquet('/tmp/scdm_e2e/demographic.parquet'); enr = pl.read_parquet('/tmp/scdm_e2e/enrollment.parquet'); print(f'demo PatIDs: {demo[\"PatID\"].n_unique()}, enr PatIDs: {enr[\"PatID\"].n_unique()}'); assert set(enr['PatID'].unique().to_list()).issubset(set(demo['PatID'].unique().to_list()))"` | Enrollment PatIDs should be a subset of demographic PatIDs (due to INNER JOIN). |
| E2E.4 | Verify provider table has hardcoded values: `uv run python -c "import polars as pl; p = pl.read_parquet('/tmp/scdm_e2e/provider.parquet'); print(p.head()); assert (p['Specialty'] == '99').all(); assert (p['Specialty_CodeType'] == '2').all()"` | All Specialty='99', all Specialty_CodeType='2'. |
| E2E.5 | Verify facility table has empty Facility_Location: `uv run python -c "import polars as pl; f = pl.read_parquet('/tmp/scdm_e2e/facility.parquet'); print(f.head()); assert (f['Facility_Location'] == '').all()"` | All Facility_Location values are empty strings. |
| E2E.6 | Re-run with CSV format: `uv run scdm-prepare --input data/ --output /tmp/scdm_e2e_csv --format csv --first 1 --last 1` | Exit code 0. 9 `.csv` files in output directory, each with a header row. |
| E2E.7 | Re-run with JSON format: `uv run scdm-prepare --input data/ --output /tmp/scdm_e2e_json --format json --first 1 --last 1` | Exit code 0. 9 `.json` files in output directory, each line valid JSON. |
| E2E.8 | Clean up: `rm -rf /tmp/scdm_e2e /tmp/scdm_e2e_csv /tmp/scdm_e2e_json` | Cleanup complete. |

---

## End-to-End: Error Recovery

**Purpose:** Validate error handling and temp preservation in failure scenarios against real data.

| Step | Action | Expected |
|------|--------|----------|
| ERR.1 | Run with invalid subsample range: `uv run scdm-prepare --input data/ --output /tmp/scdm_err --format parquet --first 50 --last 60` | Non-zero exit code. Error message about missing files or no matching subsamples. |
| ERR.2 | Create a leftover temp dir, then clean it: `mkdir -p /tmp/scdm_err/_temp && echo test > /tmp/scdm_err/_temp/junk.txt && uv run scdm-prepare --output /tmp/scdm_err --clean-temp` | Exit code 0. `_temp/` directory removed. "Cleaned temp directory" in output. |
| ERR.3 | Run `--clean-temp` when no temp exists: `uv run scdm-prepare --output /tmp/scdm_err --clean-temp` | Exit code 0. "No temp directory to clean." in output. |
| ERR.4 | Clean up: `rm -rf /tmp/scdm_err` | Cleanup complete. |

---

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|-----------|-------|
| HV-1: SAS7BDAT chunked reading | pyreadstat cannot write SAS7BDAT so test fixtures use parquet; the actual `.sas7bdat` reader path is untested in CI | Phase 1 steps 1.1-1.8 |
| HV-2: Memory behaviour at scale | Cannot meaningfully test memory pressure with small fixtures; need real 31GB+ data | Phase 2 steps 2.1-2.5 |
| HV-3: Output fidelity vs SAS reference | No SAS runtime in CI to produce comparison data | Phase 3 steps 3.1-3.7 |
| HV-4: Progress bar visual quality | CliRunner captures text output but cannot assess terminal rendering | Phase 4 steps 4.1-4.7 |
| HV-5: Disk space warning | Feature may not be implemented; requires low-disk environment | Phase 5 steps 5.1-5.2 |

---

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1: Auto-detect subsamples | `test_ingest.py::test_ac11_discover_all_subsamples` | E2E.1 |
| AC1.2: `--first`/`--last` range | `test_ingest.py::test_ac12_range_filtering_first_and_last` | E2E.1 |
| AC1.3: Omit `--last` | `test_ingest.py::test_ac13_omit_last_processes_through_highest` | -- |
| AC1.4: Missing file error | `test_ingest.py::test_ac14_missing_file_raises_with_list` | ERR.1 |
| AC1.5: Empty directory error | `test_ingest.py::test_ac15_empty_directory_raises` | -- |
| AC2.1: samplenum injection | `test_ingest.py::test_ac21_samplenum_column_injected` | 1.1, 1.7 |
| AC2.2: All 9 tables ingested | `test_ingest.py::test_ac22_all_9_tables_ingested` | 1.2 |
| AC2.3: Date column types | `test_ingest.py::test_ac23_date_columns_preserved_as_date_type` | 1.4-1.7 |
| AC3.1: Unique IDs across subsamples | `test_transform.py::test_ac31_unique_ids_across_subsamples` | E2E.3 |
| AC3.2: NULL ID handling | `test_transform.py::test_ac32_null_original_ids_excluded` | -- |
| AC3.3: Four crosswalks | `test_transform.py::test_ac33_four_crosswalks_created` | -- |
| AC4.1: Column sets | `test_transform.py::test_table_columns[*]` (9 tables) | E2E.4, E2E.5 |
| AC4.2: Column order | `test_transform.py::test_table_column_order[*]` (9 tables) | -- |
| AC4.3: Join types | `test_transform.py::test_ac43_inner_join_*` + `test_ac43_left_join_*` | E2E.3, 3.5-3.6 |
| AC5.1: Sort orders | `test_transform.py::test_table_sort_order[*]` (9 tables) | -- |
| AC6.1: Provider synthesis | `test_transform.py::test_ac61_provider_table_structure` | E2E.4 |
| AC6.2: Facility synthesis | `test_transform.py::test_ac62_facility_table_structure` | E2E.5 |
| AC6.3: NULL exclusion | `test_transform.py::test_ac63_*_excludes_null_originals` | -- |
| AC7.1: Parquet export | `test_export.py::test_ac71_*` | E2E.2 |
| AC7.2: CSV export | `test_export.py::test_ac72_*` | E2E.6 |
| AC7.3: NDJSON export | `test_export.py::test_ac73_*` | E2E.7 |
| AC7.4: File naming | `test_export.py::test_ac74_*` + `test_export_all_with_9_tables` | E2E.2, E2E.6, E2E.7 |
| AC8.1: Help output | `test_cli.py::test_ac81_help_prints_all_arguments` | -- |
| AC8.2: Required args | `test_cli.py::test_ac82_*` (4 tests) | -- |
| AC8.3: Invalid format | `test_cli.py::test_ac83_invalid_format` | -- |
| AC8.4: Nonexistent input | `test_cli.py::test_ac84_nonexistent_input_dir` | -- |
| AC9.1: Ingestion progress | `test_cli.py::test_ac91_ingestion_progress` | 4.3 |
| AC9.2: Transform/export progress | `test_cli.py::test_ac92_transform_export_progress` | 4.4 |
| AC10.1: Temp cleanup on success | `test_cli.py::test_ac101_temp_cleaned_on_success` | 1.3, E2E.1 |
| AC10.2: Temp preserved on failure | `test_cli.py::test_ac102_temp_preserved_on_failure` | -- |
| AC10.3: `--clean-temp` | `test_cli.py::test_ac103_clean_temp_removes_directory` | ERR.2, ERR.3 |
| AC10.4: Corrupt file error | `test_cli.py::test_ac104_corrupt_sas7bdat_file` | -- |
