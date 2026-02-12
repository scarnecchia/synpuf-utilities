# SCDM Prepare Implementation Plan — Phase 5: Export Layer

**Goal:** Export assembled SCDM tables from DuckDB to parquet, CSV, or NDJSON format.

**Architecture:** Three export strategies based on format: DuckDB `COPY TO` for parquet and CSV (native streaming, no memory spike), batched Arrow reader + polars `write_ndjson()` for JSON. Each writer takes a DuckDB connection, table name, and output path.

**Tech Stack:** Python 3.13.5, DuckDB (COPY command), polars (NDJSON writing), pyarrow (Arrow reader bridge)

**Scope:** 6 phases from original design (phase 5 of 6)

**Codebase verified:** 2026-02-10 — DuckDB `COPY TO` confirmed working via `con.execute()` for parquet (with zstd compression) and CSV (with headers). polars `write_ndjson()` does NOT support append mode — batched NDJSON requires `fetch_arrow_reader()` + manual file handle writing.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### scdm-prepare.AC7: Output Formats
- **scdm-prepare.AC7.1 Success:** `--format parquet` produces readable .parquet files
- **scdm-prepare.AC7.2 Success:** `--format csv` produces CSV with headers
- **scdm-prepare.AC7.3 Success:** `--format json` produces valid NDJSON files
- **scdm-prepare.AC7.4 Success:** Output files are named `{table}.{ext}` (e.g., enrollment.parquet)

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Create export.py with parquet and CSV writers

**Verifies:** scdm-prepare.AC7.1, scdm-prepare.AC7.2, scdm-prepare.AC7.4

**Files:**
- Create: `src/scdm_prepare/export.py`

**Implementation:**

Create an `export_table(con, table_name, output_dir, fmt)` function that exports a single DuckDB table to the specified format.

For **parquet**:
```python
con.execute(f"""
    COPY {table_name}
    TO '{output_dir}/{table_name}.parquet'
    (FORMAT parquet, COMPRESSION zstd)
""")
```

For **CSV**:
```python
con.execute(f"""
    COPY {table_name}
    TO '{output_dir}/{table_name}.csv'
    (FORMAT csv, HEADER true)
""")
```

Also create an `export_all(con, table_names, output_dir, fmt)` function that calls `export_table` for each of the 9 tables.

File naming: `{table_name}.{ext}` where ext is `parquet`, `csv`, or `json` (AC7.4).

Note: Use parameterized paths carefully to avoid SQL injection. Since table names and paths come from our own schema definitions (not user input), string formatting is acceptable here.

**Testing:**
Tests must verify each AC listed above:
- scdm-prepare.AC7.1: After parquet export, file exists and is readable by `polars.read_parquet()` with correct data
- scdm-prepare.AC7.2: After CSV export, file exists, has header row, and is readable by `polars.read_csv()` with correct data
- scdm-prepare.AC7.4: Files are named `{table}.parquet` or `{table}.csv`

Tests should set up a small DuckDB table, export it, and verify the output file.

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_export.py -v -k "parquet or csv"`
Expected: All parquet and CSV tests pass.

**Commit:** `feat: add parquet and CSV export via DuckDB COPY`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add NDJSON export

**Verifies:** scdm-prepare.AC7.3, scdm-prepare.AC7.4

**Files:**
- Modify: `src/scdm_prepare/export.py`

**Implementation:**

Add NDJSON export to the `export_table` function. Since polars `write_ndjson()` doesn't support append mode, use DuckDB's `fetch_arrow_reader()` for batched reading and write NDJSON manually:

```python
def _export_ndjson(con, table_name, output_path, batch_size=100_000):
    result = con.execute(f"SELECT * FROM {table_name}")
    reader = result.fetch_arrow_reader(batch_size=batch_size)
    with open(output_path, "w") as f:
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            df = pl.from_arrow(batch)
            f.write(df.write_ndjson())
```

Each line in the output file is a valid JSON object. The file as a whole is valid NDJSON (newline-delimited JSON).

**Testing:**
Tests must verify each AC listed above:
- scdm-prepare.AC7.3: After NDJSON export, file exists, each line is valid JSON, and data matches
- scdm-prepare.AC7.4: File is named `{table}.json`

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_export.py -v -k json`
Expected: NDJSON tests pass.

**Commit:** `feat: add NDJSON export with batched writing`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Export integration test — all formats

**Verifies:** scdm-prepare.AC7.1, scdm-prepare.AC7.2, scdm-prepare.AC7.3, scdm-prepare.AC7.4

**Files:**
- Modify: `tests/test_export.py`

**Testing:**
Round-trip test for each format:
1. Create a DuckDB table with known data (multiple column types: int, string, date, nullable)
2. Export via `export_table()`
3. Read back the exported file
4. Verify data matches original

Test `export_all()` with all 9 table names to verify all files are created with correct naming.

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_export.py -v`
Expected: All export tests pass.

**Commit:** `test: add export round-trip integration tests`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->
