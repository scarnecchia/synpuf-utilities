# SCDM Prepare Implementation Plan — Phase 6: CLI Integration, Progress Reporting, and Error Handling

**Goal:** Wire all pipeline stages into the CLI with progress reporting and robust error handling for production use.

**Architecture:** `cli.py` orchestrates the full pipeline (validate → ingest → transform → export → cleanup). `progress.py` wraps `rich.progress.Progress` for per-file and per-table tracking. Error handling uses early validation with clear error messages, and temp file cleanup with a `--clean-temp` flag.

**Tech Stack:** Python 3.13.5, Typer (CLI), rich (progress bars), DuckDB, polars, pyreadstat

**Scope:** 6 phases from original design (phase 6 of 6)

**Codebase verified:** 2026-02-10 — Typer includes rich as a dependency. `typer.testing.CliRunner` confirmed for CLI testing. `rich.progress.Progress` supports per-task descriptions and progress updates. All pipeline modules (ingest.py, transform.py, export.py) exist from prior phases.

---

## Acceptance Criteria Coverage

This phase implements and tests:

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

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Create progress.py with rich progress tracking

**Files:**
- Create: `src/scdm_prepare/progress.py`

**Implementation:**

Create a `PipelineProgress` class that wraps `rich.progress.Progress` to provide:

1. An `ingestion_tracker(total_files)` context that creates a progress task for file ingestion. Provides methods to update the description (current filename) and advance.
2. A `transform_tracker(total_tables)` context for tracking per-table transform progress.
3. An `export_tracker(total_tables)` context for tracking per-table export progress.

The class should use `rich.progress.Progress` under the hood and manage task lifecycle (add_task, update, advance).

Example API:
```python
progress = PipelineProgress()
with progress.ingestion_tracker(total_files=27) as tracker:
    for file_path in files:
        tracker.update_description(f"Ingesting {file_path.name}")
        # ... do work ...
        tracker.advance()
```

Keep it simple — one progress bar per stage (ingestion, transform, export) with a description showing the current item.

**Verification:**

Run: `uv run python -c "from scdm_prepare.progress import PipelineProgress; print('OK')"`
Expected: Imports without error.

**Commit:** `feat: add rich progress tracking for pipeline stages`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Wire CLI orchestration in cli.py

**Verifies:** scdm-prepare.AC8.1, scdm-prepare.AC8.2, scdm-prepare.AC9.1, scdm-prepare.AC9.2, scdm-prepare.AC10.1, scdm-prepare.AC10.2

**Files:**
- Modify: `src/scdm_prepare/cli.py`

**Implementation:**

Replace the Phase 1 skeleton `main()` function with full pipeline orchestration:

```python
@app.command()
def main(
    input_dir: Path = typer.Option(..., "--input", ...),
    output_dir: Path = typer.Option(..., "--output", ...),
    fmt: OutputFormat = typer.Option(..., "--format", ...),
    first: Optional[int] = typer.Option(None, "--first", ...),
    last: Optional[int] = typer.Option(None, "--last", ...),
    clean_temp: bool = typer.Option(False, "--clean-temp", help="Remove leftover temp files and exit."),
) -> None:
    temp_dir = output_dir / "_temp"

    # Handle --clean-temp
    if clean_temp:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            typer.echo(f"Cleaned temp directory: {temp_dir}")
        else:
            typer.echo("No temp directory to clean.")
        raise typer.Exit()

    output_dir.mkdir(parents=True, exist_ok=True)

    progress = PipelineProgress()

    try:
        # 1. Discover subsamples
        subsamples = discover_subsamples(input_dir, first, last)
        typer.echo(f"Found subsamples: {subsamples}")

        # 2. Ingest (with per-file progress)
        ingest_all(input_dir, subsamples, output_dir, progress=progress)

        # 3. Transform (with per-table progress)
        con = duckdb.connect()
        build_crosswalks(con, str(temp_dir))
        assemble_tables(con, str(temp_dir), progress=progress)

        # 4. Export (with per-table progress)
        export_all(con, TABLES.keys(), str(output_dir), fmt.value, progress=progress)

        # 5. Cleanup temp on success
        shutil.rmtree(temp_dir)
        typer.echo("Done. Temp files cleaned up.")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        typer.echo(f"Temp files preserved at: {temp_dir}", err=True)
        raise typer.Exit(code=1)
```

This requires updating `ingest_all()`, `assemble_tables()`, and `export_all()` to accept an optional `progress` parameter for progress reporting (AC9.1, AC9.2). When progress is None, operations run silently.

On success, temp directory is cleaned up (AC10.1). On failure, temp files are preserved for debugging (AC10.2).

**Testing:**
Tests must verify each AC listed above:
- scdm-prepare.AC8.1: `runner.invoke(app, ["--help"])` returns exit_code 0 and contains all argument names
- scdm-prepare.AC8.2: Invoke without required args → non-zero exit code
- scdm-prepare.AC9.1: Verify ingestion progress callback is called (mock or capture output)
- scdm-prepare.AC9.2: Verify transform/export progress is reported
- scdm-prepare.AC10.1: After successful run, `_temp/` directory does not exist
- scdm-prepare.AC10.2: After failed run, `_temp/` directory still exists

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_cli.py -v`
Expected: All CLI tests pass.

**Commit:** `feat: wire full pipeline orchestration in CLI`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->
<!-- START_TASK_3 -->
### Task 3: Add error handling for missing/corrupt files

**Verifies:** scdm-prepare.AC8.3, scdm-prepare.AC8.4, scdm-prepare.AC10.4

**Files:**
- Modify: `src/scdm_prepare/cli.py`
- Modify: `src/scdm_prepare/ingest.py`

**Implementation:**

Enhance error handling:

1. **Invalid --format** (AC8.3): Already handled by Typer's enum validation on `OutputFormat`. Verify this produces a clear error message.

2. **Non-existent --input directory** (AC8.4): Already handled by Typer's `exists=True` on the `--input` option. Verify this produces a clear error message.

3. **Corrupt SAS7BDAT file** (AC10.4): Wrap the pyreadstat read call in a try/except that catches exceptions and reports the specific file path that failed:
```python
try:
    for chunk_df, meta in pyreadstat.read_file_in_chunks(...):
        ...
except Exception as e:
    raise RuntimeError(f"Failed to read {file_path}: {e}") from e
```

**Testing:**
Tests must verify each AC listed above:
- scdm-prepare.AC8.3: `runner.invoke(app, ["--input", "/tmp", "--output", "/tmp/out", "--format", "xml"])` produces clear error about invalid format
- scdm-prepare.AC8.4: `runner.invoke(app, ["--input", "/nonexistent", "--output", "/tmp/out", "--format", "parquet"])` produces clear error about non-existent directory
- scdm-prepare.AC10.4: Create a corrupt file (write random bytes to a `.sas7bdat` file), attempt ingestion, verify error message includes the specific filename

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_cli.py -v -k error`
Expected: All error handling tests pass.

**Commit:** `feat: add error handling for corrupt and missing files`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Add --clean-temp flag and E2E smoke test

**Verifies:** scdm-prepare.AC10.3

**Files:**
- Modify: `tests/test_cli.py`

**Testing:**

1. **--clean-temp** (AC10.3):
   - Create a temp directory with files at `{output_dir}/_temp/`
   - Run CLI with `--clean-temp`
   - Verify temp directory is removed
   - Verify CLI exits cleanly without running the pipeline

2. **Full E2E smoke test**:
   - Use the `sample_parquet_dir` fixture from conftest.py
   - Run the full CLI: `scdm-prepare --input {fixture_dir} --output {tmp_dir} --format parquet`
   - Verify all 9 output files exist
   - Verify temp directory is cleaned up
   - Verify output files have data

   The CLI defaults to `.sas7bdat` extension, but pyreadstat cannot write SAS7BDAT files for fixtures. Add a `--file-ext` option (hidden from --help via `typer.Option(..., hidden=True)`) that defaults to `.sas7bdat` but can be set to `.parquet` for testing. This hidden option should be added to the `main()` function in Task 2 and threaded through to `discover_subsamples()` and `ingest_all()`.

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_cli.py -v`
Expected: All tests pass including E2E smoke test.

**Commit:** `test: add --clean-temp and E2E smoke tests`
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_B -->
