# SynPUF Overview

Last verified: 2026-02-11

## Tech Stack
- Language: Python 3.13+
- Package manager: uv
- Build: Hatchling
- Key deps: DuckDB, Polars, pyreadstat, Typer (CLI), Rich (progress)
- Testing: pytest

## Commands
- `uv run scdm-prepare --input <dir> --output <dir> --format parquet` - Run pipeline
- `uv run pytest` - Run tests
- `uv sync` - Install dependencies

## Project Structure
- `src/scdm_prepare/` - CLI tool: combines SynPUF SAS subsamples into SCDM tables
- `tests/` - Test suite for scdm-prepare
- `descriptive_statistics/` - Pre-existing SAS analysis scripts
- `synpuf_export/` - Pre-existing SAS export code
- `translational_code/` - Pre-existing translational SAS code
- `docs/` - Design plans and documentation

## Conventions
- src layout (`src/scdm_prepare/`) with Hatch build
- Frozen dataclasses for schema definitions
- Protocol-based progress tracking (structural typing)
- DuckDB for all SQL transforms; Polars for ingestion I/O

## Boundaries
- Safe to edit: `src/scdm_prepare/`, `tests/`
- Do not edit: `descriptive_statistics/`, `synpuf_export/`, `translational_code/` (legacy SAS)
- Do not edit: `uv.lock` (managed by uv)
