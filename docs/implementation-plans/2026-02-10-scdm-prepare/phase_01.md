# SCDM Prepare Implementation Plan — Phase 1: Project Scaffolding

**Goal:** Create a pip/uv-installable Python package with a working CLI entry point that parses and validates arguments.

**Architecture:** src-layout Python package managed by uv, using Typer for CLI argument parsing. The CLI skeleton accepts all required arguments but does not yet perform any data processing.

**Tech Stack:** Python 3.13.5, uv (build/package manager), Typer (CLI framework)

**Scope:** 6 phases from original design (phase 1 of 6)

**Codebase verified:** 2026-02-10 — Greenfield. No existing Python project infrastructure (no pyproject.toml, src/, tests/, .python-version). Worktree root contains only SAS reference code and documentation.

---

## Acceptance Criteria Coverage

**Verifies: None** — This is an infrastructure phase. Verification is operational (uv sync succeeds, CLI --help works).

---

<!-- START_TASK_1 -->
### Task 1: Create .python-version and pyproject.toml

**Files:**
- Create: `.python-version`
- Create: `pyproject.toml`

**Step 1: Create `.python-version`**

```
3.13.5
```

**Step 2: Create `pyproject.toml`**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "scdm-prepare"
version = "0.1.0"
description = "Combine SynPUF subsamples into standardised SCDM tables"
readme = "readme.md"
requires-python = ">=3.13"
dependencies = [
    "duckdb>=1.4",
    "polars>=1.38",
    "pyreadstat>=1.3",
    "typer[all]>=0.22",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
]

[project.scripts]
scdm-prepare = "scdm_prepare.cli:app"
```

Note: Using `hatchling` as build backend — it is the most widely supported backend for uv src-layout projects and handles editable installs reliably.

**Step 3: Verify operationally**

Run: `uv sync`
Expected: Resolves and installs all dependencies without errors. Creates `uv.lock`.

**Step 4: Commit**

```bash
git add .python-version pyproject.toml uv.lock
git commit -m "chore: initialize project with pyproject.toml and uv"
```
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create package structure and CLI skeleton

**Files:**
- Create: `src/scdm_prepare/__init__.py`
- Create: `src/scdm_prepare/cli.py`

**Step 1: Create `src/scdm_prepare/__init__.py`**

```python
"""scdm-prepare: Combine SynPUF subsamples into standardised SCDM tables."""
```

**Step 2: Create `src/scdm_prepare/cli.py`**

```python
"""CLI entry point for scdm-prepare."""

from enum import Enum
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(
    name="scdm-prepare",
    help="Combine SynPUF subsamples into standardised SCDM tables.",
)


class OutputFormat(str, Enum):
    parquet = "parquet"
    csv = "csv"
    json = "json"


@app.command()
def main(
    input_dir: Path = typer.Option(
        ...,
        "--input",
        help="Directory containing SynPUF SAS7BDAT subsample files.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output",
        help="Directory for output files. Created if it does not exist.",
        resolve_path=True,
    ),
    fmt: OutputFormat = typer.Option(
        ...,
        "--format",
        help="Output format.",
    ),
    first: Optional[int] = typer.Option(
        None,
        "--first",
        help="First subsample number to process. Omit to start from the lowest detected.",
    ),
    last: Optional[int] = typer.Option(
        None,
        "--last",
        help="Last subsample number to process. Omit to process through the highest detected.",
    ),
) -> None:
    """Combine SynPUF subsamples into 9 standardised SCDM tables."""
    if not input_dir.is_dir():
        typer.echo(f"Error: Input directory does not exist: {input_dir}", err=True)
        raise typer.Exit(code=1)

    output_dir.mkdir(parents=True, exist_ok=True)

    typer.echo(f"Input:  {input_dir}")
    typer.echo(f"Output: {output_dir}")
    typer.echo(f"Format: {fmt.value}")
    if first is not None:
        typer.echo(f"First subsample: {first}")
    if last is not None:
        typer.echo(f"Last subsample:  {last}")
```

**Step 3: Verify operationally**

Run: `uv run scdm-prepare --help`
Expected: Prints usage with all arguments (--input, --output, --format, --first, --last).

Run: `uv run scdm-prepare --input /tmp --output /tmp/out --format parquet`
Expected: Prints the parsed argument values without error.

Run: `uv run scdm-prepare --input /nonexistent --output /tmp/out --format parquet`
Expected: Error about non-existent input directory (Typer's `exists=True` handles this).

Run: `uv run scdm-prepare --input /tmp --output /tmp/out --format xml`
Expected: Error about invalid format choice.

**Step 4: Commit**

```bash
git add src/scdm_prepare/__init__.py src/scdm_prepare/cli.py
git commit -m "chore: add CLI skeleton with argument parsing"
```
<!-- END_TASK_2 -->
