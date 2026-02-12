# scdm-prepare Usage Guide

## Installation

```bash
uv sync
```

## Quick Start

```bash
# Process all 20 subsamples, output as parquet
uv run scdm-prepare --input data/ --output output/ --format parquet

# Process subsamples 1-5 only
uv run scdm-prepare --input data/ --output output/ --format parquet --first 1 --last 5

# Single subsample
uv run scdm-prepare --input data/ --output output/ --format parquet --first 1 --last 1
```

## CLI Options

```
uv run scdm-prepare [OPTIONS]
```

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--input PATH` | Yes (unless `--clean-temp`) | -- | Directory containing SynPUF SAS7BDAT subsample files. Must exist and contain files named `{table}_{N}.sas7bdat`. |
| `--output PATH` | Yes | -- | Directory for output files. Created automatically if it doesn't exist. Temp files go to `{output}/_temp/` during processing. |
| `--format FORMAT` | Yes (unless `--clean-temp`) | -- | Output format. One of: `parquet`, `csv`, `json` (NDJSON). |
| `--first N` | No | Lowest detected | First subsample number to process. |
| `--last N` | No | Highest detected | Last subsample number to process. |
| `--clean-temp` | No | `false` | Remove leftover `_temp/` directory and exit. Does not run the pipeline. Only requires `--output`. |

### Output Formats

| Format | Extension | Details |
|--------|-----------|---------|
| `parquet` | `.parquet` | Zstd compression. Best for downstream analysis. |
| `csv` | `.csv` | Includes header row. |
| `json` | `.json` | Newline-delimited JSON (one JSON object per line). |

## Examples

### Full pipeline (all subsamples)

```bash
uv run scdm-prepare \
  --input data/ \
  --output /tmp/scdm_output \
  --format parquet
```

Produces 9 files in `/tmp/scdm_output/`:
- `enrollment.parquet`
- `demographic.parquet`
- `dispensing.parquet`
- `encounter.parquet`
- `diagnosis.parquet`
- `procedure.parquet`
- `death.parquet`
- `provider.parquet`
- `facility.parquet`

### Subset of subsamples

```bash
# Process subsamples 5 through 10
uv run scdm-prepare --input data/ --output output/ --format parquet --first 5 --last 10

# Process from subsample 15 to end
uv run scdm-prepare --input data/ --output output/ --format csv --first 15
```

### CSV output

```bash
uv run scdm-prepare --input data/ --output output/ --format csv --first 1 --last 1
```

### NDJSON output

```bash
uv run scdm-prepare --input data/ --output output/ --format json --first 1 --last 1
```

### Clean up leftover temp files

If a previous run failed and left temp files behind:

```bash
uv run scdm-prepare --output output/ --clean-temp
```

This removes `output/_temp/` and exits. Does not require `--input` or `--format`.

## Pipeline Stages

The tool runs 5 stages in sequence:

1. **Discover** -- Scans `--input` for `*_{N}.sas7bdat` files, validates all 9 table types exist for each subsample in the `--first`/`--last` range
2. **Ingest** -- Reads each SAS7BDAT file in chunks via pyreadstat, writes temp parquet to `{output}/_temp/` with a `samplenum` column injected
3. **Crosswalk** -- DuckDB generates 4 crosswalk tables (patid, encounterid, providerid, facilityid) assigning globally unique sequential IDs across subsamples
4. **Assemble** -- DuckDB SQL joins ingested data with crosswalks to produce 9 SCDM tables. Provider and Facility tables are synthesised from crosswalks with placeholder values.
5. **Export** -- DuckDB writes each table to `{output}/` in the chosen format. Temp directory is cleaned up on success.

## Error Behaviour

| Scenario | Behaviour |
|----------|-----------|
| Missing `--input`, `--output`, or `--format` | Error message, exit code 1 |
| `--input` directory doesn't exist | Error message, exit code 1 |
| Invalid `--format` value (e.g., `xml`) | Error listing valid choices, exit code 1 |
| Missing subsample file within range | Lists all missing files, exit code 1 |
| No matching files in input directory | Error message, exit code 1 |
| Corrupt SAS7BDAT file | Error naming the specific file, exit code 1 |
| Any mid-pipeline failure | Temp files preserved at `{output}/_temp/` for debugging |
| Successful completion | Temp files cleaned up automatically |

## Running Tests

```bash
uv run pytest           # all 131 tests
uv run pytest -v        # verbose output
uv run pytest -k ingest # filter by name
```
