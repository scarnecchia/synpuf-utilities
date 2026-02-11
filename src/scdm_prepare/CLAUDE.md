# scdm-prepare

Last verified: 2026-02-11

## Purpose
Combines CMS SynPUF SAS7BDAT subsample files into 9 standardised SCDM
(Sentinel Common Data Model) tables, with globally unique IDs across subsamples.

## Contracts
- **Exposes**: `scdm-prepare` CLI (Typer app via `cli:app`)
- **Guarantees**:
  - Produces exactly 9 SCDM tables: enrollment, demographic, dispensing, encounter, diagnosis, procedure, death, provider, facility
  - IDs (PatID, EncounterID, ProviderID, FacilityID) are re-mapped to globally unique sequential integers via crosswalk tables
  - Output formats: parquet (zstd), CSV (with headers), NDJSON
  - Temp files preserved on failure, cleaned on success
- **Expects**: Directory of SAS7BDAT files named `{table}_{N}.sas7bdat` with all 9 table types present per subsample

## Pipeline Stages
1. **discover** (ingest.py) - Scan input dir, validate all 9 tables exist per subsample
2. **ingest** (ingest.py) - Chunked SAS reading via pyreadstat, write temp parquet with `samplenum` column
3. **transform** (transform.py) - DuckDB crosswalk generation + table assembly with JOIN remapping
4. **export** (export.py) - DuckDB COPY TO for parquet/CSV/NDJSON

## Dependencies
- **Uses**: DuckDB (SQL transforms), Polars (ingestion I/O), pyreadstat (SAS reading), Typer/Rich (CLI/progress)
- **Used by**: CLI consumers
- **Boundary**: No imports from legacy SAS directories

## Key Decisions
- DuckDB in-memory for transforms: avoids materialising large intermediates in Python
- Crosswalk pattern: `ROW_NUMBER() OVER (ORDER BY samplenum, orig_id)` produces deterministic sequential IDs
- Provider/Facility tables synthesised from crosswalks (not from source data) with hardcoded placeholder values
- pyreadstat chunked reading: handles large SAS files without loading entire file into memory
- `samplenum` column injected at ingestion: enables same original ID in different subsamples to get different global IDs

## Invariants
- TABLES dict in schema.py is the single source of truth for table definitions
- CROSSWALKS dict in schema.py defines all ID remapping relationships
- Every table with PatID uses INNER JOIN on patid_crosswalk (filters to known patients)
- EncounterID, ProviderID, FacilityID use LEFT JOIN (NULL originals become NULL new IDs)
- Temp parquet lives at `{output_dir}/_temp/` and is cleaned on success only

## Key Files
- `schema.py` - TableDef/CrosswalkDef dataclasses, TABLES and CROSSWALKS registries
- `cli.py` - Typer CLI with pipeline orchestration
- `ingest.py` - Subsample discovery and chunked SAS-to-parquet ingestion
- `transform.py` - DuckDB crosswalk building and table assembly
- `export.py` - DuckDB COPY TO for all output formats
- `progress.py` - ProgressTracker protocol and Rich-based implementation

## Gotchas
- Tests use `.parquet` file extension (not `.sas7bdat`) via `--file-ext` hidden CLI flag
- `discover_subsamples` validates ALL 9 table types exist for each subsample in range; missing any is a hard error
- Crosswalk NULL handling: NULL original IDs excluded from crosswalk; LEFT JOINs naturally produce NULL new IDs
