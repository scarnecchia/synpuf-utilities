# SCDM Prepare Implementation Plan — Phase 3: Crosswalk Generation

**Goal:** Build DuckDB-based crosswalk generation that assigns globally unique sequential IDs across subsamples for PatID, EncounterID, ProviderID, and FacilityID.

**Architecture:** DuckDB SQL reads temp parquet files (output of Phase 2 ingestion), extracts distinct (original_id, samplenum) pairs, and assigns sequential new IDs via `ROW_NUMBER()`. Crosswalk tables are stored as in-memory DuckDB tables for use by Phase 4 table assembly.

**Tech Stack:** Python 3.13.5, DuckDB (SQL engine + Python API), polars (result conversion)

**Scope:** 6 phases from original design (phase 3 of 6)

**Codebase verified:** 2026-02-10 — SAS reference implementation `assign_idvar.sas` macro uses `PROC SORT NODUPKEY` + sequential `_n_` assignment, grouped by samplenum. NULL original IDs map to `.U` (SAS missing). DuckDB Python API confirmed: `con.execute(sql).pl()` returns polars DataFrames, `read_parquet()` supports glob patterns, `ROW_NUMBER()` includes NULLs in numbering (so NULLs must be filtered before numbering).

---

## Acceptance Criteria Coverage

This phase implements and tests:

### scdm-prepare.AC3: ID Uniqueness
- **scdm-prepare.AC3.1 Success:** Crosswalks assign globally unique sequential IDs across subsamples (same original ID in different subsamples gets different new IDs)
- **scdm-prepare.AC3.2 Success:** NULL/missing original IDs map to NULL in crosswalk output
- **scdm-prepare.AC3.3 Success:** Four crosswalks produced: patid, encounterid, providerid, facilityid

---

## Key Design Decision: Crosswalk Source Tables

Each crosswalk extracts distinct IDs from specific source tables (matching the SAS `prepare_scdm.sas` logic):

| Crosswalk | Source Column | Source Table(s) | Notes |
|-----------|--------------|-----------------|-------|
| patid_crosswalk | PatID | demographic | All patients appear in demographic |
| encounterid_crosswalk | EncounterID | encounter | All encounters in one table |
| providerid_crosswalk | ProviderID | provider | Source provider files contain all provider IDs |
| facilityid_crosswalk | FacilityID | facility | Source facility files contain all facility IDs |

The SAS `assign_idvar` macro sorts by `(samplenum, orig_id)` and assigns sequential `_n_`. The DuckDB equivalent: `ROW_NUMBER() OVER (ORDER BY samplenum, orig_id)` after filtering out NULLs.

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Add crosswalk source configuration to schema.py

**Files:**
- Modify: `src/scdm_prepare/schema.py`

**Implementation:**

Add a `CrosswalkDef` dataclass and a `CROSSWALKS` dictionary to `schema.py`. Each crosswalk definition specifies:
- `id_column`: the column name to extract (e.g., `"PatID"`)
- `source_tables`: list of table names to extract IDs from (e.g., `["demographic"]`)
- `crosswalk_name`: the name of the crosswalk table (e.g., `"patid_crosswalk"`)

```python
@dataclass(frozen=True)
class CrosswalkDef:
    id_column: str
    source_tables: tuple[str, ...]
    crosswalk_name: str
```

Define four crosswalks:
- `patid`: id_column="PatID", source_tables=("demographic",), crosswalk_name="patid_crosswalk"
- `encounterid`: id_column="EncounterID", source_tables=("encounter",), crosswalk_name="encounterid_crosswalk"
- `providerid`: id_column="ProviderID", source_tables=("provider",), crosswalk_name="providerid_crosswalk"
- `facilityid`: id_column="FacilityID", source_tables=("facility",), crosswalk_name="facilityid_crosswalk"

**Verification:**

Run: `uv run python -c "from scdm_prepare.schema import CROSSWALKS; print(list(CROSSWALKS.keys()))"`
Expected: `['patid', 'encounterid', 'providerid', 'facilityid']`

**Commit:** `feat: add crosswalk source definitions to schema`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create transform.py with crosswalk generation

**Verifies:** scdm-prepare.AC3.1, scdm-prepare.AC3.2, scdm-prepare.AC3.3

**Files:**
- Create: `src/scdm_prepare/transform.py`

**Implementation:**

Create a `build_crosswalks(con, temp_dir)` function that:

1. Takes a DuckDB connection and the temp directory path (containing ingested parquet files)
2. For each crosswalk defined in `CROSSWALKS`:
   a. SELECT DISTINCT `{id_column}` AS `orig_{id_column}`, `samplenum` from the source table — filtering out NULLs
   b. Wrap in ROW_NUMBER() to assign sequential new IDs
   c. Create the crosswalk as a DuckDB table

Each crosswalk uses a single source table. The SQL pattern:

```sql
CREATE OR REPLACE TABLE {crosswalk_name} AS
SELECT
    orig_{id_column},
    samplenum,
    ROW_NUMBER() OVER (ORDER BY samplenum, orig_{id_column}) AS {id_column}
FROM (
    SELECT DISTINCT
        {id_column} AS orig_{id_column},
        samplenum
    FROM read_parquet('{temp_dir}/{source_table}_*.parquet')
    WHERE {id_column} IS NOT NULL
)
```

Also create a helper `get_crosswalk(con, crosswalk_name)` that returns the crosswalk as a polars DataFrame via `con.execute(f"SELECT * FROM {crosswalk_name}").pl()`.

**Key behaviours:**
- NULL original IDs are excluded from the crosswalk (filtered by WHERE IS NOT NULL). When tables LEFT JOIN on the crosswalk, NULL originals naturally produce NULL new IDs (AC3.2).
- Same original ID in different subsamples gets different ROW_NUMBER values because samplenum is part of the DISTINCT key (AC3.1).
- All four crosswalks are produced (AC3.3).

**Testing:**
Tests must verify each AC listed above:
- scdm-prepare.AC3.1: Insert PatID "ABC" in samplenum 1 and PatID "ABC" in samplenum 2 into fixtures. After crosswalk, they should have different new PatID values.
- scdm-prepare.AC3.2: Insert a row with NULL PatID. Crosswalk should not contain a row for it (NULL maps to NULL via LEFT JOIN absence).
- scdm-prepare.AC3.3: After `build_crosswalks()`, all four crosswalk tables exist in the DuckDB connection.

Tests should create temp parquet fixtures directly (via polars write_parquet), call `build_crosswalks()`, and verify crosswalk contents.

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_transform.py -v -k crosswalk`
Expected: All crosswalk tests pass.

**Commit:** `feat: add DuckDB crosswalk generation`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Crosswalk edge case tests

**Verifies:** scdm-prepare.AC3.1, scdm-prepare.AC3.2

**Files:**
- Modify: `tests/test_transform.py`

**Testing:**
Additional edge cases:
- Crosswalk IDs are sequential starting from 1 (no gaps)
- Single subsample produces correct crosswalk (no samplenum deduplication issues)
- Empty source table produces empty crosswalk (no errors)
- Duplicate IDs within a single source table are deduplicated (same ProviderID appearing multiple times in provider table gets one crosswalk entry)
- Large ID values (strings, large integers) are handled correctly

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_transform.py -v`
Expected: All tests pass.

**Commit:** `test: add crosswalk edge case tests`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->
