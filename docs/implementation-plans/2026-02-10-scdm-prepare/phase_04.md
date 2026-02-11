# SCDM Prepare Implementation Plan — Phase 4: Table Assembly

**Goal:** Assemble all 9 SCDM output tables from ingested data + crosswalks with correct column selections, join types, and sort orders.

**Architecture:** DuckDB SQL joins unioned subsample data with crosswalks, selects SCDM-specified columns, and applies ORDER BY per table's sort specification. Provider and Facility tables are synthesised directly from crosswalks. All 9 output tables are stored as DuckDB tables for Phase 5 export.

**Tech Stack:** Python 3.13.5, DuckDB (SQL engine)

**Scope:** 6 phases from original design (phase 4 of 6)

**Codebase verified:** 2026-02-10 — Exact SAS SQL extracted from `prepare_scdm.sas` lines 271-478. Column selections, join types, and sort orders confirmed against both SAS code and tables_documentation.json.

**Key discrepancy found:** The SAS enrollment SELECT omits PlanType and PayerType columns (present in tables_documentation.json), and the SAS enrollment JOIN omits the samplenum condition. The SAS demographic SELECT omits ImputedRace and ImputedHispanic. These discrepancies reflect that SynPUF data doesn't populate all SCDM columns. This plan follows the SAS code's actual SELECT statements as the source of truth, per the design plan's directive.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### scdm-prepare.AC4: Table Structure
- **scdm-prepare.AC4.1 Success:** Each of the 9 output tables contains exactly the columns specified by the SAS code's SELECT statements
- **scdm-prepare.AC4.2 Success:** Column order matches the SAS code's SELECT order
- **scdm-prepare.AC4.3 Success:** Join types match SAS code (INNER for PatID, LEFT for EncounterID/ProviderID/FacilityID)

### scdm-prepare.AC5: Sort Orders
- **scdm-prepare.AC5.1 Success:** Each output table is sorted per tables_documentation.json sort_order specification

### scdm-prepare.AC6: Synthesised Tables
- **scdm-prepare.AC6.1 Success:** Provider table built from providerid_crosswalk with Specialty='99' and Specialty_CodeType='2'
- **scdm-prepare.AC6.2 Success:** Facility table built from facilityid_crosswalk with empty Facility_Location
- **scdm-prepare.AC6.3 Success:** Provider/Facility exclude rows where original ID was NULL

---

## Table Assembly Reference

Exact SQL patterns per table, derived from SAS `prepare_scdm.sas`:

| Table | Crosswalk Joins | Join Types | Sort Order | Output Columns |
|-------|----------------|------------|------------|----------------|
| enrollment | patid_crosswalk | INNER (PatID) | PatID, Enr_Start, Enr_End, MedCov, DrugCov, Chart | PatID, Enr_Start, Enr_End, MedCov, DrugCov, Chart, PlanType, PayerType |
| demographic | patid_crosswalk | INNER (PatID) | PatID | PatID, Birth_Date, Sex, Hispanic, Race, PostalCode, PostalCode_Date |
| dispensing | patid_crosswalk | INNER (PatID) | PatID, RxDate | PatID, ProviderID, RxDate, Rx, Rx_CodeType, RxSup, RxAmt |
| encounter | patid, encounterid, facilityid crosswalks | INNER(PatID), LEFT(EncounterID), LEFT(FacilityID) | PatID, ADate | PatID, EncounterID, ADate, DDate, EncType, FacilityID, Discharge_Disposition, Discharge_Status, DRG, DRG_Type, Admitting_Source |
| diagnosis | patid, encounterid, providerid crosswalks | INNER(PatID), LEFT(EncounterID), LEFT(ProviderID) | PatID, ADate | PatID, EncounterID, ADate, ProviderID, EncType, DX, Dx_Codetype, OrigDX, PDX, PAdmit |
| procedure | patid, encounterid, providerid crosswalks | INNER(PatID), LEFT(EncounterID), LEFT(ProviderID) | PatID, ADate | PatID, EncounterID, ADate, ProviderID, EncType, PX, PX_CodeType, OrigPX |
| death | patid_crosswalk | INNER (PatID) | PatID | PatID, DeathDt, DtImpute, Source, Confidence |
| provider | (synthesised from crosswalk) | N/A | ProviderID | ProviderID, Specialty, Specialty_CodeType |
| facility | (synthesised from crosswalk) | N/A | FacilityID | FacilityID, Facility_Location |

**Note on Dispensing:** The SAS code does NOT apply a ProviderID crosswalk join for dispensing — ProviderID is passed through directly from the source data. This differs from diagnosis/procedure which DO crosswalk ProviderID.

**Note on column naming:** Use tables_documentation.json casing for output columns (e.g., `Dx_Codetype` not `DX_CodeType`, `PostalCode_Date` not `PostalCode_date`, `DtImpute` not `Dtimpute`). SAS is case-insensitive; Python output should match the documentation.

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Add table assembly SQL generation to transform.py

**Verifies:** scdm-prepare.AC4.1, scdm-prepare.AC4.2, scdm-prepare.AC4.3, scdm-prepare.AC5.1

**Files:**
- Modify: `src/scdm_prepare/transform.py`

**Implementation:**

Add an `assemble_tables(con, temp_dir)` function to `transform.py` that builds all 9 SCDM output tables as DuckDB tables. This function should be called AFTER `build_crosswalks()`.

For each of the 7 data-derived tables (enrollment, demographic, dispensing, encounter, diagnosis, procedure, death), generate and execute a DuckDB CREATE TABLE AS SELECT query following this structure:

1. SELECT the output columns from the source data (aliased from `a.`) and crosswalks (aliased from `b.`, `c.`, `d.`)
2. FROM `read_parquet('{temp_dir}/{table}_*.parquet')` AS a
3. INNER JOIN patid_crosswalk on `a.PatID = b.orig_PatID AND a.samplenum = b.samplenum`
4. LEFT JOIN other crosswalks as needed (matching both original ID and samplenum)
5. ORDER BY the table's sort keys from `schema.py`

**Specific SQL patterns per table (matching SAS code):**

**Enrollment:**
```sql
CREATE OR REPLACE TABLE enrollment AS
SELECT b.PatID, a.Enr_Start, a.Enr_End, a.MedCov, a.DrugCov, a.Chart,
       a.PlanType, a.PayerType
FROM read_parquet('{temp_dir}/enrollment_*.parquet') AS a
INNER JOIN patid_crosswalk AS b
  ON a.PatID = b.orig_PatID AND a.samplenum = b.samplenum
ORDER BY b.PatID, a.Enr_Start, a.Enr_End, a.MedCov, a.DrugCov, a.Chart
```
Note: The original SAS code omits the samplenum join condition for enrollment. This plan INCLUDES it for correctness. Without it, patients with the same original ID across different subsamples would produce incorrect cross-joins.

**Demographic:**
```sql
CREATE OR REPLACE TABLE demographic AS
SELECT b.PatID, a.Birth_Date, a.Sex, a.Hispanic, a.Race,
       a.PostalCode, a.PostalCode_Date
FROM read_parquet('{temp_dir}/demographic_*.parquet') AS a
INNER JOIN patid_crosswalk AS b
  ON a.PatID = b.orig_PatID AND a.samplenum = b.samplenum
ORDER BY b.PatID
```

**Dispensing:**
```sql
CREATE OR REPLACE TABLE dispensing AS
SELECT b.PatID, a.ProviderID, a.RxDate, a.Rx, a.Rx_CodeType,
       a.RxSup, a.RxAmt
FROM read_parquet('{temp_dir}/dispensing_*.parquet') AS a
INNER JOIN patid_crosswalk AS b
  ON a.PatID = b.orig_PatID AND a.samplenum = b.samplenum
ORDER BY b.PatID, a.RxDate
```
Note: ProviderID is NOT crosswalked for dispensing — passed through from source (matching SAS behaviour).

Note: The SAS code sorts dispensing by 5 keys (`PatID RxDate Rx_CodeType Rx ProviderID`), but tables_documentation.json specifies only `PatID, RxDate`. This plan uses the tables_documentation.json sort order (2 keys), consistent with how other tables are sorted.

**Encounter:**
```sql
CREATE OR REPLACE TABLE encounter AS
SELECT b.PatID, c.EncounterID, a.ADate, a.DDate, a.EncType,
       d.FacilityID, a.Discharge_Disposition, a.Discharge_Status,
       a.DRG, a.DRG_Type, a.Admitting_Source
FROM read_parquet('{temp_dir}/encounter_*.parquet') AS a
INNER JOIN patid_crosswalk AS b
  ON a.PatID = b.orig_PatID AND a.samplenum = b.samplenum
LEFT JOIN encounterid_crosswalk AS c
  ON a.EncounterID = c.orig_EncounterID AND a.samplenum = c.samplenum
LEFT JOIN facilityid_crosswalk AS d
  ON a.FacilityID = d.orig_FacilityID AND a.samplenum = d.samplenum
ORDER BY b.PatID, a.ADate
```

**Diagnosis:**
```sql
CREATE OR REPLACE TABLE diagnosis AS
SELECT b.PatID, c.EncounterID, a.ADate, d.ProviderID, a.EncType,
       a.DX, a.Dx_Codetype, a.OrigDX, a.PDX, a.PAdmit
FROM read_parquet('{temp_dir}/diagnosis_*.parquet') AS a
INNER JOIN patid_crosswalk AS b
  ON a.PatID = b.orig_PatID AND a.samplenum = b.samplenum
LEFT JOIN encounterid_crosswalk AS c
  ON a.EncounterID = c.orig_EncounterID AND a.samplenum = c.samplenum
LEFT JOIN providerid_crosswalk AS d
  ON a.ProviderID = d.orig_ProviderID AND a.samplenum = d.samplenum
ORDER BY b.PatID, a.ADate
```

**Procedure:**
```sql
CREATE OR REPLACE TABLE procedure AS
SELECT b.PatID, c.EncounterID, a.ADate, d.ProviderID, a.EncType,
       a.PX, a.PX_CodeType, a.OrigPX
FROM read_parquet('{temp_dir}/procedure_*.parquet') AS a
INNER JOIN patid_crosswalk AS b
  ON a.PatID = b.orig_PatID AND a.samplenum = b.samplenum
LEFT JOIN encounterid_crosswalk AS c
  ON a.EncounterID = c.orig_EncounterID AND a.samplenum = c.samplenum
LEFT JOIN providerid_crosswalk AS d
  ON a.ProviderID = d.orig_ProviderID AND a.samplenum = d.samplenum
ORDER BY b.PatID, a.ADate
```

**Death:**
```sql
CREATE OR REPLACE TABLE death AS
SELECT b.PatID, a.DeathDt, a.DtImpute, a.Source, a.Confidence
FROM read_parquet('{temp_dir}/death_*.parquet') AS a
INNER JOIN patid_crosswalk AS b
  ON a.PatID = b.orig_PatID AND a.samplenum = b.samplenum
ORDER BY b.PatID
```

The function should build these SQL strings dynamically using the schema definitions rather than hardcoding each query. Use the `TABLES` dict from `schema.py` for column lists and sort keys, and `CROSSWALKS` for join configuration. The specific SQL patterns above serve as reference for the dynamic generation logic.

**Testing:**
Tests must verify each AC listed above:
- scdm-prepare.AC4.1: Each output table contains exactly the expected columns
- scdm-prepare.AC4.2: Column order matches (verify by checking DataFrame column list)
- scdm-prepare.AC4.3: Rows with missing PatID are excluded (INNER JOIN). Rows with missing EncounterID/ProviderID/FacilityID are included with NULL new IDs (LEFT JOIN).
- scdm-prepare.AC5.1: Output is sorted by the expected sort keys

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_transform.py -v -k assemble`
Expected: All assembly tests pass.

**Commit:** `feat: add table assembly with crosswalk joins`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add Provider and Facility synthesis to transform.py

**Verifies:** scdm-prepare.AC6.1, scdm-prepare.AC6.2, scdm-prepare.AC6.3

**Files:**
- Modify: `src/scdm_prepare/transform.py`

**Implementation:**

Add Provider and Facility table synthesis to the `assemble_tables()` function (or add a separate `synthesise_tables(con)` function called after assembly).

**Provider table:**
```sql
CREATE OR REPLACE TABLE provider AS
SELECT
    ProviderID,
    '99' AS Specialty,
    '2' AS Specialty_CodeType
FROM providerid_crosswalk
WHERE orig_ProviderID IS NOT NULL
ORDER BY ProviderID
```
- Built from `providerid_crosswalk`, not from source data
- Hardcoded `Specialty='99'`, `Specialty_CodeType='2'`
- Excludes rows where original ProviderID was NULL (AC6.3)

**Facility table:**
```sql
CREATE OR REPLACE TABLE facility AS
SELECT
    FacilityID,
    '' AS Facility_Location
FROM facilityid_crosswalk
WHERE orig_FacilityID IS NOT NULL
ORDER BY FacilityID
```
- Built from `facilityid_crosswalk`, not from source data
- Empty `Facility_Location` string
- Excludes rows where original FacilityID was NULL (AC6.3)

**Testing:**
Tests must verify each AC listed above:
- scdm-prepare.AC6.1: Provider table has columns (ProviderID, Specialty, Specialty_CodeType) with Specialty='99' and Specialty_CodeType='2' for all rows
- scdm-prepare.AC6.2: Facility table has columns (FacilityID, Facility_Location) with empty Facility_Location for all rows
- scdm-prepare.AC6.3: If crosswalk contains a NULL original ID entry, the synthesised table should NOT include it

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**

Run: `uv run pytest tests/test_transform.py -v -k "provider or facility"`
Expected: All synthesis tests pass.

**Commit:** `feat: add Provider and Facility table synthesis`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->
