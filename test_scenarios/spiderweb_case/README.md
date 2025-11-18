# Spiderweb Remap Validation Scenario

This case fabricates a dense, multi-schema migration so that `db_comparator_fixup_release.py` is forced to exercise every code path: tables, views, synonym remaps, sequences, triggers, procedures, functions, packages (spec + body), indexes, constraints, and the VARCHAR/VARCHAR2 1.5× length rule.  The Oracle source side contains five schemas (`ORA_SALES`, `ORA_REF`, `ORA_LOG`, `ORA_UTIL`, `ORA_SEC`), while the OceanBase side spreads the objects across six different schemas plus one schema that keeps its original name.  Most objects jump to new schemas, some stay in place, and several targets intentionally omit or misconfigure metadata so the comparator must flag missing, extra, and mismatched objects.

## Contents

```
test_scenarios/spiderweb_case/
├── README.md                       <- This document
├── oracle_setup.sql                <- Complete Oracle-side DDL (users + objects)
├── oceanbase_setup.sql             <- OceanBase-side DDL with purposeful gaps/mismatches
└── remap_rules_spiderweb.txt       <- Cross-schema remap matrix used by the comparator
```

## Schema / Object Matrix (Oracle → OceanBase)

| Source schema | Key objects (non-exhaustive) | Remap target(s) | Notes |
| ------------- | --------------------------- | --------------- | ----- |
| `ORA_SALES`   | CUSTOMER_DIM table, ORDER_FACT table, SEQ_CUSTOMER, SEQ_ORDER, TRG_ORDER_FACT_BI, PKG_ORDER_MGMT (spec/body), SP_CREATE_ORDER, FN_CUSTOMER_SCORE, VW_HOT_CUSTOMERS | `OB_ODS`, `OB_DW`, `OB_APP` | Columns include many VARCHAR2 fields to test the 1.5× rule. ORDER_FACT has FKs to ORA_REF objects, so all dependent DDL must be rebuilt after remap. |
| `ORA_REF`     | REGION_DIM table, SHIP_METHOD table, SEQ_REGION | `OB_STAGE` | Supplies lookup data and FK targets for ORA_SALES. |
| `ORA_LOG`     | AUDIT_EVENTS / ERROR_LOG tables, AUDIT_ARCHIVE table, SEQ_AUDIT / SEQ_ERROR, TRG_AUDIT_EVENTS_BI, SP_ARCHIVE_AUDIT, FN_LAST_AUDIT_ID | `OB_AUDIT` | Makes the trigger/sequence/PLSQL coverage independent from the sales objects. |
| `ORA_UTIL`    | SYN_CUSTOMER / SYN_ORDER / SYN_REGION synonyms, VW_CUSTOMER_REGION view, PKG_DATA_ROUTER (spec/body) | `OB_SHARE` | Demonstrates synonym remap and view remap, plus a second package pair. |
| `ORA_SEC`     | USER_MATRIX table, VW_LOGIN_FAILURE view | stays `ORA_SEC` | Ensures the tool simultaneously handles remapped and untouched schemas. |

The remap file routes individual objects, not just schemas, so tables, packages, sequences, triggers, synonyms, and standalone PL/SQL units hop across multiple targets.  Objects not listed in `remap_rules_spiderweb.txt` keep their original schema name, which mirrors the “spiderweb” migration topology described in the requirements.

## Execution Steps

1. **Prepare Oracle schemas.**  
   - Connect as a privileged user (e.g. `sqlplus / as sysdba`).  
   - Execute `@test_scenarios/spiderweb_case/oracle_setup.sql`.  
   - The script creates the five source schemas, grants privileges, and builds every source object with clean dependencies.

2. **Prepare OceanBase schemas (Oracle-compatible tenant).**  
   - Connect with `obclient` (or any SQL tool) as a user that can create schemas and objects.  
   - Execute `@test_scenarios/spiderweb_case/oceanbase_setup.sql`.  
   - This script mirrors the migration target but intentionally introduces issues:
        * Missing columns (`VIP_FLAG`, `ORDER_NOTE`, etc.) and wrong VARCHAR lengths (not 1.5×).
        * Missing indexes, FKs, triggers, and sequences.
        * Packages without bodies, absent procedures/functions, and partially created synonyms.
        * Extra objects such as `OB_DW.EXTRA_ORPHAN_SEQ` to test the “extra object” reporting path.

3. **Wire in the remap file.**  
   - Replace or point `remap_file` in `db.ini` to `test_scenarios/spiderweb_case/remap_rules_spiderweb.txt`.  
   - Update `source_schemas` to `ORA_SALES, ORA_REF, ORA_LOG, ORA_UTIL, ORA_SEC`.

4. **Run the comparator.**  
   ```
   python3 db_comparator_fixup_release.py
   ```
   Use the credentials for the Oracle PDB and the OB tenant where you executed the scripts.

5. **Inspect output.**  
   - The HTML-style console report should enumerate missing/mismatch object counts across every type.  
   - `fix_up/` should contain CREATE/ALTER scripts for each difference.

## What the Comparator Should Detect

This scenario intentionally exercises every category reported by the tool:

- **Tables & Column Rules**
  - `OB_ODS.CUST_DIM` is missing the `VIP_FLAG` column, has a truncated `CUSTOMER_NAME` (120 vs required 180), and omits the FK to `REGION_DIM`.
  - `OB_DW.F_ORDER_METRIC` lacks `ORDER_NOTE`, the check constraint on `STATUS`, and the `SHIP_METHOD_ID` FK.
  - `OB_STAGE.REGION_DIM` contains an extra column `Migrated_by` and drops the unique constraint on `REGION_CODE`.
  - `OB_STAGE.SHIP_METHOD` keeps VARCHAR2 lengths unchanged (20/80) and misses its FK.
  - `ORA_SEC.USER_MATRIX` is untouched so it should be reported as OK, showing the tool can mix remapped and non-remapped schemas.

- **Indexes / Constraints**
  - `IDX_ORDER_FACT_CUST_DT`, `UK_ORDER_FACT_CODE`, and `IDX_ERROR_LOG_CODE` are absent on OceanBase.
  - `OB_DW` defines an extra `IDX_F_ORDER_STATUS` that has no Oracle counterpart.

- **Sequences**
  - `OB_ODS.SEQ_CUST_DIM` and `OB_AUDIT.SEQ_AUDIT` are missing.
  - `OB_DW.EXTRA_ORPHAN_SEQ` exists only in OB and should be flagged as extra.

- **Triggers**
  - `TRG_ORDER_FACT_BI` and `TRG_AUDIT_EVENTS_BI` are not created on the OB side.

- **Procedures, Functions, Packages, Package Bodies**
  - `OB_APP.PKG_ORDER_MGMT` is created with the spec only (no body).  
  - `OB_APP.SP_CREATE_ORDER`, `OB_APP.FN_CUSTOMER_SCORE`, and the entire `OB_SHARE.PKG_DATA_ROUTER` pair are intentionally absent.

- **Views & Synonyms**
  - `OB_DW.VW_HOT_CUST` and `OB_SHARE.VW_CUSTOMER_REGION` are missing.  
  - Only one of the three required synonyms (`SYN_REGION`) is present, so two should be reported as missing.

- **VARCHAR/VARCHAR2 1.5× Expansion**
  - The OB tables deliberately copy Oracle lengths instead of enlarging them: e.g., `CUSTOMER_NAME VARCHAR(120)` vs the expected 180, `ORDER_CODE VARCHAR(40)` vs 60, `ERROR_MESSAGE VARCHAR(400)` vs 600.  This covers both tables with and without remap to ensure the validator catches them through the column metadata comparison logic.

Use the `Expected Comparator Findings` section inside this README as the acceptance criteria when you review the generated report.

## Optional Extensions

- Multiply the number of objects per schema (copy/paste blocks inside the SQL scripts) to stress-test dump sizes.  
- Introduce contradictory remap lines (duplicate targets or circular remaps) to validate the tool’s `extraneous` and “invalid remap” handling.  
- Toggle the OceanBase script to gradually fix objects and confirm that `fix_up/*` stops generating entries when the schemas align.

This single scenario is intentionally noisy; you can run it as-is whenever you need to regression-test the comparator or use it as a template for more focused cases.
