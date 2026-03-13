## 1. Classification
- [x] 1.1 Decouple `TRIGGER_ON_TEMP_TABLE_UNSUPPORTED` from blacklist-only reason matching.
- [x] 1.2 Use temporary-table metadata directly so blacklist-disabled runs still classify temp-table triggers correctly.
- [x] 1.3 Keep non-trigger temporary-table dependents on their existing validation/fixup path.

## 2. Fixup Output
- [x] 2.1 Route temporary-table trigger DDL to `fixup_scripts/unsupported/trigger/`.
- [x] 2.2 Ensure such triggers do not enter normal `fixup_scripts/trigger/`.
- [x] 2.3 Annotate unsupported trigger DDL with explicit reason/action comments.

## 3. Reports
- [x] 3.1 Preserve `triggers_temp_table_unsupported_detail_<ts>.txt`.
- [x] 3.2 Keep summary/manual-actions/report index/report_db semantics consistent.

## 4. Tests and Verification
- [x] 4.1 Add unit tests for blacklist-disabled temporary-table trigger classification.
- [x] 4.2 Add unit tests for unsupported trigger DDL routing.
- [x] 4.3 Run `python3 -m py_compile $(git ls-files '*.py')`.
- [x] 4.4 Run focused unit tests.
- [x] 4.5 Run one Oracle + OceanBase real-DB reproduction case and inspect outputs.
