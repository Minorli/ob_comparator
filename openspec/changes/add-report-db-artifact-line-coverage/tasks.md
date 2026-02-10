## 1. Implementation
- [x] 1.1 Add `DIFF_REPORT_ARTIFACT_LINE` table and indexes in report DB bootstrap DDL.
- [x] 1.2 Implement artifact line scanner for run directory txt files (preserve line order and blank lines).
- [x] 1.3 Implement batched insert pipeline for artifact line rows with CLOB-safe SQL literals.
- [x] 1.4 Integrate artifact line persistence into `save_report_to_db` for `report_db_store_scope=full`.
- [x] 1.5 Update artifact coverage status semantics under full scope.

## 2. Docs
- [x] 2.1 Update HOW_TO SQL playbook with artifact-line query templates.
- [x] 2.2 Update `readme_config.txt` and `docs/ADVANCED_USAGE.md` to explain 100% txt coverage path.

## 3. Validation
- [x] 3.1 Run `python3 -m py_compile $(git ls-files '*.py')`.
- [x] 3.2 Run `.venv/bin/python -m unittest -q test_schema_diff_reconciler.py`.
- [x] 3.3 Run one real main program execution and verify `DIFF_REPORT_ARTIFACT_LINE` has rows for all txt artifacts in latest run.
