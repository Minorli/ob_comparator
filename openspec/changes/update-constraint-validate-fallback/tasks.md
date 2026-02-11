## 1. Implementation
- [ ] 1.1 Add `constraint_missing_fixup_validate_mode` default/validation/wizard handling in `schema_diff_reconciler.py`.
- [ ] 1.2 Add `constraint_missing_fixup_validate_mode` into `config.ini.template` and `readme_config.txt`.
- [ ] 1.3 Update missing-constraint DDL generation to honor mode (`safe_novalidate`/`source`/`force_validate`).
- [ ] 1.4 Add deferred validation script generation under `fixup_scripts/constraint_validate_later`.
- [ ] 1.5 Export deferred validation summary/detail report artifacts.
- [ ] 1.6 Add run_fixup ORA-02298 classification and non-retry handling with clear summary hints.
- [ ] 1.7 Update HOW TO queries/docs for deferred validation visibility.

## 2. Tests
- [ ] 2.1 Add/adjust unit tests for mode normalization and SQL generation.
- [ ] 2.2 Add/adjust unit tests for run_fixup ORA-02298 classification path.
- [ ] 2.3 Run `python3 -m py_compile $(git ls-files '*.py')`.
- [ ] 2.4 Run unit test suite and verify pass.
- [ ] 2.5 Run Oracle/OB integration verification for VALIDATE vs NOVALIDATE behavior.

## 3. Validation
- [ ] 3.1 `openspec validate update-constraint-validate-fallback --strict` passes.
- [ ] 3.2 Ensure spec/task/proposal remain in sync with implementation.
