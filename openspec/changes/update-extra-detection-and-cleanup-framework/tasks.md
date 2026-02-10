## 1. Implementation
- [x] 1.1 Update primary object comparison to include print-only `MATERIALIZED VIEW` in expected target set and extra-target detection.
- [x] 1.2 Keep PACKAGE/PACKAGE BODY excluded from extra-target detection (existing behavior).
- [x] 1.3 Reduce rough INDEX/CONSTRAINT pre-warning noise in `compute_object_counts` and keep final summary driven by reconciled semantic results.
- [x] 1.4 Add `generate_extra_cleanup` config key (default false) to loader, validator path, and wizard prompts.
- [x] 1.5 Implement opt-in cleanup-candidate export to `fixup_scripts/cleanup_candidates/` as commented SQL candidates.
- [x] 1.6 Update `config.ini.template` and `readme_config.txt` for the new switch.

## 2. Tests
- [x] 2.1 Unit test: MVIEW print-only object can appear in `extra_targets` when present only on target.
- [x] 2.2 Unit test: PACKAGE/PACKAGE BODY remain excluded from `extra_targets`.
- [x] 2.3 Unit test: cleanup-candidate export writes expected commented entries when enabled.

## 3. Verification
- [x] 3.1 Run unit tests for changed areas.
- [x] 3.2 Run `python3 -m py_compile $(git ls-files '*.py')`.
- [ ] 3.3 Run one real `schema_diff_reconciler.py config.ini` execution and verify output paths/logs.
