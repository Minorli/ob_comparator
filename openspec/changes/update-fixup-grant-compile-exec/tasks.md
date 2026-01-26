## 1. Implementation
- [x] Update fixup execution to apply ALTER SESSION SET CURRENT_SCHEMA to each statement when present.
- [x] Skip VIEW/MATERIALIZED VIEW/TYPE BODY compile statements in fixup generation.
- [x] Skip VIEW/MATERIALIZED VIEW/TYPE BODY recompilation in run_fixup when --recompile is enabled.
- [ ] Filter grant generation to allowed owners (source schemas + remap targets) and remove session directives from grant files.
- [x] Add unit tests for current_schema statement handling in run_fixup.
- [x] Add unit tests for grant owner filtering and compile skip/recompile skip behavior.

## 2. Validation
- [x] Run python3 -m py_compile $(git ls-files '*.py')
- [x] Run python3 -m unittest
- [ ] Re-run fixup generation + run_fixup to confirm grant/compile failures resolved.
