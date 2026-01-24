## 1. Implementation
- [ ] Update fixup execution to apply ALTER SESSION SET CURRENT_SCHEMA to each statement when present.
- [ ] Skip VIEW/MATERIALIZED VIEW compile statements in fixup generation.
- [ ] Filter grant generation to allowed owners (source schemas + remap targets) and remove session directives from grant files.
- [ ] Add unit tests for current_schema statement handling in run_fixup.
- [ ] Add unit tests for grant owner filtering and compile skip behavior.

## 2. Validation
- [ ] Run python3 -m py_compile $(git ls-files '*.py')
- [ ] Run python3 -m unittest
- [ ] Re-run fixup generation + run_fixup to confirm grant/compile failures resolved.
