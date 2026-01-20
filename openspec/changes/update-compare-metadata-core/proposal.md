# Change: Core metadata comparison upgrades (constraints, columns, indexes)

## Why
Critical Oracle-to-OceanBase parity gaps remain in constraint semantics, numeric precision, character semantics, and function-based indexes. These gaps can cause false OKs and missed fixup opportunities, which is high risk for migration correctness.

## What Changes
- Expand Oracle metadata capture to include CHECK constraints, DELETE_RULE, SEARCH_CONDITION, VIRTUAL_COLUMN, and function-based index expressions.
- Expand OceanBase metadata capture to include DATA_LENGTH and CHAR_USED when available, with safe fallbacks when fields are missing or NULL.
- Enhance comparisons:
  - NUMBER precision/scale consistency checks.
  - CHECK constraints (normalized condition) and FK DELETE_RULE semantics.
  - Virtual column presence and expression parity.
  - Function-based index comparison by expression rather than SYS_NC column names.
- Enhance fixup generation:
  - ADD CHECK constraint DDL (excluding system-generated NOT NULL checks).
  - FK DDL includes ON DELETE rule when present.
  - ALTER ADD for virtual columns uses GENERATED ALWAYS AS expressions.
  - ALTER MODIFY for NUMBER precision/scale widening.
  - Index fallback generation uses function expressions when dbcat extraction is unavailable.

## Impact
- Affected specs: compare-objects, generate-fixup
- Affected code: schema_diff_reconciler.py, tests
- Requires Oracle/OB integration testing on representative schemas
