# Change: Preserve and compare column visibility

## Why
Oracle INVISIBLE columns affect application behavior (SELECT * results and column exposure). Current comparison ignores visibility mismatches and fixup DDL does not preserve INVISIBLE, which can lead to behavioral drift after migration.

## What Changes
- Load INVISIBLE_COLUMN metadata from Oracle and OceanBase (fallback to safe HIDDEN_COLUMN detection when needed).
- Compare column visibility and report mismatches when a source column is INVISIBLE but the target is visible (and vice versa).
- Generate ALTER TABLE MODIFY ... INVISIBLE/VISIBLE statements to align visibility when policy allows.
- Ensure CREATE TABLE DDL preserves INVISIBLE columns via post-processing or follow-up ALTER statements.
- Add column_visibility_policy (auto|enforce|ignore) to control enforcement.

## Impact
- Affected specs: compare-objects, generate-fixup, configuration-control
- Affected code: schema_diff_reconciler.py (metadata load, comparison, fixup DDL generation)
- Tests: add unit tests for visibility detection and DDL output; integration tests against Oracle/OB when visibility metadata is available
