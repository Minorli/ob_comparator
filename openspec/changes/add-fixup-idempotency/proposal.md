# Change: Add idempotent fixup DDL generation

## Why
Fixup scripts are often re-run during iterative migration. Current CREATE DDL fails on re-run when objects already exist, forcing manual cleanup and slowing remediation.

## What Changes
- Add fixup_idempotent_mode (off|guard|replace|drop_create) and fixup_idempotent_types to control idempotent DDL generation.
- Use CREATE OR REPLACE for replace-capable types (VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER, TYPE, TYPE BODY, SYNONYM).
- For non-replaceable types (TABLE, SEQUENCE, INDEX, CONSTRAINT, JOB, SCHEDULE), emit guard blocks that check existence and skip or drop/create based on mode.
- Report a summary of guarded/replaced statements in the run log.

## Impact
- Affected specs: configuration-control, generate-fixup
- Affected code: schema_diff_reconciler.py (fixup DDL assembly, config parsing)
- Tests: add unit tests for guard/replace output and statement splitter handling of PL/SQL blocks
