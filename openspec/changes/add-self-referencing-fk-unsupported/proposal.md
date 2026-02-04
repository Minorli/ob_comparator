# Change: Mark self-referencing foreign keys as unsupported in OceanBase

## Why
OceanBase Oracle mode rejects self-referencing foreign keys (FK references its own table). This causes fixup DDL failures and noisy reports. We need deterministic classification to prevent invalid fixup scripts and provide clear guidance.

## What Changes
- Detect self-referencing FK constraints in the source metadata.
- Classify them as **unsupported** with reason code `FK_SELF_REF`.
- Report them in `constraints_unsupported_detail_<ts>.txt` and `unsupported_objects_detail_<ts>.txt`.
- Exclude them from fixup generation (or route to unsupported output).

## Impact
- Affected spec: `compare-objects` (constraint compatibility rule)
- Affected code: constraint comparison/classification + unsupported reporting paths
- No change to fixup execution flow beyond excluding these constraints

## Validation Evidence (OB 4.2.5.7)
- `CREATE TABLE ... CONSTRAINT FK ... REFERENCES same_table` → `ORA-00600: ... -5317, Cannot add foreign key constraint`
- `ALTER TABLE ... ADD CONSTRAINT FK ... REFERENCES same_table` → same error
- `DISABLE` also fails

## Non-Goals
- Do not introduce a new compatibility switch in this change.
- Do not alter other constraint comparison logic.
