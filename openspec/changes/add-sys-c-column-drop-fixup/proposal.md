# Change: Enable fixup FORCE for SYS_C* extra columns

## Why
Target-side OceanBase sometimes contains extra system-generated columns named like `SYS_C000xxx`. These columns cannot be dropped directly; the cleanup path is `ALTER TABLE ... FORCE`. Today the fixup scripts only emit commented DROP COLUMN suggestions for extra columns, so SYS_C cleanup never happens unless users manually edit scripts. This causes repeated drift noise and blocks automation.

## What Changes
- Add a new configuration switch to emit `ALTER TABLE ... FORCE` for extra columns that match the `SYS_C\d+` pattern.
- Keep all other extra columns as commented DROP COLUMN suggestions to preserve safety.
- Apply only when SYS_C columns are **extra** (present in target, absent in source); comparison logic unchanged.
- Update documentation/config templates to expose the new switch and default behavior.

## Impact
- Affected specs: `generate-fixup`, `configuration-control`
- Affected code: `schema_diff_reconciler.py` (table ALTER generation)
- Risk: FORCE is a stronger operation; mitigated by opt-in switch and pattern guard.
