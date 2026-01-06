# Change: Update view DDL extraction to prefer dbcat

## Why
View fixup currently forces DBMS_METADATA extraction due to a historical dbcat bug. Now that dbcat can correctly extract and convert VIEW DDL, we should use it for consistency and let DBMS_METADATA serve only as a fallback.

## What Changes
- Prefer dbcat for VIEW DDL extraction during fixup generation.
- Keep OceanBase version-based cleanup (e.g., WITH CHECK OPTION handling) for VIEW DDL.
- Fall back to DBMS_METADATA when dbcat output is missing for a VIEW.
- Update DDL source logging/summary to reflect VIEW DDL sources.

## Impact
- Affected specs: generate-fixup
- Affected code: `schema_diff_reconciler.py`
- Affected docs: `README.md`, `docs/CHANGELOG.md`, `docs/ARCHITECTURE.md`, `docs/DEPLOYMENT.md`
