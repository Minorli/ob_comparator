# Change: Use constraint INDEX_NAME to align expression index matching

## Why
Expression-based UNIQUE indexes can still be reported as missing because index comparison only checks
constraint column lists. In OceanBase, expression constraints often expose SYS_NC columns, which do
not match normalized index expressions. We verified that `DBA_CONSTRAINTS.INDEX_NAME` exists in the
current target environment, enabling a reliable link from constraints to their backing index
expressions.

## What Changes
- Capture `INDEX_NAME` for target PK/UK constraints when available.
- Build constraint-backed index signatures from the referenced index (including expressions) to
  suppress false missing index reports.
- Fall back to column sequences when `INDEX_NAME` or index metadata is unavailable.
- Add unit coverage for expression index + SYS_NC constraint matching.

## Impact
- Affected specs: `compare-objects`.
- Affected code: `schema_diff_reconciler.py` (OB constraint loader, index compare logic).
- Tests: `test_schema_diff_reconciler.py`.
- No new configuration switches.
