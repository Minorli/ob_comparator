# Change: Normalize OceanBase GTT internal index artifacts

## Why
OceanBase global temporary tables (GTT) may prepend `SYS_SESSION_ID` to business indexes and create internal indexes like `IDX_FOR_HEAP_GTT_*`. Current index comparison can treat these engine artifacts as missing/extra differences, creating migration noise.

## What Changes
- Normalize target index column sequences for OB GTT tables by ignoring leading `SYS_SESSION_ID`.
- Exclude internal OB GTT helper indexes (`IDX_FOR_HEAP_GTT_*`) from index mismatch detection.
- Keep non-GTT tables unchanged to avoid masking real differences.
- Add unit tests for positive and negative paths.

## Impact
- Affected specs: `compare-objects`
- Affected code: `schema_diff_reconciler.py`, `test_schema_diff_reconciler.py`
