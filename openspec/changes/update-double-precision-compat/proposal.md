# Change: Normalize DOUBLE PRECISION for OceanBase compatibility

## Why
OceanBase (Oracle mode) rejects `DOUBLE PRECISION` syntax in DDL, which causes fixup scripts generated from Oracle metadata to fail even though an equivalent `BINARY_DOUBLE` type is supported. We need a deterministic normalization so fixup succeeds and comparisons remain accurate.

## What Changes
- Normalize `DOUBLE PRECISION` to `BINARY_DOUBLE` during DDL cleanup for fixup generation.
- Treat `DOUBLE PRECISION` as equivalent to `BINARY_DOUBLE` when comparing column types (defensive normalization in case metadata emits the alias).
- Add unit tests and a compatibility-suite case to prove the behavior on Oracle 19c and OceanBase 4.2.5.7.

## Impact
- Affected specs: `compare-objects`, `generate-fixup`.
- Affected code: `schema_diff_reconciler.py` (DDL cleanup + type normalization), tests (`test_schema_diff_reconciler.py`, compatibility suite).
- No new switches; behavior is a compatibility fix in the existing cleanup pipeline.
