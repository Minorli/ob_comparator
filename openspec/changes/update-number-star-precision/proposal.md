# Change: Treat Oracle NUMBER(*,0) as NUMBER(38,0)

## Why
Oracle exposes NUMBER(*,0) with an unspecified precision (data_precision is NULL, scale=0). OMS/OceanBase migrations often materialize this as NUMBER(38,0). The current comparison treats NULL vs 38 as a mismatch, producing false errors and incorrect fixup DDL.

## What Changes
- Recognize NUMBER(*,0) (precision NULL, scale=0) as compatible with NUMBER(38,0).
- Suppress mismatch reporting and fixup generation for this equivalence.

## Impact
- Affected specs: compare-objects
- Affected code: schema_diff_reconciler.py (number precision comparison and fixup selection)
- Tests: unit tests for NUMBER(*,0) equivalence and non-equivalent cases
