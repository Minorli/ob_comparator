# Change: Improve view dependency remap and sequence/column feature comparison

## Why
Current fixup generation misses PUBLIC synonym rewrites in VIEW DDL and dependency extraction fails for subquery-based views, leading to invalid or unremapped DDL. Sequence comparison only checks existence and table comparison omits IDENTITY/DEFAULT ON NULL feature detection, which hides migration risks.

## What Changes
- Resolve PUBLIC synonym references during VIEW DDL rewrite so remap targets use base objects.
- Add dependency fallback for VIEW rewrite using Oracle dependency metadata when SQL extraction is incomplete (subqueries/CTE).
- Capture and compare SEQUENCE attributes (increment/min/max/cycle/order/cache) rather than existence only.
- Detect IDENTITY and DEFAULT ON NULL column features in source metadata and surface mismatches in table comparison reports.

## Impact
- Affected specs: compare-objects, generate-fixup
- Affected code: schema_diff_reconciler.py (metadata load, view rewrite, sequence compare, table compare)
- Tests: update existing unit tests and add cases for PUBLIC synonym view rewrite, subquery dependency extraction, sequence attr mismatch, and identity/default-on-null mismatch
