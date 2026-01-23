# Change: Improve CHECK constraint compatibility handling and reporting

## Why
CHECK constraint comparison and fixup are incomplete, and OceanBase rejects some Oracle-accepted CHECK patterns. Missing compatibility classification causes confusing reports and fixup failures.

## What Changes
- Expand constraint comparison to include CHECK constraints with extracted expressions.
- Detect OceanBase-incompatible CHECK constraints (e.g., SYS_CONTEXT usage, DEFERRABLE INITIALLY DEFERRED) and classify them as UNSUPPORTED.
- Export unsupported CHECK constraints to a dedicated report in the per-run report directory.
- Exclude unsupported CHECK constraints from fixup generation while still reporting them as unsupported.

## Impact
- Affected specs: compare-objects, export-reports, generate-fixup
- Affected code: schema_diff_reconciler.py (constraint metadata extraction, comparison, reporting, fixup filtering)
- Tests: add unit tests for expression extraction and compatibility classification
