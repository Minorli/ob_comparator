# Change: Remap terminology, synonym policy, and retry controls

## Why
- Synonym remap inference can misclassify 1:1 mappings and remap synonyms across schemas.
- Mapping summary wording is ambiguous in logs/reports.
- obclient failures are handled uniformly, making transient errors noisy and hard to recover.
- Oracle DDL batch fallback retries are unbounded.

## What Changes
- Add a synonym remap policy with a safe default that preserves 1:1 schema mapping.
- Normalize mapping summary terminology in logs/reports (1:1, N:1, 1:N, fallback).
- Classify obclient failures into transient vs fatal and apply bounded retries (default 3 retries, 1000ms backoff).
- Add a retry limit for oracle_get_ddl_batch fallback to avoid indefinite attempts.

## Impact
- Affected specs: resolve-remap, export-reports, configuration-control
- Affected code: schema_diff_reconciler.py, run_fixup.py (obclient usage)
- Tests: unit tests for remap policy, terminology output, and retry behavior
