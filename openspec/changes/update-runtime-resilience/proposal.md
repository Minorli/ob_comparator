# Change: Improve runtime resilience for metadata and DDL extraction

## Why
Large migration runs often encounter transient obclient/dbcat/Oracle metadata failures. Today a single failure can abort the entire run or leave missing DDL with little diagnostic context. We need controlled retries, per-object fallback, and clearer failure reporting to reduce manual intervention.

## What Changes
- Add retry/backoff controls for obclient metadata queries and Oracle DBMS_METADATA batch fetches (default one retry).
- Introduce dbcat failure policies (abort|fallback|continue) with per-object fallback when possible (default fallback).
- Record and export structured failure summaries for metadata/DDL extraction.
- Add integration/E2E test harness (opt-in) to validate end-to-end resiliency paths.

## Impact
- Affected specs: configuration-control, compare-objects, generate-fixup, export-reports
- Affected code: schema_diff_reconciler.py (metadata, dbcat, DDL fetch, reporting), tests
- Tests: new unit tests for retry/fallback logic and opt-in integration tests
