## Context
Large-scale migration runs suffer from transient network hiccups, dbcat timeouts, and sporadic metadata failures. Current behavior can abort the entire run or produce silent gaps in fixup output. We need resilience without changing core comparison semantics.

## Goals / Non-Goals
- Goals:
  - Make metadata and DDL extraction resilient to transient failures.
  - Provide deterministic fallback behavior with clear reporting.
  - Add opt-in integration tests to validate end-to-end flows.
- Non-Goals:
  - Changing remap logic or object comparison rules.
  - Altering fixup execution semantics in run_fixup.py (beyond reporting).
  - Introducing new external dependencies.

## Decisions
- Add retry/backoff controls for obclient metadata queries:
  - `obclient_retry_count` (default 1, retries after the initial attempt), `obclient_retry_backoff_sec` (default 1), `obclient_retry_max_backoff_sec` (default 5), `obclient_retry_jitter_ms` (default 200).
  - Retry only for transient error patterns (timeout, connection reset/refused, network errors). Non-transient errors fail fast.
- Add dbcat failure policies:
  - `dbcat_failure_policy`: `abort` (current behavior), `fallback` (try DBMS_METADATA per object), `continue` (skip and report). Default is `fallback`.
  - `dbcat_retry_limit`: number of chunk retries before policy enforcement (default 1).
  - Fallback applies only to types supported by DBMS_METADATA; unsupported types are recorded as failed.
- Add per-object fallback for DBMS_METADATA batch fetch:
  - `oracle_ddl_batch_retry_limit` for batch execution (default 1).
  - `oracle_ddl_single_retry_limit` for per-object retry (default 1).
  - Failed objects are recorded in the failure report with action=SKIP or FALLBACK.
- Failure reporting:
  - Export `main_reports/ddl_fetch_failures_<timestamp>.txt` with `|` delimiter and header.
  - Add summary counts to main report (dbcat failures, fallback success, final skips).
- Testing:
  - Unit tests with mocked subprocess/oracledb to verify retry/backoff and policy routing.
  - Integration tests (opt-in via `RUN_INTEGRATION_TESTS=1`) covering obclient metadata query, dbcat fallback behavior, and end-to-end fixup generation on a small schema.

## Risks / Trade-offs
- Retries can increase total runtime; defaults are conservative and configurable.
- Fallback to DBMS_METADATA can add load to Oracle; enforce per-object fallback and reporting to keep visibility.
- Error pattern classification may miss edge cases; include raw stderr in failure reports to refine over time.

## Migration Plan
- Ship new settings with safe defaults and document recommended production values.
- Maintain current behavior by setting `dbcat_failure_policy=abort` and `obclient_retry_count=0`.
- Update docs and config templates together with code changes.

## Open Questions
- Do we need a global cap on total fallback objects per run to protect source DB?
