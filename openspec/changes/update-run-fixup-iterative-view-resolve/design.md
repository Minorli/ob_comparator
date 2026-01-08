## Context
run_fixup currently executes SQL files as whole units and retries by re-running full grant files. VIEW failures (missing objects or insufficient privileges) cause repeated full GRANT passes and timeouts.

## Goals / Non-Goals
- Goals:
  - Resolve VIEW failures in --iterative runs by creating missing dependencies when a fixup script exists.
  - Apply only required GRANT statements for a failing VIEW.
  - Continue executing remaining statements after a statement-level failure.
  - Support long-running or unlimited timeouts for run_fixup execution.
- Non-Goals:
  - Do not change schema_diff_reconciler.py or fixup generation.
  - Do not attempt to create objects that are not present in fixup_scripts.
  - Do not introduce a full SQL parser; use lightweight parsing and error-based hints.

## Decisions
- Decision: Enable dependency-aware VIEW resolution only in --iterative mode.
  - Rationale: The behavior adds extra discovery work and retries; iterative mode is already intended for retry loops.
- Decision: Use error parsing as the primary signal for missing objects and insufficient privileges.
  - Rationale: Start with the smallest viable implementation; if the error does not surface an object name, log and skip.
- Decision: Build an in-memory index of fixup scripts and GRANT statements.
  - Rationale: Avoid re-reading grant files and avoid full GRANT replays.
- Decision: Execute SQL files statement-by-statement with per-statement error handling.
  - Rationale: Prevent single-statement failures from skipping remaining statements and allow partial progress.

## Algorithm
1) Pre-index fixup scripts by object key (TYPE, SCHEMA.OBJECT) from fixup_scripts subdirectories.
2) Pre-index GRANT statements from fixup_scripts/grants by (GRANTEE, OBJECT) and by OBJECT only.
3) For each VIEW file in the iterative queue:
   - Execute the VIEW statement(s).
   - On "table or view does not exist":
     - Parse the error message for the missing object name.
     - If a corresponding fixup script exists, execute it and retry the VIEW; otherwise log and continue.
   - On "insufficient privileges":
     - Parse the error message for object/privilege hints if present.
     - Use the VIEW owner as grantee and match dependencies to GRANT index.
     - Execute matching GRANT statements individually, then retry the VIEW.
4) Stop retrying when:
   - The VIEW succeeds,
   - The maximum iteration rounds are reached, or
   - A full pass makes no progress (no new dependencies created, no new grants applied).

## Error Parsing Rules (initial set)
- Missing object: Oracle-style ORA-00942, OceanBase "table or view does not exist".
- Privilege: ORA-01031 or OceanBase equivalent "insufficient privileges".

## Timeout Handling
- New setting fixup_cli_timeout (seconds). 0 disables timeout. Default is long-running (e.g., 3600 seconds).
- run_fixup uses this timeout for obclient execution; main program remains unchanged.

## Risks / Trade-offs
- Heuristic dependency parsing may miss complex references; fallback to error parsing mitigates.
- Statement-level execution may allow partial changes; failures are logged and the file stays in place.

## Open Questions
- Should failure summaries be written to a dedicated report file, or remain in the log only?
- Should grant matching consider grantees other than the VIEW owner?
