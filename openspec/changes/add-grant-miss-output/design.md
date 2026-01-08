## Context
Grant generation currently produces a full set of GRANT statements derived from Oracle metadata and remap rules. OceanBase is not used to compute a delta, so run_fixup must re-run all grants.

## Goals / Non-Goals
- Goals:
  - Compute missing grants by comparing against OceanBase privileges.
  - Output both full and missing-only grant scripts.
  - Default run_fixup to execute missing-only grants.
- Non-Goals:
  - Do not change non-grant fixup generation.
  - Do not alter grant semantics or merging logic.

## Decisions
- Decision: Compare against OceanBase full catalog (DBA_TAB_PRIVS / DBA_SYS_PRIVS / DBA_ROLE_PRIVS).
  - Rationale: User requires full-database diff for accuracy.
- Decision: Output both grants_all and grants_miss.
  - Rationale: Preserve full output for audit while enabling faster execution.
- Decision: Default run_fixup to grants_miss when both exist.
  - Rationale: Safe and fast default.

## Algorithm
1) Generate full grant plan from Oracle metadata + remap as today.
2) Query OceanBase DBA_TAB_PRIVS, DBA_SYS_PRIVS, DBA_ROLE_PRIVS.
3) Normalize and remap OB privileges to match target naming and compare with expected grants.
4) Emit:
   - fixup_scripts/grants_all/*.sql (full expected)
   - fixup_scripts/grants_miss/*.sql (missing-only)
5) run_fixup selects grants_miss when present; grants_all is still available via --only-dirs.

## Risks / Trade-offs
- Privilege normalization must align Oracle/OB naming; mismatches can lead to false positives.
- Reading full OB privilege catalogs may be heavy; ensure batching and reasonable timeouts.

## Open Questions
- Should missing grants include PUBLIC grants or handle them separately?
