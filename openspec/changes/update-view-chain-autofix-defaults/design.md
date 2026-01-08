## Context
View-chain autofix currently rebuilds plans for every chain entry on each run. This can reprocess already-created views, and blocked cases (missing DDL or missing GRANT statements) require manual intervention. Execution summaries can also under-report successful creation when a later statement fails.

## Goals / Non-Goals
- Goals:
  - Skip existing views by default to reduce rework
  - Resolve common BLOCKED cases without manual steps
  - Provide accurate per-view outcome reporting
- Non-Goals:
  - Force-create views to break dependency cycles
  - Auto-fix SQL syntax issues in DDL (handled separately)

## Decisions
- Decision: Pre-check root view existence and mark SKIPPED when present; still emit plan/SQL for audit.
- Decision: When DDL is missing, search fixup_scripts/done for prior scripts before blocking.
- Decision: When grants_miss/grants_all lack a match, generate an object GRANT using the required privilege by type.
- Decision: Classify outcomes into SUCCESS/PARTIAL/FAILED/BLOCKED/SKIPPED and emit failure summaries.

## Risks / Trade-offs
- Auto-generated GRANTs assume object-level privileges are acceptable; system-level grants are not auto-generated.
- Skipping existing views may leave unmet GRANTs unaddressed; the plan output remains for manual review.

## Migration Plan
No migration required. Behavior changes apply when --view-chain-autofix is used.
