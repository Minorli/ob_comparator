## Context
The tool already exports generic dependency chains, but it does not provide a view-specific fixup chain with object type/owner/existence/privilege annotations. Investigations require a chain that mirrors the fixup target set and resolves synonym hops.

## Goals / Non-Goals
- Goals:
  - Generate VIEW chains only for views selected for fixup.
  - Annotate each hop with object type, owner, existence, and grant status.
  - Resolve SYNONYM hops to their referenced target types where possible.
  - Output a concise, line-based report for diagnostics.
- Non-Goals:
  - Do not change fixup execution logic.
  - Do not emit a full dependency graph UI.

## Decisions
- Decision: Use Oracle dependency metadata + remap to build chains, then expand synonym hops using cached synonym metadata and annotate using OceanBase metadata and missing-grant computation.
  - Rationale: Keeps chains consistent with fixup mapping.
- Decision: Output file under main_reports with timestamp, named VIEWs_chain_<timestamp>.txt.
  - Rationale: Align with existing report naming while matching requested name.

## Algorithm
1) Identify views that require fixup (missing in target or mismatched).
2) Build dependency chains for these views using the dependency graph (Oracle DBA_DEPENDENCIES remapped).
3) Expand SYNONYM hops using cached synonym metadata when available:
   - Resolve to referenced owner/object/type (TABLE/VIEW/SEQUENCE/PROCEDURE/etc.).
   - If resolution fails, keep SYNONYM as terminal with UNKNOWN target.
4) For each hop:
   - Record object type and owner.
   - Mark existence (EXISTS/MISSING) using OceanBase metadata.
   - Mark grant status (GRANT_OK/GRANT_MISSING) using the missing-grant plan.
5) Write chains to VIEWs_chain_<timestamp>.txt with one chain per line.

## Risks / Trade-offs
- Chains can be long; cap depth or detect cycles to prevent infinite traversal.
- Grant status requires consistent privilege normalization to avoid false positives.
- Synonym metadata gaps may produce UNKNOWN targets.

## Open Questions
- Should we include a per-view summary section with aggregated counts?
