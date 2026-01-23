## Context
The audit identified multiple correctness issues that cross-cut fixup generation, execution ordering, and metadata comparison. These issues are intertwined with dependency graphs, output safety, and OceanBase metadata gaps.

## Goals / Non-Goals
- Goals:
  - Deterministic and safe cleanup for fixup outputs.
  - Expand PL/SQL cleanup coverage without changing semantic content.
  - Improve dependency ordering for PL/SQL objects using existing dependency graphs.
  - Avoid false CHECK constraint mismatches when OB lacks deferrable metadata.
  - Make dependency-grant status visible for non-VIEW objects.
- Non-Goals:
  - Password transport redesign.
  - Codebase refactor into modules.
  - Column order comparison (report-only) unless requested.

## Decisions
- Fixup cleanup:
  - Always attempt cleanup before checking master_list.
  - Keep safety guard for absolute paths; add fixup_force_clean to override guard explicitly.
  - Log and continue on per-file deletion errors.
- PL/SQL cleanup:
  - Add a targeted regex to correct FIRST/LAST/COUNT single-dot ranges only when a double-dot is clearly intended.
  - Do not modify generic a.b cases to avoid breaking valid attribute references.
- Dependency ordering:
  - Use dependency pairs to topo-sort TYPE/TYPE BODY and PROCEDURE/FUNCTION/TRIGGER within fixup generation.
  - Preserve stable ordering when cycles or missing edges are detected.
  - Update run_fixup dependency layers to ensure TYPE precedes PROCEDURE/FUNCTION in smart order.
- Constraint metadata:
  - Query DEFERRABLE/DEFERRED from OB when present; otherwise treat as unknown and avoid mismatch solely due to missing fields.
- Grant linkage:
  - Extend dependency grant checks to non-VIEW nodes and distinguish GRANT_UNKNOWN for unmapped privilege types to avoid false negatives.

## Risks / Trade-offs
- Aggressive cleanup could remove scripts outside the project; the safety guard and explicit fixup_force_clean mitigate this.
- Expanded PL/SQL cleanup may still miss ambiguous single-dot ranges; false positives are mitigated by restricting to FIRST/LAST/COUNT.
- Topo-sorting across additional object types could surface cycles; fallback to stable ordering is required.

## Migration Plan
1. Add config switch and update templates/documentation.
2. Implement cleanup and PL/SQL cleanup rules.
3. Extend ordering logic and dependency-grant evaluation.
4. Update tests and validate with P0/P1 cases.

## Open Questions
- Should INDEX/JOB/SCHEDULE/TRIGGER be treated as GRANT_NA or mapped to a best-effort privilege for dependency checks?
- Do we want to expose column order reporting as a gated feature in a later change?
