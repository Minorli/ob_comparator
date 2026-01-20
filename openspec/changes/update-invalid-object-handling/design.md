## Context
Recent audit reports identified multiple mismatches between expected migration behavior and current reporting/fixup logic:
- Triggers tied to blacklisted tables are reported as INVALID in status reports even though the base table is intentionally not migrated.
- INVALID VIEW/TRIGGER objects in the source can still generate fixup DDL, which then fails to compile in OB.
- INVALID objects are not used to block dependent objects in support classification.
- PACKAGE/PACKAGE BODY fixup ordering is not dependency-aware, which can create avoidable compile failures.
- SYNONYM objects referencing INVALID targets are not flagged as blocked.

## Goals / Non-Goals
- Goals:
  - Prevent false-positive trigger status discrepancies for blacklisted/unsupported tables.
  - Prevent generation of fixup DDL for INVALID VIEW/TRIGGER objects.
  - Propagate INVALID source objects into blocked dependency classification and synonym support status.
  - Add dependency-aware ordering for PACKAGE and PACKAGE BODY fixups.
- Non-Goals:
  - Do not change existing blacklist rules or object-type scopes.
  - Do not alter OB execution behavior or auto-compile invalid objects.
  - Do not introduce new configuration flags in this change.

## Decisions
- Decision: Expand Oracle object status collection to include VIEW/TRIGGER/PLSQL types required for invalid handling.
  - Rationale: invalid handling requires reliable status signals for missing objects; cost is minimal compared to existing DBA_OBJECTS reads.
- Decision: Build an INVALID source object set and add it to support classification as blocked/unsupported nodes.
  - Rationale: keeps the missing-object classification consistent with dependency-aware reporting.
- Decision: Mark SYNONYM as BLOCKED when its resolved target is INVALID (TABLE/VIEW/PLSQL) using source_objects/type hints.
  - Rationale: user-visible dependencies should show actionable root causes.
- Decision: Filter trigger status report rows when the trigger’s base table is blacklisted/unsupported.
  - Rationale: the trigger invalidity is expected and not actionable.
- Decision: Topologically order PACKAGE specs and bodies separately; bodies always follow specs; cycles fall back to deterministic name order with warnings.
  - Rationale: reduces compile failures while keeping deterministic output on cycles.

## Risks / Trade-offs
- Additional DBA_OBJECTS filtering for VIEW/PLSQL may add minor overhead; mitigated by restricting to configured schemas/types.
- Dependency graph may be incomplete for some packages; fallback ordering must remain deterministic.
- Marking INVALID objects as blocked may reduce fixup output counts; report messaging must clarify why items are skipped.

## Migration Plan
1. Extend Oracle status query scopes and store INVALID status for required types.
2. Add invalid-object propagation to support classification + synonym checks.
3. Filter trigger status report for blacklisted/unsupported tables.
4. Introduce package dependency ordering and cycle warnings.
5. Update fixup to skip invalid view/trigger DDL generation.
6. Add unit/integration tests for each scenario.

## Open Questions
- Which object types should be considered INVALID for dependency blocking beyond VIEW/TRIGGER (e.g., PROCEDURE/FUNCTION/TYPE)?
- Should invalid objects be marked as UNSUPPORTED vs BLOCKED in reports (and which reason_code)?
- Should invalid view/trigger skips be surfaced in fixup_skip_summary for visibility?
