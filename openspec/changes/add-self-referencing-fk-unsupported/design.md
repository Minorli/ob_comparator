## Context
OceanBase Oracle mode fails to create self-referencing foreign keys (FKs referencing the same table). Oracle allows such FKs. This mismatch should be caught as an unsupported constraint to avoid invalid fixup DDL and confusing missing reports.

## Goals / Non-Goals
- Goals:
  - Detect self-referencing FKs from Oracle metadata.
  - Report them as unsupported with a clear reason code and detail.
  - Exclude them from fixup scripts.
- Non-Goals:
  - No new runtime switch in this iteration.
  - No changes to other constraint comparison rules.

## Detection Strategy
1. For each source FK constraint (constraint_type = 'R'):
   - Resolve referenced constraint via `r_owner` + `r_constraint_name`.
   - Obtain referenced table owner/table name.
2. If referenced owner/table equals the FK owner/table → mark as self-referencing.
3. Classification:
   - `support_state=UNSUPPORTED`
   - `reason_code=FK_SELF_REF`
   - `reason=自引用外键，OB 不支持`
   - `detail=<owner.table>` or include constraint name

## Reporting
- `constraints_unsupported_detail_<ts>.txt` includes a row with `reason_code=FK_SELF_REF`.
- `unsupported_objects_detail_<ts>.txt` includes the constraint as unsupported.
- No fixup DDL generated for that constraint.

## Risks / Trade-offs
- False positives if referenced table resolution fails. Mitigation: only mark unsupported when the referenced table is resolved and equals the FK table.
- If OB later supports self-referencing FK, a new switch can relax the rule.

## Migration Plan
- Add detection and classification in constraint comparison phase.
- Add unit tests for constraint resolution and classification.

## Open Questions
- None for this iteration.
