## Context
Large Oracle->OceanBase migrations often carry temporary target-side data inconsistency during cutover windows. Creating missing FK/CHECK constraints with VALIDATE can fail with ORA-02298 and block fixup progression.

Runtime evidence (Oracle + OceanBase):
- `ADD CONSTRAINT ... ENABLE VALIDATE` fails on dirty child rows (ORA-02298)
- `ADD CONSTRAINT ... ENABLE NOVALIDATE` succeeds
- New violating DML is still rejected (ORA-02291)
- After data cleanup, `ENABLE VALIDATE CONSTRAINT` succeeds

## Goals / Non-Goals
- Goals:
  - Make missing-constraint fixup robust by default.
  - Keep an explicit path to full validation after cleanup.
  - Align generation and execution semantics between main reconciler and run_fixup.
- Non-Goals:
  - No change to object compare semantics.
  - No automatic data repair for violating rows.

## Decisions
- Decision 1: Add `constraint_missing_fixup_validate_mode` with values:
  - `safe_novalidate` (default): missing constraints created with NOVALIDATE where applicable.
  - `source`: follow source `VALIDATED` state when metadata exists; fallback to safe_novalidate when unavailable.
  - `force_validate`: always emit VALIDATE behavior.
- Decision 2: Emit deferred promotion scripts in `fixup_scripts/constraint_validate_later` for constraints created/degraded as NOVALIDATE.
- Decision 3: run_fixup classifies ORA-02298 as data-quality/validate failure and does not blindly retry.
- Decision 4: export concise deferred-validation reports so users can close the loop explicitly.

## Risks / Trade-offs
- Trade-off: defaulting to NOVALIDATE can postpone optimizer/metadata benefits from VALIDATED constraints.
- Mitigation: provide explicit deferred validation scripts and report counts/details for operational closure.
- Risk: behavior drift if source VALIDATED metadata is unavailable.
- Mitigation: deterministic fallback to `safe_novalidate` with report annotation.

## Migration Plan
1. Add config parsing/default/validation/wizard/template/doc for the new switch.
2. Update constraint DDL generation path for missing constraints.
3. Emit `constraint_validate_later` scripts and report entries.
4. Update run_fixup error classification + summaries for ORA-02298.
5. Add/adjust tests for generation mode, fallback, and execution classification.
6. Run compile/unit tests, then Oracle/OB integration checks for validate behavior.

## Open Questions
- Whether to apply NOVALIDATE default to PK/UK/CHECK equally, or FK-first. Initial implementation keeps behavior configurable and explicit per generated statement path.
