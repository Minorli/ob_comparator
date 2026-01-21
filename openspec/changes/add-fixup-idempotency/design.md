## Context
Fixup scripts are executed repeatedly during migration dry runs. Re-running CREATE statements in OceanBase Oracle mode often fails when objects already exist. Some object types support CREATE OR REPLACE, while others do not.

## Goals / Non-Goals
- Goals: safe re-run of fixup scripts, reduced manual cleanup, opt-in drop/create for non-replaceable objects.
- Non-Goals: automatic data migration, destructive drops by default, full transactional DDL safety.

## Decisions
- Introduce fixup_idempotent_mode with default off to preserve current behavior.
- Modes:
  - off: emit current DDL unchanged.
  - replace: use CREATE OR REPLACE for supported types only; others are emitted unchanged.
  - guard: wrap non-replaceable CREATE statements in PL/SQL blocks that check existence and skip when present.
  - drop_create: optionally emit DROP (guarded) followed by CREATE for allowed types.
- Introduce fixup_idempotent_types to scope which object types are wrapped. When empty, apply to a safe default set (VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER, TYPE, TYPE BODY, SYNONYM).
- Guard checks will use USER_OBJECTS/USER_TABLES/USER_SEQUENCES and the remapped target schema.
- If metadata required for existence checks is unavailable, fall back to raw DDL and log a warning.

## Risks / Trade-offs
- Guard blocks increase runtime and require PL/SQL execution privileges.
- Drop/create is destructive and must be explicitly enabled by users.
- Statement splitting must treat PL/SQL blocks as a single statement to avoid execution errors.

## Migration Plan
- Ship with fixup_idempotent_mode=off by default.
- Update config documentation and examples.
- Add tests covering guard/replace output and statement splitting.

## Open Questions
- Should CREATE TABLE IF NOT EXISTS be used when OceanBase supports it?
- Which object types should be included in the default fixup_idempotent_types set?
