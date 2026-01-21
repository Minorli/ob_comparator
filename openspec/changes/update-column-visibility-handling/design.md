## Context
Oracle exposes column visibility via INVISIBLE_COLUMN (user-defined invisibility) and HIDDEN_COLUMN (system/internal hidden columns). OceanBase Oracle mode may expose similar metadata but support and naming can vary by version.

## Goals / Non-Goals
- Goals: preserve explicit INVISIBLE columns, surface visibility mismatches, and generate safe fixup DDL.
- Non-Goals: handling internal system-generated hidden columns or rewriting virtual columns.

## Decisions
- Prefer INVISIBLE_COLUMN when available; only fall back to HIDDEN_COLUMN when explicitly configured and safe.
- Introduce column_visibility_policy with default auto:
  - auto: enforce only when visibility metadata is available and target indicates support.
  - enforce: always generate visibility fixups when source metadata indicates INVISIBLE.
  - ignore: skip visibility comparison and fixup.
- Compare visibility only for user columns (exclude OMS_* and system-hidden columns).
- For missing tables, preserve INVISIBLE in CREATE TABLE DDL when present; otherwise append ALTER TABLE MODIFY ... INVISIBLE statements after creation.
- For existing tables, generate ALTER TABLE MODIFY ... INVISIBLE/VISIBLE when a visibility mismatch is detected and policy permits.
- If OceanBase metadata does not expose visibility fields, log a warning and skip enforcement in auto mode.

## Risks / Trade-offs
- Visibility metadata may be unavailable in restricted environments, leading to partial enforcement.
- Some OceanBase versions may not fully support column visibility; fixups must degrade gracefully.
- Fallback to HIDDEN_COLUMN risks including system-generated hidden columns; require explicit opt-in if used.

## Migration Plan
- Default column_visibility_policy=auto to avoid breaking existing runs.
- Update documentation and config templates.
- Add unit tests and Oracle/OB integration checks for visibility metadata.

## Open Questions
- Should fallback to HIDDEN_COLUMN be opt-in (column_visibility_hidden_fallback=true)?
- Do we need a whitelist/blacklist for columns that should never be toggled to INVISIBLE?
