## Context
Grant generation currently emits one GRANT per privilege entry from `DBA_TAB_PRIVS`, and loads grants by both OWNER and GRANTEE. Large environments (400k–500k grants) spend significant time in grant planning and produce extremely large GRANT script counts, leading to multi-day execution on OceanBase.

## Goals / Non-Goals
- Goals:
  - Reduce grant planning time and memory pressure.
  - Reduce GRANT statement count without changing semantics.
  - Provide visible progress during grant planning.
- Non-Goals:
  - Changing privilege semantics or removing grants beyond configured scope.
  - Altering dependency-driven grant logic.

## Decisions
- **Grant scope**: introduce `grant_tab_privs_scope` to control DBA_TAB_PRIVS filtering. Default to `owner` (objects in source schemas), optional `owner_or_grantee` for legacy behavior.
- **Merging**: allow `grant_merge_privileges` and `grant_merge_grantees` to compact GRANT statements.
- **Caching**: cache remap target resolution for object privileges to avoid repeated inference.
- **Progress**: reuse `progress_log_interval` to emit progress logs for grant planning steps.

## Risks / Trade-offs
- Merging grantees assumes OceanBase supports `GRANT ... TO user1, user2`, which has been confirmed by the user.
- Restricting scope to OWNER may omit grants on external objects; the legacy option remains available.

## Migration Plan
1. Add new config options with safe defaults.
2. Update grant extraction to respect scope.
3. Introduce merge and cache logic.
4. Update documentation and changelog.

## Open Questions
- None.
