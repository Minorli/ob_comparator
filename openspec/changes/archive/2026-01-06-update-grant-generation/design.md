# Design: Grant Generation Pipeline (Remap + Dependencies)

## Goals
- Preserve Oracle's privilege intent (object, system, and role grants) while remapping objects/schemas.
- Add required cross-schema grants introduced by remap and deep dependency chains.
- Output grants as fixup SQL and inject object-level grants into per-object DDL.
- Avoid changing existing comparison logic; grant generation is additive and gated by config.

## Data Sources
- **DBA_TAB_PRIVS**: object grants (GRANTEE, OWNER, TABLE_NAME, PRIVILEGE, GRANTABLE, TYPE).
- **DBA_SYS_PRIVS**: system privileges (GRANTEE, PRIVILEGE, ADMIN_OPTION).
- **DBA_ROLE_PRIVS**: role grants (GRANTEE, GRANTED_ROLE, ADMIN_OPTION).
- **DBA_DEPENDENCIES**: dependency graph for grant augmentation, with remap applied.
- **DBA_SYNONYMS** metadata: enrich dependency graph edges (synonym → target object).
- **object_parent_map**: parent table references for triggers and other dependent objects.

## Scope & Gating
- Controlled by `generate_grants` (new config). When disabled, no grant generation or DDL injection occurs.
- Dependency loading is enabled for grant generation even if `check_dependencies=false`.

## Grantee Resolution
- Base grantees: configured `source_schemas` plus `PUBLIC`.
- Role closure: iteratively expand roles from DBA_ROLE_PRIVS (roles granted to schemas or roles), to capture nested roles.
- Grantee remap: if grantee is a schema present in schema mapping, remap to target schema; roles and PUBLIC are not remapped.

## Object Mapping for Privileges
- Use `resolve_remap_target` with the same remap inputs (explicit rules, inferred mapping, dependency graph, parent map).
- If mapping is unavailable for in-scope schemas, skip the grant and log a warning; external schemas fall back to `SRC_SCHEMA.OBJECT`.
- Respect `NO_INFER_SCHEMA_TYPES`: these remain in source schema unless explicit remap exists.

## Grant Sources and Merge
1. **Source grants** (from DBA_TAB_PRIVS):
   - Preserve original privilege and WITH GRANT OPTION (GRANTABLE=YES).
   - Remap object owner/schema and grantee schema.
   - Retain PUBLIC and ROLE grants.
2. **Dependency-derived grants**:
   - For each dependency edge (dep → ref) after remap, add required privilege based on ref type.
   - For TABLE→TABLE, also add REFERENCES.
   - For VIEW/MVIEW, compute transitive dependency chains and add grants to all referenced objects along the chain (cross-schema only).
3. **System/role grants** (DBA_SYS_PRIVS / DBA_ROLE_PRIVS):
   - Remap grantee schema where applicable; keep role name unchanged.
   - Include WITH ADMIN OPTION when present.

Merge logic deduplicates all grants by (grantee, privilege, object/system/role, grant option).

## Output
- **Central grants**: write `fixup_scripts/grants/*.sql` grouped by target object owner (object grants) and by grantee for system/role grants.
- **Per-object DDL injection**: append object-level GRANT statements for the object being created/altered.
- Do not emit system/role grants inside object DDL.

## Reporting
- Remove grant sections from the report; grant output is fixup-only.

## Risks & Mitigations
- Over-granting risk from deep dependency traversal: limit to cross-schema edges and log counts.
- Missing mappings: log skipped grants and proceed without aborting comparisons.
