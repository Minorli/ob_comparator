## Context
We now generate VIEWs_chain reports that include object type, existence, and grant status per hop. However, fixing VIEWs still requires manual inspection and incremental creation/granting. The run_fixup executor can use these chains plus live OceanBase checks to automate the same workflow precisely.

## Goals / Non-Goals
- Goals:
  - Use the latest VIEWs_chain report to drive per-view repairs.
  - Generate per-view plan and SQL files before executing.
  - Execute only required DDL and GRANT statements (no full grant files).
  - Use fixup directory DDL only; no new DDL generation.
- Non-Goals:
  - Do not re-generate fixup scripts or modify the main comparator.
  - Do not execute full grants_miss/grants_all files.
  - Do not build a full dependency graph UI.

## Decisions
- Decision: Use the latest VIEWs_chain_*.txt in report_dir (by mtime).
  - Rationale: matches user intent without extra config.
- Decision: New CLI flag --view-chain-autofix enables this mode.
  - Rationale: avoid changing default run_fixup behavior.
- Decision: Output per-view files under fixup_scripts/view_chain_plans/ and fixup_scripts/view_chain_sql/.
  - Rationale: keep artifacts alongside fixup scripts for manual edits.
- Decision: Grants are selected from grants_miss first, then grants_all, for exact statement matches only.
  - Rationale: precise and fast; avoids full grant runs.

## Algorithm
1) Locate latest VIEWs_chain file in report_dir.
2) Parse each chain line into nodes: OWNER.OBJ + TYPE (ignore EXISTS/GRANT annotations).
3) Group chains by root view (first node). Merge nodes into a depth-ordered plan from leaf -> root.
4) Build fixup DDL index from fixup_scripts and GRANT index from grants_miss/grants_all.
5) For each view:
   - Detect cycles; if found, mark view as blocked and skip auto-exec.
   - For each hop dep -> ref:
     - Query OB for existence (DBA_OBJECTS) and required privilege (DBA_TAB_PRIVS/DBA_SYS_PRIVS/DBA_ROLE_PRIVS + SYS privilege implications).
     - If ref missing: add its fixup DDL to plan.
     - If privilege missing: add matching GRANT statements from grants_miss or grants_all.
   - Add the view DDL last.
6) Write plan and SQL files, then execute the SQL in order.
7) Preserve plan/SQL files for manual edits; log per-view outcomes.

## Risks / Trade-offs
- Chain parsing depends on report formatting; changes require parser updates.
- Some dependencies may not have fixup DDL; those views will be blocked.
- Privilege checks require multiple OB queries; use caching/batching to keep runtime manageable.

## Open Questions
- Should there be a --view-chain-no-exec option to only generate plans?
