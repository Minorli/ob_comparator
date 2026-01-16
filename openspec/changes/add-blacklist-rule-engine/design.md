## Context
Today the tool only reads `OMS_USER.TMP_BLACK_TABLE`. If that table does not exist or is stale, unsupported tables are not filtered and the missing-table output becomes noisy. The blacklist logic itself is useful, but the data source should be generated automatically and be easy to update as OceanBase compatibility evolves.

## Goals / Non-Goals
- Goals:
  - Remove the operational dependency on manually creating `TMP_BLACK_TABLE`.
  - Keep blacklist logic data-driven and version-aware.
  - Preserve current blacklist behavior and reporting format as much as possible.
  - Provide deterministic, deduplicated results scoped to configured source schemas.
- Non-Goals:
  - Redesign the overall compare pipeline or remap rules.
  - Add new object-type compatibility checks outside the blacklist scope.
  - Require new external dependencies beyond the standard library.

## Decisions
- Decision: Introduce a blacklist rule engine with a JSON rules file.
  - Each rule defines `id`, `black_type`, `sql`, and optional `params`, `min_ob_version`, `max_ob_version`.
  - SQL templates are executed against Oracle metadata views with owner filters.
  - Default rule set lives in the repo and can be overridden via config.
- Decision: Add `blacklist_mode` to control the data source.
  - `auto` (default): merge rule results with `TMP_BLACK_TABLE` if present.
  - `table_only`: use `TMP_BLACK_TABLE` only.
  - `rules_only`: ignore `TMP_BLACK_TABLE` even if present.
  - `disabled`: skip blacklist entirely.
- Decision: Keep the output format stable.
  - Continue using `blacklist_tables.txt` with `TABLE, BLACK_TYPE, DATA_TYPE, STATUS, DETAIL`.
  - Include the rule source in `DETAIL` (e.g., `RULE=LOB_OVERSIZE`) to avoid breaking consumers.
- Decision: Rule evaluation is owner-scoped and chunked.
  - Use the existing owner chunking logic to prevent overly large IN clauses.
  - Skip rules that require unavailable views and log a warning.
- Decision: Provide configurable rule parameters.
  - Example: `blacklist_lob_max_mb` used in the LOB size rule.
  - Rule enable/disable lists allow fast overrides without editing SQL.

## Rule Coverage (Initial)
- DIY: user-defined types (non-SYS) referenced by table columns.
- SPE: unsupported column data types (ROWID, BFILE, XMLTYPE, UROWID, UNDEFINED, UDT, ANYDATA).
- LONG: LONG and LONG RAW columns.
- LOB_OVERSIZE: LOB segment size greater than `blacklist_lob_max_mb` (default 512MB).
- TEMPORARY: global temporary tables.
- DBLINK: external tables.
- IOT: IOT tables.

## Risks / Trade-offs
- Performance: LOB size checks can be expensive. Mitigation: make it optional and parameterized, and query only for selected owners.
- Permissions: Some Oracle metadata views may be inaccessible. Mitigation: log and skip failing rules without aborting the run.
- Version drift: Rules may become outdated as OceanBase evolves. Mitigation: rule file supports version gating and local overrides.

## Migration Plan
- Default `blacklist_mode=auto` to preserve existing behavior when `TMP_BLACK_TABLE` exists.
- Ship a default rules file that mirrors current manual SQL logic.
- Document new config options and how to override rules.

## Open Questions
- Should rule SQL support `TMP_DBA_*` sources when DBA views are unavailable?
- Should `blacklist_tables.txt` add explicit `SOURCE` column in a future revision?
- Do we need per-rule timeout limits or a global blacklist query timeout?
