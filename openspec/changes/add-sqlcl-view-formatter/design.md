## Context
The comparator already cleans and remaps Oracle DDL to OceanBase-compatible SQL, but the output is often hard to read and fragile when line comments collapse or long SQL is flattened. SQLcl embeds the SQL Developer formatter and can normalize DDL for multiple object types. Formatting must remain optional and must not affect fixup logic or object selection.

## Goals / Non-Goals
- Goals:
  - Provide an opt-in SQLcl formatter stage for multiple object types.
  - Keep fixup generation logic unchanged; formatting is output-only.
  - Allow users to choose which object types to format.
  - Preserve safety: formatting failure falls back to original DDL.
- Non-Goals:
  - Replace DBMS_METADATA/dbcat DDL sources.
  - Use formatting to repair invalid DDL logic (beyond whitespace/layout).
  - Make SQLcl a required dependency.

## Decisions
- Decision: Add a global formatter toggle + type list.
  - Config: `ddl_format_enable` (bool) + `ddl_format_types` (list) + `ddl_formatter` (sqlcl/none).
  - Rationale: Users may want final formatting only for select object types.
- Decision: Apply formatting **after** all cleanup/remap steps.
  - Rationale: Formatting is a final polish; it must not affect fixup logic.
- Decision: Best-effort behavior by default.
  - Rationale: Formatting errors should never block fixup outputs.
- Decision: For PL/SQL DDL, strip trailing `/` before formatting and re-append afterward if originally present.
  - Rationale: SQLcl `FORMAT FILE` fails on trailing `/` delimiters.
- Decision: Keep view comment repair inside the fixup pipeline (existing sanitize), not in the formatter.
  - Rationale: Fixup logic must produce valid DDL even when formatting is disabled.
- Decision: Use batch formatting to amortize SQLcl startup cost, with size-based skips for huge DDL.
  - Config: `ddl_format_batch_size`, `ddl_format_max_lines`, `ddl_format_max_bytes`, `ddl_format_timeout`.
  - Rationale: SQLcl startup is expensive; formatting 10k+ objects or 50k-line PL/SQL must be gated.

## CLI Notes (validated)
- SQLcl root: `/home/minorli/sqlcl` (resolve `bin/sql`).
- Command used: `sql -S /nolog` with `FORMAT FILE <input> <output>`.
- Use `JAVA_TOOL_OPTIONS=-Duser.home=<tmp_dir>` to suppress history warnings.

## SQLcl Formatting Coverage (local tests)
Verified with SQLcl 25.4.0 (offline format only):
- OK (no `/` delimiter): TABLE, INDEX, SEQUENCE, SYNONYM, VIEW, MATERIALIZED VIEW,
  ALTER TABLE (add column), ALTER TABLE ADD CONSTRAINT,
  PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER, TYPE, TYPE BODY.
- FAIL (parse error) when PL/SQL DDL includes trailing `/`.
- JOB/SCHEDULE blocks are **best-effort**: a simple DBMS_SCHEDULER block formats;
  some complex `job_action` strings may still fail; fallback applies.

## Recommended Defaults
- `ddl_format_enable = false` (opt-in).
- `ddl_format_types = VIEW` (safe default; expand as needed).
- Recommended “safe” list when desired:
  TABLE, VIEW, MATERIALIZED VIEW, INDEX, SEQUENCE, SYNONYM,
  PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER, TYPE, TYPE BODY,
  CONSTRAINT, TABLE_ALTER.
- Treat JOB/SCHEDULE as optional “best-effort” types.
- `ddl_format_batch_size = 200` (reduce SQLcl startups).
- `ddl_format_max_lines = 30000`, `ddl_format_max_bytes = 2_000_000` (skip huge DDL by default).
- `ddl_format_timeout = 60` seconds per batch (override as needed).

## Alternatives Considered
- sqlfluff/sqlglot/sqlparse: weaker Oracle DDL coverage and heavier dependencies.
- Custom formatter: too costly and incomplete.

## Risks / Trade-offs
- External dependency (SQLcl + Java) may not exist on all hosts.
  - Mitigation: opt-in config and explicit validation; fallback on failure.
- Formatting can alter case/whitespace.
  - Mitigation: formatting is optional; fallback keeps original DDL.
- Performance overhead when formatting many objects.
  - Mitigation: enable only for selected types; apply size limits, batching, and timeouts.

## Migration Plan
1. Add config keys for global formatter + type list.
2. Implement SQLcl wrapper with timeout and slash handling.
3. Apply formatting only at output stage (no impact on fixup logic).
4. Add formatter report and tests.

## Open Questions
- Confirm if a SQL Developer formatting profile will be provided later.
- Decide whether to add an optional “formatted output in parallel directory” mode.
