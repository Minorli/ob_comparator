## Context
The tool exports missing TABLE/VIEW mappings to `main_reports/tables_views_miss` for OMS consumption and prints a summary of missing objects. OMS cannot handle certain Oracle tables (unsupported types, temp tables, oversized LOBs, etc.), which are already curated in `OMS_USER.TMP_BLACK_TABLE`. The current output does not exclude these tables, so OMS rules can be invalid and the summary overcounts missing tables. Column diffs also need to recognize manual LONG/LONG RAW conversions.

## Goals / Non-Goals
- Goals:
  - Ingest `TMP_BLACK_TABLE` and classify blacklisted tables.
  - Exclude blacklisted missing tables from `tables_views_miss` output.
  - Emit a human-readable blacklist report with reasons.
  - Split missing TABLE counts into supported vs blacklisted.
  - Treat `LONG -> CLOB` and `LONG RAW -> BLOB` as compatible in column comparisons and fixups.
- Non-Goals:
  - Change OMS mapping file format beyond filtering invalid entries.
  - Auto-generate or update `TMP_BLACK_TABLE` on the source.
  - Alter remap inference rules or dependency logic outside the blacklist scope.

## Decisions
- Source of truth: read `OMS_USER.TMP_BLACK_TABLE` (OWNER, TABLE_NAME, DATA_TYPE, BLACK_TYPE). Any row for an OWNER.TABLE marks the table as blacklisted.
- Normalization: compare `BLACK_TYPE` case-insensitively; preserve unknown categories in outputs.
- Reason mapping (case-insensitive):
  - `SPE`: unsupported column types, no DDL
  - `TEMP_TABLE`: temporary table, no DDL
  - `DIY`: custom type, no DDL
  - `LOB_OVERSIZE`: LOB > 512 MiB; table can be created but OMS cannot sync
  - `LONG`: LONG/LONG RAW handled via conversion to CLOB/BLOB
- `blacklist_tables.txt` format: emit `main_reports/blacklist_tables.txt` grouped by schema with section headers, sorted by schema/table, and one line per unique (TABLE, BLACK_TYPE, DATA_TYPE) with the mapped reason text.
- `tables_views_miss` output: exclude any missing TABLE that is blacklisted; keep VIEW rules unchanged.
- Summary counts: add a separate line for blacklisted missing TABLEs; the TABLE missing count excludes them.
- Column diff logic: treat LONG->CLOB and LONG RAW->BLOB as equivalent; missing LONG columns map to CLOB/BLOB in ALTER TABLE ADD.

## Risks / Trade-offs
- `TMP_BLACK_TABLE` may contain many rows per table; reporting is verbose but preserves detailed reasons.
- If the blacklist is incomplete, OMS may still fail on unsupported tables; the tool only enforces known entries.

## Migration Plan
- No config changes required. New output file is additive and does not break existing OMS workflows.
