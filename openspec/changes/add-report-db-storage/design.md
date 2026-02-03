# Design: Report DB Storage (obclient-only)

## Constraints
- **Must use obclient** for all DB writes (oracledb driver unavailable for OB Oracle mode).
- **No change** to existing file reports; DB storage is additive and optional.

## Table Naming
- All tables use `diff_` prefix to avoid collisions.
- Default schema: `report_db_schema` if provided, otherwise OB connection user schema.

## Tables
- `DIFF_REPORT_SUMMARY`: run-level summary
- `DIFF_REPORT_DETAIL`: per-object detail rows
- `DIFF_REPORT_GRANT`: grants plan rows (optional)
- `DIFF_REPORT_COUNTS`: per-object-type counts (from “检查汇总”)

## Insert Strategy
1. Prefer `INSERT ALL ... SELECT 1 FROM DUAL` for batch inserts.
2. If INSERT ALL fails (syntax/length), fallback to single-row INSERTs in a loop.

## SQL Literal Safety
- Implement a `sql_quote_literal(value)` helper:
  - Replace `'` with `''`.
  - Preserve newlines and tabs (OB Oracle mode accepts literal newlines).
  - Return `NULL` for empty/None.
- For large text fields (CLOB/JSON):
  - Chunk into 2000–3000 chars and join with `TO_CLOB('...') || ...` to avoid literal limits.

## Failure Policy
- `report_db_fail_abort=false` by default: write failures are logged but do not interrupt the run.
- If set true, propagate error after logging.

## Retention Cleanup
- Use DELETE on summary table with timestamp predicate; rely on FK cascade to detail/grant tables.

## Performance Controls
- `report_db_detail_mode` selects which detail types are stored (default missing/mismatched/unsupported).
- `report_db_detail_max_rows` caps detail inserts to avoid huge writes; overflow logged in summary.
- `report_db_insert_batch` sets batch size for INSERT ALL.

## Summary Truncation Metadata
- Summary table records `DETAIL_TRUNCATED` and `DETAIL_TRUNCATED_COUNT` when detail rows exceed cap.
