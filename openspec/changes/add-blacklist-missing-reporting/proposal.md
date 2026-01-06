# Change: Add blacklist-aware missing table reporting

## Why
OMS cannot migrate certain Oracle tables (unsupported data types, temp tables, oversized LOBs, etc.). The current `tables_views_miss` output includes these tables, producing unusable rules and inflating missing counts. We also need explicit handling for LONG/LONG RAW column conversions to avoid false mismatches and incorrect fixups.

## What Changes
- Ingest `OMS_USER.TMP_BLACK_TABLE` to classify blacklisted tables.
- Exclude blacklisted missing tables from `tables_views_miss` and generate `main_reports/blacklist_tables.txt` with reasons.
- Split missing table counts in the summary: supported vs blacklisted.
- Treat `LONG -> CLOB` and `LONG RAW -> BLOB` as compatible; map missing LONG columns to `CLOB`/`BLOB` in fixup.

## Impact
- Affected specs: missing-object-reporting, column-diff-handling
- Affected code: `schema_diff_reconciler.py`, `test_schema_diff_reconciler.py`, `README.md`, `docs/` files
