## 1. Implementation
- [x] 1.1 Load `OMS_USER.TMP_BLACK_TABLE` into source metadata and group entries by table (OWNER.TABLE_NAME).
- [x] 1.2 Normalize `BLACK_TYPE` case, map known categories to reason text, and preserve unknown categories in outputs.
- [x] 1.3 Exclude blacklisted missing TABLE entries from `tables_views_miss` export while keeping supported TABLE/VIEW output unchanged.
- [x] 1.4 Generate `main_reports/blacklist_tables.txt` grouped by schema and sorted, with `BLACK_TYPE`, `DATA_TYPE`, and reason details.
- [x] 1.5 Update summary counts to split missing TABLE counts into supported vs blacklisted.
- [x] 1.6 Treat `LONG -> CLOB` and `LONG RAW -> BLOB` as compatible column types; map missing LONG columns to `CLOB`/`BLOB` in ALTER TABLE ADD.
- [x] 1.7 Add/update unit tests for blacklist filtering, report outputs, summary counts, and LONG handling.
- [x] 1.8 Update docs and bump the software version to `0.9.3`.
