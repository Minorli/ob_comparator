## 1. Implementation
- [x] Add new settings parsing/validation (report_to_db, report_db_schema, report_retention_days, report_db_fail_abort, report_db_detail_mode, report_db_detail_max_rows, report_db_insert_batch, report_db_save_full_json)
- [x] Implement SQL literal escaping + CLOB chunking for obclient (single-quote, newline safe)
- [x] Implement report DB table creation (diff_ prefix) with obclient
- [x] Implement summary insert (obclient) with safe literal handling
- [x] Implement detail insert with INSERT ALL fallback to single-row inserts on failure
- [x] Implement grant detail insert (optional)
- [x] Implement counts table (DIFF_REPORT_COUNTS) and insert per-type summary counts
- [x] Implement retention cleanup
- [x] Wire into report generation flow (post report file output)
- [x] Ensure failure handling respects report_db_fail_abort

## 2. Tests
- [x] Unit tests for SQL literal escaping and CLOB chunking
- [x] Unit tests for report_id format + uniqueness
- [ ] Unit tests for SQL builder for summary/detail/grants
- [ ] Integration test: create tables + insert summary/detail/grants via obclient (local OB)
- [ ] Integration test: large detail rows with batch sizing and fallback
- [ ] Integration test: retention cleanup

## 3. Docs
- [x] Update config docs and templates with new switches
- [x] Add usage guide + query examples to docs
