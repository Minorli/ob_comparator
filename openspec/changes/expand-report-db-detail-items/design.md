## Context
DIFF_REPORT_DETAIL uses detail_json for structured differences. Users want queryable row-level details without parsing JSON.

## Decisions
- Add a single generic table `DIFF_REPORT_DETAIL_ITEM` instead of many specialized tables.
- Use `ITEM_TYPE` to classify detail rows (e.g., MISSING_COLUMN, EXTRA_COLUMN, LENGTH_MISMATCH, TYPE_MISMATCH, MISSING_INDEX, EXTRA_INDEX, INDEX_DIFF, MISSING_CONSTRAINT, EXTRA_CONSTRAINT, CONSTRAINT_DIFF, MISSING_SEQUENCE, EXTRA_SEQUENCE, SEQUENCE_DIFF, MISSING_TRIGGER, EXTRA_TRIGGER, TRIGGER_DIFF, REASON_CODE, DEPENDENCY, ACTION, ROOT_CAUSE).
- Use `ITEM_KEY` for the element name (column/index/constraint/sequence/trigger), `SRC_VALUE` and `TGT_VALUE` for source/target attributes when applicable, `ITEM_VALUE` for free-form detail or expression.
- Gate writes by `report_db_store_scope=full` and `report_db_detail_item_enable`.
- Enforce `report_db_detail_item_max_rows` (default = report_db_detail_max_rows) to avoid unbounded growth.

## Risks / Trade-offs
- Row counts can be high in very wide tables. Mitigate via row cap and store_scope gating.

## Migration Plan
- Create new table if missing.
- Insert rows after DIFF_REPORT_DETAIL is written.
- Update documentation and query examples.
