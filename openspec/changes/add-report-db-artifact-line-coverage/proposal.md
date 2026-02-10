# Change: report_to_db 实现 TXT 报告 100% 覆盖（按行入库）

## Why
当前 report_to_db 已覆盖大部分结构化明细，但仍有一部分信息仅存在于 run 目录 txt 文件（例如主对象 extra 清单等）。客户在生产排查时希望“不看 txt，也能在 OB 里完整查询”。

## What Changes
- 新增 `DIFF_REPORT_ARTIFACT_LINE` 表：按 `report_id + 文件 + 行号` 存储 run 目录下所有 txt 的原始行文本。
- `report_db_store_scope=full` 时，自动将 run 目录 txt 全量逐行写入数据库，实现文本层面 100% 覆盖。
- 保持现有结构化表（detail/detail_item/...）不变；按行表作为“兜底全量层”。
- 扩展 HOW_TO 手册：补充“按文件/按行回放 txt”的查询模板。
- 调整 artifact 覆盖状态判定：在 `full` 范围下，txt 报告状态应反映“已入库可查”。

## Impact
- Affected specs: `export-reports`
- Affected code: `schema_diff_reconciler.py`
- Affected docs: `HOW_TO_READ_REPORTS_IN_OB_*_sqls.txt`, `readme_config.txt`, `docs/ADVANCED_USAGE.md`
- Backward compatibility: 无破坏性变更；仅在 `report_to_db=true` 且 `report_db_store_scope=full` 时增加写库数据量。
