# Change: Per-type missing/unsupported reports with pipe-delimited rows

## Why
当前报告把“因不支持导致差异”和“支持但未创建导致差异”混在多个文件里，且格式不统一，客户很难快速定位哪些对象需要改造、哪些需要直接 fixup。需要为每个对象类型输出清晰、可粘贴到 Excel 的明细文件，避免混淆。

## What Changes
- 新增按对象类型拆分的两类明细报告：
  - **missing_<TYPE>_detail_<ts>.txt**：支持但缺失（需要修补脚本）
  - **unsupported_<TYPE>_detail_<ts>.txt**：不支持/被阻断（需要改造，含 ROOT_CAUSE）
- 两类报告统一管道符 `|` 分隔，并包含 `#` 开头的字段头，便于 Excel 导入。
- 不支持/阻断明细补充 ROOT_CAUSE 字段，快速定位根因。
- 报告索引（report_index）新增上述文件条目，主报告增加明确指引。
- 兼容保留现有 `missing_objects_detail_*` / `unsupported_objects_detail_*` / `indexes_unsupported_detail_*` 等报告，不改变既有输出，避免破坏现有流程。

## Impact
- Affected specs: `export-reports`
- Affected code: `schema_diff_reconciler.py` 报告导出与索引、`readme_config.txt`、`docs/*`（报告说明）
- **No breaking changes**: 新增报告，不移除旧报告。
