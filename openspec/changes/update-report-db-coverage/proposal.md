# Change: 扩展 report_to_db 覆盖面（减少依赖 TXT 报告）

## Why
当前 report_to_db 已覆盖 summary / counts / detail / grants / usability / package_compare / trigger_status，但大量关键诊断信息仍只能从 TXT 报告中获取。客户反馈使用数据库查询（+HOW_TO SQL）效率远高于人工翻阅 20+ 报告，希望绝大多数问题能通过固定 SQL 直接定位。

## What Changes
- 扩展报告写库覆盖面，新增多张 `diff_` 表以结构化存储以下内容：
  - 依赖链（dependency_chains / VIEWs_chain）
  - remap 冲突
  - 全量对象映射
  - 黑名单表明细（含 LONG 转换状态）
  - fixup 跳过汇总（按类型/原因）
  - OMS missing 表/视图映射（可选）
- 新增 **报告工件目录表**（artifact catalog），记录所有 TXT 报告的路径、hash、字段、行数和 DB 覆盖状态，保证“即使不看 TXT，也知道 DB 覆盖范围”。
- 为诊断查询提供统一套路（在文档中提供固定 SQL 模板），并保证能在 OB 中直接查出：
  - 缺失 vs 不支持
  - 依赖阻断与根因
  - remap 冲突
  - 黑名单与转换状态
  - fixup 可执行范围
- 新增写库范围控制开关 `report_db_store_scope`（`summary|core|full`），默认 `full`。

## Impact
- Affected specs: `export-reports`, `configuration-control`
- Affected code: `schema_diff_reconciler.py`（report_db 写库扩展、工件目录、数据落地逻辑）
- Affected docs: `readme_config.txt`, `docs/ADVANCED_USAGE.md`, `HOW_TO_READ_REPORTS_IN_OB_60_sqls.txt`
- Non-breaking: 默认行为保持一致（report_to_db=true 时扩展写库；report_to_db=false 不影响 TXT 报告）
