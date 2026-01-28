## Context
目前报告按“汇总/明细/辅助”混排，且不支持与缺失分散在多个文件中；部分明细无统一分隔符，导致用户筛选困难。

## Goals / Non-Goals
- Goals:
  - 每个对象类型都有「支持但缺失」与「不支持/阻断」两份明细。
  - 输出格式统一为 `|`，可直接导入 Excel。
  - 保留原有明细与辅助报告，不改变既有消费者。
- Non-Goals:
  - 不改动依赖链、VIEWs_chain 等结构化链路文件格式。
  - 不减少现有报告数量（仅新增）。

## Decisions
- **命名**：在 `main_reports/run_<ts>/` 下新增：
  - `missing_supported_<TYPE>_<ts>.txt`
  - `unsupported_<TYPE>_<ts>.txt`
- **范围**：覆盖所有对象类型（PRIMARY + EXTRA），包括 `TABLE/VIEW/PROCEDURE/.../INDEX/CONSTRAINT/TRIGGER/SEQUENCE`。
- **字段**：统一字段头，保持 `missing_objects_detail` 语义：
  - `SRC_FULL|TYPE|TGT_FULL|STATE|REASON_CODE|REASON|DEPENDENCY|ACTION|DETAIL`
  - 额外信息（如索引列、约束表达式）写入 `DETAIL` 的 `key=value` 形式。
- **触发条件**：仅在 `report_detail_mode=split` 时生成（与现有明细策略一致）。
- **索引与指引**：写入 `report_index_<ts>.txt` 并在主报告“检查汇总”后新增指引段落。

## Risks / Trade-offs
- **文件数量增加**：每个类型两份文件会增量输出。通过集中在 run 目录并保留索引降低可见负担。
- **字段统一 vs 类型差异**：统一字段便于工具化，但部分细节只能放入 `DETAIL`，需在说明文档明确。

## Migration Plan
1. 新增导出函数输出 per-type missing/unsupported 报告。
2. 更新 report_index 与主报告说明。
3. 更新 `readme_config.txt` 与 docs 中“报告说明”。
4. 增加单元测试覆盖：per-type 文件生成与字段一致性。

## Open Questions
- 是否需要引入 `report_detail_by_type` 开关以控制输出数量？（当前方案默认随 `report_detail_mode=split`）
