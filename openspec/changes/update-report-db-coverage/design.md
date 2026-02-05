## Context
现有 report_to_db 已覆盖部分报告，但仍有大量 TXT 报告无法在数据库中直接查询。客户希望 90% 诊断问题可通过固定 SQL 在 OB 中定位，尽可能减少 TXT 依赖。

## Goals
- 覆盖关键诊断报告到 OB（依赖、remap、黑名单、fixup 跳过、映射）。
- 明确 DB 覆盖范围：可查询数据 vs 仍需 TXT。
- 保持 report_to_db 的性能与容量可控（行数/字段裁剪/工件目录）。

## Non-Goals
- 不移除 TXT 报告（仍保留完整输出）。
- 不重构现有对比逻辑，仅扩展写库输出。
- 不在此版本引入复杂的 DB 视图/物化视图（仅提供 SQL 模板）。

## Decisions
1. **新增结构化表而非 CLOB 全量入库**：优先结构化字段 + JSON 明细，避免查询困难。
2. **新增工件目录表**记录所有 TXT 报告的路径与 hash，用于覆盖说明与溯源。
3. **容量控制**：沿用 `report_db_detail_max_rows`，超过阈值的明细改为工件记录并在 summary 标记截断。
4. **写库范围控制**：新增 `report_db_store_scope`（summary|core|full），默认 full，确保可按需降级写库规模。
5. **向后兼容**：新增表不影响既有表结构；已存在表仅新增索引或字段时做安全 ALTER（可选）。

## Data Model (新增表，统一 diff_ 前缀)
- `DIFF_REPORT_ARTIFACT`：报告工件目录（report_id, artifact_type, path, hash, row_count, fields, status, note）
- `DIFF_REPORT_DEPENDENCY`：依赖边（src/dep 类型+状态+reason）
- `DIFF_REPORT_VIEW_CHAIN`：VIEW 依赖链（root/view/node/level/exist/grant_status/block_reason）
- `DIFF_REPORT_REMAP_CONFLICT`：remap 冲突（source_full, object_type, reason, candidates/json）
- `DIFF_REPORT_OBJECT_MAPPING`：全量映射（src_full, object_type, tgt_full, remap_source, infer_method）
- `DIFF_REPORT_BLACKLIST`：黑名单表明细（owner, table, black_type, data_type, status, detail, conversion_status）
- `DIFF_REPORT_FIXUP_SKIP`：fixup 跳过汇总（object_type, reason, counts）
- `DIFF_REPORT_OMS_MISSING`（可选）：OMS 迁移表/视图映射（target_schema, type, src_full, tgt_full）

## Ingestion Plan
- 所有表在 `report_to_db=true` 时创建/写入。
- 数据来源使用内存结构（避免再读 TXT）：
  - dependency_chains / view_chain：依赖图与 view chain 生成过程
  - remap_conflicts / object_mapping：映射阶段结果
  - blacklist_tables：黑名单收集结构
  - fixup_skip_summary：fixup 生成统计
  - OMS missing：从 missing detail + mapping 直接推导
- 若超出 `report_db_detail_max_rows`，写入 summary 截断标记，并将 TXT 路径写入 `DIFF_REPORT_ARTIFACT`。

## Risks / Trade-offs
- 报告写库规模增大 → 需要强制行数/字段裁剪。
- 多表写入耗时增加 → 可按 scope（summary/core/full）分级控制。

## Migration Plan
- 新增表自动建表，不破坏已有数据。
- 可选：新增列时使用 `ALTER TABLE ADD` 并忽略已存在字段。

## Open Questions
- 是否需要为工件目录提供最小 SQL 视图（便于查询覆盖情况）？
