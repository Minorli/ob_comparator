# Change: Classified missing/unsupported reports + LONG dependency handling + sequence existence-only

## Why
客户关注的核心已明确：
1) 哪些差异是**不兼容/不支持**导致（需要改造）
2) 哪些差异是**支持但未创建**导致（需要 fixup）

当前报告与阻断逻辑在以下方面混淆：
- 缺失/不支持混在同一明细中，且格式不统一，难以按类型筛选。
- LONG/LONG RAW 被当作严格黑名单，导致依赖对象被阻断，与“已转换仍可用”的现实不一致。
- SEQUENCE 属性对比噪声过大，影响用户判断。

本提案融合审计文档：
- `audit/BLACKLIST_DEPENDENCY_TEST_CASE.md`
- `audit/UNSUPPORTED_BY_TYPE_IMPL_SPEC.md`
并整合此前暂停的提案：
- `update-report-per-type-support-split`
- `update-long-blacklist-dependency-handling`
- `update-sequence-existence-only`

## What Changes
1) **按对象类型输出两类明细**（支持缺失 vs 不兼容/阻断）
   - `missing_<TYPE>_detail_<ts>.txt`：支持但缺失（应生成 fixup）
   - `unsupported_<TYPE>_detail_<ts>.txt`：不兼容/阻断（需改造）
   - 统一 `|` 分隔并包含 `#` 头部，便于 Excel。
   - 保留现有汇总明细文件（不删除旧文件），避免破坏已有流程。

2) **LONG/LONG RAW 黑名单逻辑调整（非阻断，仍生成 fixup）**
   - LONG/LONG RAW 属于“类型转换表”，不再作为依赖阻断源。
   - 目标端表存在即视为已转换（符合用户现状）。
   - 仍输出 LONG 转换状态到黑名单报告，但不再影响阻断判断。
   - 对于 LONG 表自身的缺失：标记为“缺失”，**生成 fixup**（自动将 LONG/LONG RAW 转为 CLOB/BLOB），且依赖对象不阻断。

3) **SEQUENCE 比较降级为存在性检查**
   - 仅检查是否存在（missing/extra），不比较 cache/increment/min/max 等属性。
   - 清理 sequence mismatched 的统计与明细，避免噪声。

4) **ROOT_CAUSE 追溯**
   - 对 BLOCKED 对象新增根因追溯字段（溯源到具体黑名单表/不支持对象）。
   - 输出在 per-type unsupported 报告中，辅助快速定位。

## Impact
- Affected specs: `compare-objects`, `export-reports`, `generate-fixup`
- Affected code: `schema_diff_reconciler.py`（分类逻辑、依赖阻断、序列对比、报告导出）
- **No breaking changes**: 旧报告保留，新增 per-type 明细。
