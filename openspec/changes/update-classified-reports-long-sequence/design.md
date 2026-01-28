## Context
审计文档指出：
- LONG/LONG RAW 应视为“类型转换表”，不应阻断依赖对象；
- 缺失/不支持应按对象类型拆分并统一可粘贴格式；
- SEQUENCE 属性对比导致噪声。

该提案在不破坏现有输出的前提下新增“按类型分类明细”，并修正阻断逻辑与序列比较。

## Goals / Non-Goals
- Goals:
  - per-type 明确区分 **支持缺失** vs **不兼容/阻断**。
  - LONG/LONG RAW 不再阻断依赖；黑名单报告仍输出转换状态。
  - 序列比较仅存在性，消除噪声。
  - 保留旧报告，新增索引指引。
- Non-Goals:
  - 不删除现有明细/辅助报告。
  - 不改变 VIEW chain、dependency_chains 的格式。

## Decisions
### 1) LONG/LONG RAW 处理
- 分类为“类型转换表”：**不进入依赖阻断源集合**。
- 目标端表存在 ⇒ 视为已转换（满足用户确认）。
- 目标端表缺失 ⇒ 记录为“缺失”，依赖对象仍按正常缺失逻辑处理。
- 黑名单报告继续输出 LONG 转换校验状态，仅用于提示。
- **Fixup 决策**：LONG 表缺失时 **仍生成 CREATE TABLE fixup**，并将 LONG/LONG RAW 自动映射为 CLOB/BLOB；依赖对象可正常生成 fixup。

### 2) 依赖阻断来源
- 仅由“真正不支持表/对象（SPE/DIY/等）”及 INVALID 对象触发阻断。
- LONG 不进入阻断源集合。

### 3) per-type 明细输出
- 仅在 `report_detail_mode=split` 下输出。
- 统一 `|` 分隔，首行 `#` 头。
- 文件命名：
  - `missing_<TYPE>_detail_<ts>.txt`
  - `unsupported_<TYPE>_detail_<ts>.txt`
- 旧明细文件仍保留：`missing_objects_detail_*`, `unsupported_objects_detail_*`, `indexes_unsupported_detail_*` 等。

### 4) ROOT_CAUSE 追溯
- 仅对 BLOCKED 对象计算根因：
  - 追溯依赖链直到命中“硬不支持对象”，输出 `ROOT_OBJ(REASON)`。
- 用缓存避免 O(N^2) 追溯开销。

### 5) SEQUENCE 比较
- 仅比较是否存在（missing/extra）。
- 不再生成/输出 `detail_mismatch`。

## Risks / Trade-offs
- LONG 表自动生成 fixup 后，需验证字段映射与表级 DDL 清洗是否稳定。
- per-type 文件数量增加，但主报告与 report_index 会明确指引。

## Migration Plan
1. 调整 LONG 黑名单阻断逻辑与 fixup 排除。
2. 加入 ROOT_CAUSE 追溯字段。
3. 新增 per-type 缺失/不支持明细导出。
4. 简化 sequence compare，清理相关报告输出。
5. 更新报告索引与文档说明。

## Open Questions
- 是否需要可选开关控制 per-type 明细输出数量？（默认跟随 report_detail_mode=split）
