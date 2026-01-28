## Context
LONG/LONG RAW 目前通过 TMP_BLACK_TABLE 进入黑名单逻辑，且依赖关系会被阻断。现有代码虽然会校验 LONG→CLOB/LONG RAW→BLOB 的转换，但阻断判定依赖“是否转换成功”。用户明确要求：只要目标端表存在，就不应阻断依赖对象。

## Goals / Non-Goals
- Goals:
  - 依赖阻断仅基于“目标端表是否存在”，不再依赖 LONG 列转换校验。
  - 保留 LONG 转换校验的报告输出（黑名单报告）。
- Non-Goals:
  - 不改变 LONG 列在表差异比较与 fixup 生成中的校验规则。
  - 不新增新的配置开关。

## Decisions
- 判断逻辑：
  - LONG-only 黑名单表：
    - 目标端表存在 ⇒ 不加入 unsupported_nodes（不阻断依赖）。
    - 目标端表不存在 ⇒ 继续加入 unsupported_nodes（阻断依赖）。
- LONG 转换校验仍执行，仅用于 blacklist_tables.txt 报告的 STATUS/DETAIL。

## Risks / Trade-offs
- 可能出现“表存在但 LONG 列未转换”的情况：
  - 该问题将通过**表列差异**继续暴露，但不再阻断依赖对象。
  - 符合用户期望的“依赖不中断”策略。

## Migration Plan
1. 调整 LONG-only 黑名单阻断判断为“目标表是否存在”。
2. 保留 LONG 转换校验与报告输出逻辑。
3. 更新报告说明中 LONG 黑名单的阻断含义。

## Open Questions
- 无（按用户确认：表存在即视为已转换）。
