## Context
序列当前比较依赖属性字段（cache/min/max/last_number 等），Oracle 与 OceanBase 的元数据差异导致大量“mismatch”噪声。客户只关心序列是否存在，属性一致性不作为迁移验收条件。

## Goals / Non-Goals
- Goals:
  - 序列比较仅判定存在/缺失/多余。
  - 修补脚本生成逻辑不变（缺失仍生成）。
- Non-Goals:
  - 不对序列属性做任何自动修补或校正。
  - 不修改序列 DDL 的生成来源/清洗规则。

## Decisions
- 比较阶段：只做存在性检查（source in target? target extra?).
- 报告阶段：不再输出 sequence mismatched 明细与统计。

## Risks / Trade-offs
- 可能遗漏序列属性的细微差异，但与用户目标一致（只关心可用性）。

## Migration Plan
1. 调整 sequence compare 逻辑为 existence-only。
2. 清理 sequence mismatched 的统计/报告输出。
3. 增加单测：存在/缺失/多余的统计与报告。
