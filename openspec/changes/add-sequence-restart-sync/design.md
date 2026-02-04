## Context
序列在迁移后若从 START WITH 重新开始，可能与已迁移数据发生主键冲突。现有策略仅做存在性校验与 CREATE SEQUENCE 生成，缺少“值同步”机制。

## Goals / Non-Goals
- Goals:
  - 可选生成“序列值同步”脚本
  - 不改变现有“序列仅存在性校验”的比较逻辑
  - 默认安全（不开启、不自动执行）
- Non-Goals:
  - 不基于业务表扫描推导 MAX(id)
  - 不自动执行序列值同步（避免影响上线节奏）

## Decisions
- Decision: 引入 `sequence_sync_mode`，默认 `off`，仅在开启时生成 `sequence_restart` 脚本
- Decision: 采用 `LAST_NUMBER` 作为 RESTART WITH 值，不再叠加 CACHE
- Decision: `sequence_restart` 默认不执行，需显式开启 run_fixup 选项

## Risks / Trade-offs
- Risk: Oracle `LAST_NUMBER` 受 CACHE 影响，可能跳跃
  - Mitigation: 明确告知“LAST_NUMBER 可能有 gap，但避免回退冲突更安全”
- Risk: OB 对 `RESTART WITH` 支持度需确认
  - Mitigation: 加实测与报错降级（保留脚本但不自动执行）

## Migration Plan
1. 增加配置项与默认值（off）
2. 生成 sequence_restart 目录及脚本
3. run_fixup 增加显式执行开关
4. 文档更新与实测

## Open Questions
- 是否需要支持 `table_max` 模式（基于业务表 MAX(id)）
- 是否需要在报告中附带“推荐执行顺序”
