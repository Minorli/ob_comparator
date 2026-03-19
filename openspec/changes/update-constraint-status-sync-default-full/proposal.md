# Change: Make Constraint Status Sync Default to Full

## Why
当前 `constraint_status_sync_mode` 默认值是 `enabled_only`，这会导致工具默认只同步：

- `ENABLED`
- `DISABLED`

而不会默认检查或生成以下状态漂移修复：

- `VALIDATED`
- `NOT VALIDATED`

客户现场已经出现以下真实困惑：

- Oracle 侧外键是 `VALIDATED`
- OceanBase 侧同名外键已经存在，但状态是 `NOT VALIDATED`
- 工具默认没有报出该状态漂移，也没有生成 `ENABLE VALIDATE CONSTRAINT` 修复 DDL

这会让客户误判为工具“不支持已存在约束的 validate 状态对齐”。

## Scope
本 change 只调整默认值与默认文档口径：

- `constraint_status_sync_mode` 默认值从 `enabled_only` 改为 `full`
- 保持开关名、可选值与现有行为不变
- 保持 `PK/UK` 不生成 `ENABLE/[NO]VALIDATE` 状态修复 SQL 的现有保护不变

不包含：

- 新增配置项
- 改动缺失约束 `safe_novalidate/source/force_validate` 策略
- 改动 `run_fixup.py` 执行策略

## What Changes
1. 将 `constraint_status_sync_mode` 默认值改为 `full`
2. 同步更新：
   - 代码默认值
   - 配置模板
   - 配置说明
   - README / 技术文档
3. 做 Oracle + OceanBase 实库验证，证明默认配置下：
   - 已存在 `FK` 的 `VALIDATED -> NOT VALIDATED` 状态漂移会被识别
   - 会生成 `ENABLE VALIDATE CONSTRAINT` 状态修复 DDL
   - 执行后 rerun compare 能消除对应状态差异

## Hard Constraints
1. 不得改变 `constraint_missing_fixup_validate_mode` 的默认值与行为
2. 不得让 `PK/UK` 生成 `ENABLE/[NO]VALIDATE` 状态修复 SQL
3. 不得改变主报告、detail、report_db 的现有统计口径，仅改变默认是否生成状态修复脚本

## Impact
- Affected specs:
  - `configuration-control`
  - `generate-fixup`
