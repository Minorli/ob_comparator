## Context
视图迁移涉及 DDL 清洗、依赖链授权、执行顺序控制。当前 grants_miss 可能在视图创建前执行，导致对视图对象授权失败；同时 FORCE 视图会掩盖不可用问题；X$ 系统表引用在 OB 不可用但易漏检。

## Goals / Non-Goals
- Goals:
  - 识别 X$ 依赖并阻断，保留用户自建 X$ 例外
  - 移除 FORCE 关键字，避免无效视图被创建
  - 拆分 view_prereq_grants / view_post_grants，保证顺序
- Non-Goals:
  - 不改变已有对象存在性校验口径
  - 不引入新的运行时依赖

## Decisions
- X$ 规则：默认阻断，但若依赖对象存在于受管 schema 则视为用户自建并放行。
- GRANT 顺序：新增 view_prereq_grants 在视图创建前执行；view_post_grants 在视图创建后执行。

## Risks / Trade-offs
- 拆分 grants 会引入新目录，需要更新 run_fixup 顺序与文档提示。
- X$ 识别依赖于依赖图/DDL 提取，极少数解析失败的场景仍需人工复核。

## Migration Plan
- 更新 fixup 输出目录及 run_fixup 顺序。
- 更新报告提示与文档，提醒执行顺序。

## Open Questions
- 是否需要为 view_prereq_grants 增加可选开关（默认开启）？
