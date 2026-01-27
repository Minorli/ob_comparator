## Context
当前 DDL 生成路径中大量直接拼接 `schema.object`，导致生成 `"SCHEMA.OBJECT"` 的非法格式；触发器和视图/同义词/FK/ALTER/DROP 等路径均受到影响。另有 CHECK 约束在名称已存在时仍被判缺失的问题，造成重复创建。

## Goals / Non-Goals
- Goals:
  - 统一输出 `"SCHEMA"."OBJECT"` 格式，避免非法引号。
  - 约束比对以“名称存在”为主，不再重复创建。
  - 不改变 remap 推导、依赖图与检查范围，仅修复输出与误判。
- Non-Goals:
  - 不新增用户开关或改变现有配置语义。
  - 不改变 dbcat/metadata 的来源选择策略。

## Decisions
- 使用统一 helper 处理引号，避免散落的手工拼接。
- 对触发器 CREATE/ON 子句与触发器体内引用进行一致化处理。
- 约束比对优先按名称判断存在性，表达式不一致仅记录差异。

## Risks / Trade-offs
- 引号统一可能影响某些已带引号对象：需确保不重复包裹。
- 触发器体内替换需避免误改字符串/注释，继续使用 SqlMasker。

## Migration Plan
1) 引入引号 helper 并替换核心拼接点。
2) 更新触发器重写与 FK/VIEW/SYNONYM DDL 生成。
3) 修正约束比对逻辑。
4) 增补单测与真实 OB 执行验证。

## Open Questions
- ALTER/DROP 等“可选引号”场景是否全部强制引号？默认计划统一强制，以提升兼容与确定性。
