## Context
report_to_db 现已覆盖 SUMMARY/DETAIL 等核心表，但客户希望在 OB 内直接得到“可执行任务清单”和“对象全景画像”。

## Goals / Non-Goals
- Goals:
  - 只读视图聚合已有表，减少 TXT 依赖。
  - SQL 模板可直接执行（预填 report_id）。
- Non-Goals:
  - 不影响校验/修补逻辑。
  - 不引入新的校验规则。

## Decisions
- 仅新增视图与模板文件，不新增新表（Phase A）。
- 视图创建失败（权限不足等）时不阻断主流程，记录到 ARTIFACT/日志。

## Proposed Views
1. DIFF_REPORT_ACTIONS_V
   - 基于 DETAIL/DETAIL_ITEM/USABILITY/BLACKLIST 汇总 action_type。
2. DIFF_REPORT_OBJECT_PROFILE_V
   - 每个对象一行，聚合缺失/不支持/阻断/可用性/依赖/黑名单摘要。
3. DIFF_REPORT_TRENDS_V
   - 基于 SUMMARY/COUNTS 聚合趋势。

## Risks / Trade-offs
- 视图聚合逻辑错误会误导用户 → 必须用实机 SQL 验证。

## Migration Plan
- Phase A：创建视图 + 模板输出；不修改主逻辑。
- Phase B：增加用户标记表、写库失败追踪等。

## Open Questions
- 是否需要 `report_db_generate_sql_templates` 开关？（建议 Phase A 默认生成）
