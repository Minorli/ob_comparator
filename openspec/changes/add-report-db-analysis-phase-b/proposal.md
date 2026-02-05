# Change: Add Report-DB Analysis (Phase B)

## Why
report_to_db 已覆盖多数明细，但客户还需要：
1) 记录写库失败细节，避免“写库成功但表空”。
2) 支持修复闭环标记与“剩余问题”视图。
3) GRANT/USABILITY 的分类视角，便于定位原因。
4) 更易查询的辅助索引。

## What Changes (Phase B)
- 新增表：
  - `DIFF_REPORT_WRITE_ERRORS`：记录写库失败详情（表名/SQL/错误）。
  - `DIFF_REPORT_RESOLUTION`：用户标记修复状态（手工更新）。
- 新增分析视图：
  - `DIFF_REPORT_PENDING_ACTIONS_V`：actions 视图与 resolution 结合，输出未闭环清单。
  - `DIFF_REPORT_GRANT_CLASS_V`：GRANT 缺失/多余分类视图。
  - `DIFF_REPORT_USABILITY_CLASS_V`：USABILITY 原因归类视图（基于 reason 关键字）。
- 写库错误追踪：当 report_db 写入失败时，记录到 `DIFF_REPORT_WRITE_ERRORS`（不影响主流程）。
- 增加索引：针对新表及常用查询路径新增索引。

## Non-Goals
- 不修改校验与修补逻辑。
- 不改变已有报告语义。
- 不引入新的兼容性判断规则。

## Impact
- Affected specs: `export-reports`
- Affected code: `schema_diff_reconciler.py`（report_db DDL / 写库失败记录 / 视图创建）
- Risk: 仅新增表/视图/记录错误，不改变主逻辑。
