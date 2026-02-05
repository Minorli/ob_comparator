# Change: Add Report-DB Analysis (Phase A)

## Why
客户希望绝大多数排查工作在 OceanBase 内完成，不再依赖大量 TXT 报告。
当前 report_to_db 已覆盖基础明细，但缺少任务视图、对象画像、趋势与可直接执行的 SQL 模板。

## What Changes (Phase A)
- 新增 **只读分析视图**（基于既有报告表，不改变主逻辑）：
  - `DIFF_REPORT_ACTIONS_V`：迁移任务清单（FIXUP/REFACTOR/DEPENDENCY/GRANT/VERIFY）。
  - `DIFF_REPORT_OBJECT_PROFILE_V`：对象问题画像（缺失/不支持/阻断/可用性/依赖/黑名单聚合）。
  - `DIFF_REPORT_TRENDS_V`：多次运行趋势（基于 SUMMARY/COUNTS）。
- 新增 **SQL 模板文件**：`report_sql_<timestamp>.txt`（预填 report_id）。
- 扩展 **report artifact** 记录上述视图/模板的可用性与路径。

## Phase B (Roadmap, not implemented in this change)
- 用户修复闭环标记表（RESOLUTION）。
- 写库失败追踪表（WRITE_ERRORS）。
- GRANT 差异细化（with grant option 分类）。
- USABILITY 精细化字段（ROOT_CAUSE_CODE）。
- 性能索引增强/分区策略（按 report_id）。

## Impact
- Affected specs: `export-reports`
- Affected code (future implementation): `schema_diff_reconciler.py`（report_to_db 写库 + 视图/模板输出）
- Risk: 仅新增只读视图与输出文件，不改变校验/修复逻辑。

## Non-Goals
- 不调整主对比逻辑或 fixup 行为。
- 不引入新的校验规则或兼容性判断。
