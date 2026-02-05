## Context
Phase A 已提供 actions/profile/trends 视图和 SQL 模板。Phase B 补齐闭环标记、写库失败追踪和分类视图。

## Goals / Non-Goals
- Goals:
  - 将写库失败可追踪；
  - 支持“已修复/未修复”标记；
  - 通过分类视图降低客户理解成本。
- Non-Goals:
  - 不改变现有校验/修复逻辑。

## Decisions
- 新增 `DIFF_REPORT_WRITE_ERRORS` 表，任何写库异常都尝试记录，不影响主流程。
- 新增 `DIFF_REPORT_RESOLUTION` 表，由用户手动维护。
- 视图只读；创建失败不阻断主流程，写入 ARTIFACT 记录。

## Proposed Tables
1) DIFF_REPORT_WRITE_ERRORS
   - report_id, table_name, sql_snippet, error_message, created_at
2) DIFF_REPORT_RESOLUTION
   - report_id, object_type, schema_name, object_name, action_type, resolution_status, resolved_by, resolved_at, note

## Proposed Views
- DIFF_REPORT_PENDING_ACTIONS_V：actions 与 resolution 结合，仅输出未完成项
- DIFF_REPORT_GRANT_CLASS_V：GRANT 视角分类（missing/extra/with_grant_option）
- DIFF_REPORT_USABILITY_CLASS_V：USABILITY 分类（ORA-00942/权限/语法/超时/依赖等）

## Risks / Trade-offs
- 分类基于文本 reason，可能存在误判 → 仅辅助视图，不影响主逻辑。

## Migration Plan
- 在 report_to_db 下创建新表/视图；旧环境不受影响。
