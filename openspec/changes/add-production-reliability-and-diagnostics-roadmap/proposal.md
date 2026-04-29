## Why

0.9.9.5 暴露了一个发布流程问题：本地单测和静态检查可以通过，但没有强制实库 smoke、挂起诊断和回滚约束时，客户现场一旦出现“不报错、不继续”，定位成本很高。这个 change 把生产可靠性、操作可解释性、fixup 分级、断点恢复、兼容矩阵和现场诊断包固化为 0.9.x 后续优先工作，明确把大规模代码重构放到 1.0，把 UI 放到 1.1。

## What Changes

- 增加 release governance：每次正式版本必须有 Oracle->OB 实库 smoke、验证矩阵、发布回滚/热修策略和 release evidence。
- 增强 runtime observability：主程序与 `run_fixup.py` 的长阶段、对象循环、SQL 文件/语句执行必须有心跳、当前对象/文件、耗时阈值告警和可定位日志。
- 收敛 timeout 策略：统一说明并约束 `cli_timeout`、`obclient_timeout`、`fixup_cli_timeout`、`ob_session_query_timeout_us` 的默认值、边界、日志展示和现场建议。
- 增强差异解释：每条 mismatch/fixup SQL 必须能回答“为什么生成”，特别是 VARCHAR/VARCHAR2、CHAR/BYTE、NULLABLE、CHECK、DEFAULT、GRANT、dependency。
- 增加 fixup safety tiers：将脚本按 `safe`、`review`、`destructive`、`manual` 分层，并让 `run_fixup.py` 支持按层选择和默认保护。
- 增加 run recovery：主程序阶段级断点恢复、对象级重放和 `run_fixup.py` 文件/语句级恢复能力。
- 增加 compatibility matrix：通过独立 JSON registry 机器可读地表达 Oracle/OB/source mode/object family 的 supported/degraded/manual/unsupported 决策，并写入报告。
- 深入设计 customer diagnostic package：新增独立 `diagnostic_bundle.py` CLI，一键采集配置摘要、版本、环境、最后阶段、耗时、日志尾部、报告索引、fixup 计划、诊断清单，但不采集明文密码。
- 非目标：本 change 不做 `schema_diff_reconciler.py` 模块化重构；该工作规划到 1.0 release。
- 非目标：本 change 不做图形界面；只为未来 1.1 UI 准备结构化数据。

## Capabilities

### New Capabilities
- `release-governance`: 版本发布门禁、release evidence、回滚/热修策略和验证约束。
- `operator-diagnostic-package`: 独立客户现场诊断包 CLI、脱敏配置摘要、挂起/失败证据采集和支持交付格式。

### Modified Capabilities
- `configuration-control`: timeout 策略、诊断包开关、release/smoke 配置和恢复相关配置需要固化。
- `compare-objects`: compare 结果需要携带差异解释、证据来源和兼容矩阵决策。
- `generate-fixup`: fixup SQL 需要带生成原因、安全分层、兼容决策和恢复元数据。
- `execute-fixup`: 执行器需要按安全层选择、输出执行心跳、暴露当前 SQL、支持恢复和更清晰的超时行为。
- `export-reports`: 报告需要展示 release evidence、阶段心跳摘要、差异解释、兼容矩阵、诊断包索引和恢复入口。

## Impact

- Affected code: `schema_diff_reconciler.py`, `run_fixup.py`, `diagnostic_bundle.py`, `init_users_roles.py`, config parsing/wizard paths, report/export helpers, release scripts, compatibility registry loader.
- Affected docs: `README.md`, `readme_config.txt`, `readme_lite.txt`, `docs/CHANGELOG.md`, deployment docs, release checklist/runbooks.
- Affected outputs: `main_reports/run_<ts>/`, `fixup_scripts/`, `logs/`, `diagnostic_bundle_<run_id>.zip`, compatibility registry/matrix artifacts, optional report_db summaries.
- Test impact: local-only or ignored verification harness checks for explanation/tiering/recovery/diagnostic package, plus mandatory real Oracle/OB smoke for release candidates.
- Operational impact: release is slower but defensible; field triage gets deterministic artifacts instead of ad hoc log requests.
