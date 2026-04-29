## Context

当前工具已经覆盖大量 Oracle->OceanBase 与 OceanBase->OceanBase 结构差异场景，但生产可靠性能力仍偏“事后人工判断”：发布前缺少强制实库 smoke，长时间运行时缺少稳定心跳，timeout 之间的关系不够直观，fixup 脚本缺少统一安全层，现场问题依赖人工收集日志和报告。0.9.9.5 的发布过程说明，本地单测通过不等于现场运行可靠；后续需要把这些约束做成机制。

现阶段不做大规模模块化重构，因为 `schema_diff_reconciler.py` 的拆分会扩大改动面，适合作为 1.0 release 的主线。也不做 UI，因为 UI 会引入新的交互面和执行风险，适合作为 1.1 release；本 change 只产出未来 UI 可复用的结构化数据。

## Goals / Non-Goals

**Goals:**
- 把发布前实库验证变成硬门禁，而不是发布者记忆。
- 让主程序和 `run_fixup.py` 在长阶段中持续说明“正在做什么、处理到哪里、已经多久”。
- 统一 timeout 语义，避免“进程还活着但用户以为挂死”。
- 让差异和生成 SQL 具备解释链：rule、evidence、decision、action。
- 让 fixup 输出和执行按安全层受控选择。
- 支持阶段级/对象级/文件级恢复，减少大库重跑成本。
- 输出机器可读兼容矩阵，为报告、诊断包和未来 UI 复用。
- 设计并实现独立客户现场诊断包 CLI，降低远程支持来回问日志的成本。

**Non-Goals:**
- 不拆分核心巨型文件为多模块；这属于 1.0 release。
- 不做图形界面、Web 服务或在线 SQL 执行器；这属于 1.1 release。
- 不改变 compare/fixup 的业务语义，除非为了记录解释、安全层或恢复元数据。
- 不把客户诊断包变成全量数据库导出工具；只采集运行证据和必要摘要。

## Decisions

### 1. 发布门禁采用“release evidence 文件 + 脚本化 smoke”

发布流程新增机器可读 evidence，记录版本、commit、tag、命令、结果、报告路径、实库 smoke 范围和未执行项。这样 PR、tag、release note 都可以引用同一份证据。

Alternative considered: 只在 README 增加人工 checklist。拒绝原因是 checklist 很容易被跳过，不能防止重复发生。

### 2. 心跳日志采用统一 Operation Tracker，而不是散落 log.info

主程序与 `run_fixup.py` 都需要统一字段：phase、operation_id、object_type、object_identity、current、total、elapsed_sec、last_success、artifact_path。默认按 `progress_log_interval` 输出，超过 `slow_phase_warning_sec` 或 `slow_sql_warning_sec` 输出 warning。

Alternative considered: 只在几个热点循环补日志。拒绝原因是下一次挂起可能发生在新路径，字段不统一也不利于诊断包解析。

### 3. Timeout 策略采用分层展示，不立即改成一个总开关

保留现有 `cli_timeout`、`obclient_timeout`、`fixup_cli_timeout`、`ob_session_query_timeout_us`，但运行开始必须打印 effective timeout table，并在报告中解释它们分别控制 dbcat 进程、obclient 进程、fixup SQL 进程和 OB session query timeout。默认值可后续调整，但必须先让用户看懂。

Alternative considered: 引入 `timeout_profile=fast|normal|large` 并隐藏细项。暂不采用为主路径，因为现场仍需要精确调参；可以作为向导辅助。

### 4. 差异解释使用结构化 reason record

每条 mismatch/fixup 建议都附带：
- `reason_code`: 稳定枚举，例如 `VARCHAR_BYTE_LENGTH_WINDOW`, `TYPE_LITERAL_MISMATCH`, `NULLABILITY_TIGHTEN`
- `rule_id`: 兼容规则或 compare 规则 ID
- `source_evidence`: 源端字段、DDL 片段或查询来源
- `target_evidence`: 目标端字段、DDL 片段或查询来源
- `decision`: `OK`, `MISMATCH`, `FIXUP`, `REVIEW`, `SUPPRESS`, `MANUAL`
- `action`: 生成 SQL、只报告、跳过、人工确认

这样既能回答“为什么生成 ALTER”，也能为诊断包和未来 UI 提供稳定数据。

责任边界：
- compare 阶段负责构造内存中的 reason record，并把它挂到 mismatch、review、manual、suppressed 或 generated-fixup 候选对象上。
- generate-fixup 阶段只消费这些 reason record 并补充 SQL 文件、statement、safety tier、dependency 等 fixup-specific 字段。
- export-reports 阶段只序列化既有 reason record 到 txt/json/report_db，不重新推导业务差异原因，也不直接驱动 compare 决策。

Alternative considered: 只在 SQL 注释中写说明。拒绝原因是 SQL 注释不适合查询、过滤、报告入库和 UI 复用。

### 5. Fixup 安全层成为生成和执行的共同契约

每个脚本目录和每条 SQL 都归入 `safe`、`review`、`destructive`、`manual`。主程序写 `fixup_plan_<ts>.jsonl` 和文本摘要，`run_fixup.py` 默认只执行 safe/review 中明确允许的 family，destructive/manual 需要显式参数。

`safe` 必须是白名单，不允许实施者按“看起来无破坏”自由扩展。0.9.x 首批 safe 仅包括已有对象的编译类操作，例如 `ALTER ... COMPILE`、PACKAGE/TYPE/VIEW/TRIGGER compile，且必须有完整对象身份和依赖证据。`review` 包含 CREATE missing object、ALTER TABLE ADD/MODIFY COLUMN、索引/约束创建、COMMENT、SYNONYM、GRANT、SEQUENCE restart 等需要人工核对或可能触发锁/重写/权限变化的操作。`destructive` 包含 DROP/TRUNCATE/DROP COLUMN/cleanup extra object/disable/force cleanup 等删除或改变现有行为的操作。`manual` 包含 unsupported、degraded、semi-auto-only family、缺少依赖证据和需要客户补充参数的对象。

Alternative considered: 只靠目录名约定。拒绝原因是目录名已经越来越多，用户无法稳定判断执行风险。

### 6. 恢复能力先做“粗粒度可验证”，再细化

主程序先实现阶段 checkpoint：metadata dump、compare、fixup generation、report export；对象级重放先覆盖 TABLE/VIEW/GRANT/fixup generation 的高价值路径。`run_fixup.py` 复用现有 ledger，扩展到语句级状态和 `--resume-from-report`。

checkpoint 使用双 hash：`decision_config_hash` 只覆盖影响 compare/fixup 决策的配置，例如源/目标 schema、source mode、remap、对象类型开关、兼容规则、过滤规则和 fixup 行为开关；`runtime_config_hash` 覆盖日志级别、输出路径、心跳间隔、诊断包开关等运行展示配置。恢复默认只要求 `decision_config_hash`、code version、input artifact hash 匹配；`runtime_config_hash` 变化允许恢复但必须记录到 recovery manifest。支持 `--force-resume --resume-override-reason` 作为人工支持旁路，但必须打印 changed keys 并记录 override reason。

Alternative considered: 立即做全对象 DAG 级恢复。拒绝原因是复杂度接近 1.0 重构，应避免在当前阶段扩大风险。

### 7. 兼容矩阵采用机器可读 registry

为 source mode、OB version、object family、operation 形成 registry，输出 `compatibility_matrix_<ts>.json` 和文本摘要。registry 作为独立 JSON 资产随工具发布，默认路径类似 `compatibility_registry.json`，配置可覆盖；本 change 不把规则内嵌进主程序，以免 1.0 重构前继续扩大硬编码。报告中只展示 operator 需要的 supported/degraded/manual/unsupported 结论，细节留在 JSON。

Alternative considered: 继续把版本门控逻辑散在代码中。拒绝原因是无法稳定解释“为什么这个对象进入 manual-only family”。

### 8. 诊断包采用独立 CLI，而不是主程序附属模式

诊断包主入口为新独立脚本 `diagnostic_bundle.py`：

```bash
python3 diagnostic_bundle.py --run-dir main_reports/run_<ts> --config config.ini --output diagnostic_bundle_<run_id>.zip
python3 diagnostic_bundle.py --pid <pid> --run-dir main_reports/run_<ts> --config config.ini --hang
```

`schema_diff_reconciler.py` 和 `run_fixup.py` 只负责写 heartbeat/checkpoint/fixup plan 等可采集状态，并在报告或日志中打印推荐诊断命令；它们不承载诊断包打包逻辑。独立 CLI 的原因是客户反馈“主程序挂起”时，诊断命令必须能在另一个进程中运行，不能依赖挂起进程继续执行。

Alternative considered: 在 `schema_diff_reconciler.py` 增加 `--diagnostic-bundle`。拒绝原因是主程序已经过大，且进程挂起时无法依赖它自身生成证据包。

### 9. Release smoke 不新增仓内 tracked 测试 schema

0.9.x release gate 以真实 Oracle->OceanBase smoke evidence 为硬门禁，证据来自受控实库环境和本地忽略的 smoke 配置/fixture。暂不在仓库新增 tracked test schema 初始化脚本，避免和用户要求的“test 文件本地-only”冲突。后续如果需要可在子 change 中把最小 schema 初始化做成明确的本地工具或外部 runbook。

## Customer Diagnostic Package Research

现场诊断包是复杂功能，复杂点不在“打包文件”，而在选择正确证据、保护敏感信息、支持运行中挂起和运行后失败两种场景。

### Artifact inventory

需要采集：
- 版本与代码：tool version、commit、tag、Python 版本、依赖版本、平台信息。
- 配置摘要：`source_db_mode`、schema 列表、对象类型开关、timeout、report/fixup/log 路径、关键功能开关。
- 运行状态：当前 run_id、最后阶段、当前对象/SQL 文件、elapsed、phase duration、heartbeat tail。
- 报告索引：`report_index_<ts>.txt`、主报告摘要、manual actions、runtime degraded、compatibility matrix。
- fixup 计划：`README_FIRST.txt`、`fixup_plan_<ts>.jsonl`、skip summary、errors、ledger 摘要。
- 日志证据：run log tail、最近 warning/error、外部命令 stderr 摘要。
- 可选 report_db 摘要：如果启用 report_db，则导出计数一致性查询结果，而不是 dump 全表。

### Redaction model

必须脱敏：
- 所有 password、dsn 中可能包含的凭据、obclient defaults 临时文件路径内容、token、secret、私钥。
- 默认采集 SQL 文件名、路径、大小、hash、对象身份和摘要，不采集完整 DDL/fixup SQL；如用户显式允许，才带 SQL 文件或 DDL 片段。
- 对象名默认不脱敏，因为对象名是定位差异的必要证据；如客户要求，可启用 `diagnostic_redact_identifiers=true` 生成 hash 映射。

### Run-time vs post-run collection

运行后诊断包可以从 `main_reports`、`fixup_scripts`、`logs` 生成。运行中挂起则需要读取 heartbeat state 文件和进程快照；不能依赖 Python 进程自然结束。

### Package format

建议输出：
- `diagnostic_bundle_<run_id>.zip`
- `manifest.json`: 文件列表、hash、脱敏策略、采集时间
- `summary.txt`: 给支持人员第一眼看的摘要
- `config_sanitized.ini`
- `run_state.json`
- `artifacts/`：报告和日志摘要

### Complexity and rollout

分三步：
1. Post-run bundle：只从已完成或失败 run 的现有文件采集，风险最低。
2. Hang bundle：读取 heartbeat state + process snapshot，解决“不报错、不继续”。
3. Support quality gate：bundle 自检，提示缺少哪些证据，必要时给用户下一条采集命令。

report_db 诊断只导出现有 report_db 的 count/status 摘要和一致性查询结果，不在本 change 新增 dedicated diagnostic views。专用视图属于后续 report_db 查询面优化。

## Risks / Trade-offs

- [Risk] 心跳日志过多影响大库日志可读性 → 默认 interval 保守，支持按阶段聚合，DEBUG 中保留细节。
- [Risk] 结构化 explanation 增加内存和报告体积 → 只对差异、fixup、manual、suppressed 记录，不对所有 OK 对象记录完整证据。
- [Risk] fixup 分层改变用户执行习惯 → 保持现有目录输出，同时新增 tier manifest；默认行为只收紧高风险路径。
- [Risk] 恢复状态与用户手动修改输出目录冲突 → checkpoint 使用 decision/runtime 双 hash；决定性配置不匹配时默认拒绝，运行展示配置变化只记录 warning。
- [Risk] 诊断包泄露敏感信息 → 默认脱敏、默认不包含完整 SQL、manifest 记录 redaction policy，测试覆盖密码/DSN/token 样例。
- [Risk] release gate 增加交付时间 → 以小 schema smoke 为最低门槛，大库全量验证作为 release note 的可选证据。

## Migration Plan

1. Phase A：先落 release gate、real DB smoke evidence、timeout table 和 heartbeat，可快速降低发布和挂起风险。
2. Phase B：再落 difference explanation、VARCHAR/VARCHAR2 窗口约束和 fixup safety tiers，为报告和执行安全打基础。
3. Phase C：然后落 compatibility registry、compatibility matrix export 和 recovery manifest，打通可解释和可恢复。
4. Phase D：最后落独立 `diagnostic_bundle.py`，从 post-run bundle 到 hang bundle 分阶段上线；它依赖 Phase A 的 heartbeat、Phase B 的 fixup plan 和 Phase C 的 recovery/compatibility artifacts。
5. 所有新增开关进入 `schema_diff_reconciler.py` defaults + validation + wizard prompts、`config.ini.template.txt`、`readme_config.txt`。
6. 每一阶段都必须保留现有 CLI 主路径，不要求用户使用 UI。

## Resolved Decisions

- Release smoke：0.9.x 使用受控实库 smoke evidence，不新增 tracked test schema 初始化脚本。
- Diagnostic SQL evidence：默认采集 SQL 文件名/路径/大小/hash/摘要，不采集 SQL 正文；SQL 正文必须显式 opt-in。
- report_db：本 change 只导出现有表的 count/status/consistency 摘要，不新增 dedicated diagnostic views。
- Compatibility matrix：使用随工具发布的独立 JSON registry，主程序加载并输出本次运行的 matrix artifact。
