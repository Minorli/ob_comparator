# 变更日志

本文件记录 OceanBase Comparator Toolkit 的重要变更。

## [0.9.9.3] - 2026-04-14

### 新增
- 新增 `source_db_mode=oceanbase` 与 `[OCEANBASE_SOURCE]`，主程序现在支持 OceanBase Oracle-mode source → OceanBase target 的严格 compare 与 certified fixup family。
- 新增 OceanBase source metadata / dependency / source-side DDL provider 分发层；支持同 endpoint 与跨版本场景，不再把 OceanBase source 硬塞进 Oracle loader。

### 变更
- OB→OB 默认按严格 normalized type compare 处理，`IDENTITY`、`DEFAULT ON NULL`、`INVISIBLE` 改为 capability-gated；source/target version 与 deferred/manual family 会在报告和 fixup 目录中显式标出。
- certified OB source fixup family 扩展到 TABLE、VIEW、PROCEDURE、FUNCTION、PACKAGE、PACKAGE BODY、SYNONYM、SEQUENCE、TRIGGER、TYPE、TYPE BODY、INDEX、CONSTRAINT；未认证 family 继续转 deferred/manual。
- 运行前校验和依赖项按 source mode 分流：`source_db_mode=oceanbase` 下不再为 source-side compare/fixup 强制要求 Oracle Thick / dbcat / JAVA_HOME。

### 修复
- 修复 OB→OB 链路误复用 Oracle-only TABLE rewrite 的问题：不再错误套用 GTT rewrite、OMS exclusion、缺表/缺列 `VARCHAR` 长度膨胀等 Oracle→OB 迁移改写。
- 修复 OB→OB strict compare 的 `type_literal_mismatch` 只输出注释、不生成可执行 `ALTER TABLE ... MODIFY` 的问题。
- 追加 mode-aware 回归保护，防止 Oracle→OB 和 OB→OB 在 TABLE ALTER / fixup 路径上互相回退。

### 文档
- README / `readme_lite.txt` / `readme_config.txt` / `docs/*` 已同步到 `0.9.9.3`，并补充 OB→OB 模式、当前 certified family、deferred/manual 边界和非表触发器限制说明。

## [0.9.9.2] - 2026-04-09

### 变更
- 落地最近一轮综合审计中确认成立的 18 项问题，覆盖主 compare、`run_fixup`、辅助诊断工具和测试基线。
- `scope_integrity_dependency_graph_raw` 改为按需构建，非 scope-integrity 路径不再无条件构建全量依赖图。
- `run_fixup` 的脚本执行语义收紧：单脚本内某条语句若命中 timeout，会停止后续语句执行；普通非 timeout 失败仍保持原有逐语句继续策略。
- `INDEX/CONSTRAINT` 在缺失父 TABLE 映射时改为保守保留源 schema，不再套用 generic schema fallback。
- `SYS_NC` / expression-equivalent 索引 compare 改为 multiplicity-aware 匹配，减少当前已知 `2:1` 假阳性。
- `collect_source_object_stats.py` 现在将 CHECK 纳入 CONSTRAINT 统计，并改用有界百分位计算。
- `expert_swarm.py` 改为失败隔离 + 聚合全部 assistant 消息，Unicode 输出保持原样。
- `init_test.py` 改为安全凭据文件方式，不再把 OB 密码放进命令行参数。

### 修复
- 修复 Oracle Q-quote 字面量中单独一行 `/` 被 `split_sql_statements()` 误判为 PL/SQL 终止符的问题。
- 修复 `run_fixup` 语句级 timeout 后仍继续执行同文件后续 SQL 的问题。
- 修复深依赖链 `topo_sort_nodes()` 递归爆栈风险。
- 修复 grant rewrite 失败时临时文件残留和固定 `.tmp` 名称带来的竞态问题。
- 修复 fallback role 枚举缺少系统角色过滤的问题。
- 修复硬编码版本串/措辞耦合测试导致的非语义性测试脆弱问题。

### 文档
- README / `readme_lite.txt` / `readme_config.txt` / `docs/*` 已同步到 `0.9.9.2`，并补充本版执行安全、compare 保守性和测试基线变化。

## [0.9.9.1] - 2026-04-08

### 变更
- 统一 compare/fixup 统计口径：`检查汇总`、Rich 主报告、`fixup_skip_summary_<ts>.txt`、`trigger_status_report.txt` 与 report_db 的 `DIFF_REPORT_COUNTS / DIFF_REPORT_FIXUP_SKIP` 现统一按同一组 compare/fixup 分层字段表达，不再混用“原始阻断集”和“最终 compare 结果”。
- fixup 入口与报告入口统一收口：父 `TABLE/VIEW` 已缺失、人工处理或不支持时，对应 `INDEX / CONSTRAINT / TRIGGER` 会先从 fixup compare scope 中剔除，保留诊断明细，但不再重复污染 fixup 计划数量。
- TRIGGER patch 统计重命名固定：`compare_missing_total / selected_missing / filtered_missing` 用于 compare+清单层；`missing_total / selected_total / task_total / blocked_total / filtered_total` 用于 fixup 计划层，主报告与客户排障说明可直接对照。

### 修复
- 修复同一轮运行内 `TRIGGER` 在 `0.b 检查汇总`、`fixup_skip_summary` 与 `DIFF_REPORT_FIXUP_SKIP` 之间可能出现数量分叉的问题。
- 修复 Rich 报告“迁移聚焦 / 执行结论 / 扩展对象”面板仍引用旧 blocked 口径，导致主摘要偏大的问题。

### 文档
- README / `readme_config.txt` / `docs/ADVANCED_USAGE.md` 已补充新的 compare/fixup 分层统计定义和触发器生产排障口径。

## [0.9.9.0] - 2026-04-08

### 新增
- 新增 `grant_generation_mode=full|structural`。默认 `full` 保持原有 Oracle 权限镜像逻辑；`structural` 只生成对象创建、编译、跨 schema 依赖闭环所需的最小授权，便于把迁移结构权限与业务访问权限拆开治理。
- 新增 `runtime_degraded_detail_<ts>.txt`。当 `JOB_ACTION` 文本扫描、依赖链导出等保护性逻辑触发时，会把受影响范围与事件类型（`COMPARE` / `ARTIFACT`）单独输出，主报告和 run summary 同步标记 `compare incomplete` 或“仅附件降级”。
- 新增 `sequence_sync_mode=last_number`，可按 Oracle/OB 当前 `LAST_NUMBER` 生成 `fixup_scripts/sequence_restart/`，用于补齐 sequence 当前值。

### 变更
- VIEW 依赖 remap 收敛为“只改数据来源对象”：`TABLE / VIEW / MVIEW / SYNONYM` 继续参与 rewrite；`FUNCTION / PACKAGE / PROCEDURE / SEQUENCE / TYPE` 只保留诊断，不再误改 VIEW DDL，也不再把 Oracle 内置包调用混进 unresolved warning。
- `remap_root_closure` 继续收口：managed mapping 与 discovery-only mapping 分离；同义词依附、反向依赖、`trigger_list` 保留对象会继续进入 closure，但不再污染 operator-facing compare/fixup 范围。
- `safe text fallback` / `JOB_ACTION` 性能保护增强：对超大文本、高扇出候选和单 job 递归深挖加入保护性上限，并在日志中输出更细的 batch/expand 进度。
- `dependency_chains_*.txt` 导出增加大图保护：导出前不再无脑构造全量 source 依赖 pair；体量过大时可提前跳过或截断链路，而不再拖慢主 compare。
- `gtt_table_handling_mode` 正式进入主流程：Oracle GTT 可按普通 TABLE 受管或保留原语义受管；`mview_check_fixup_mode=auto` 继续按 OB 版本动态门控。
- `run_fixup` 并发/重放防护继续强化：同目录执行锁与状态账本保持启用，避免“执行成功但移动失败”后的重复执行。

### 修复
- 规范化默认值与单列 `IS NOT NULL` 语义 compare，降低大小写、系统命名和冗余等价约束带来的噪音。
- VIEW grant replay 补齐 `ORA-01720` 场景：现有 VIEW 在 prerequisite grant 后需要 refresh 的场景可继续闭环。
- report_db 建表/升级逻辑增强：`DIFF_REPORT_BLACKLIST` 等表的 `STATUS/REASON/DETAIL` 过窄列会自动扩容，降低老表结构引发的写库失败。

### 文档
- README / `readme_lite.txt` / `readme_config.txt` / `docs/*` 已同步到 `0.9.9.0`，并补入上次文档更新后新增的授权模式、运行时降级、VIEW rewrite 边界、依赖导出保护与实操提示。

## [0.9.8.9] - 2026-03-27

### 新增
- 黑名单表重纳管正式进入发布版本：当源端黑名单表已在目标端以人工改造后的 TABLE 形式存在时，可恢复进入 compare/fixup，并自动保护黑名单改造列不被写回 Oracle 原始类型/长度/default/nullability。

### 变更
- 触发器兼容边界调整：`INSTEAD OF ... ON VIEW` 已纳入正常 compare/fixup；`DATABASE/SCHEMA` 级事件触发器继续保留为人工处理。
- Oracle 派生表排除补齐：`RUPD$_*`、`SNAP$_*` 与既有 `MLOG$_*` 一样按系统工件从 compare/fixup 中排除，降低物化视图/复制维护噪声。

### 文档
- README / `readme_config.txt` / `readme_lite.txt` / `docs/*` 当前版本号同步到 `0.9.8.9`。

## [0.9.8.8] - 2026-03-13

### 变更
- owner 级授权文件渲染增强：当 `OBJECT_TYPE=TABLE` 时，继续细分 `TABLE_OBJECT_GRANTS` 与 `TABLE_COLUMN_GRANTS`，提升列级授权审核可读性。
- 上述细分为输出层增强，不改变授权语句集合、merge/dedupe 规则、`grants_*` 目录布局与 `run_fixup` 执行语义。

### 文档
- README / `readme_config.txt` / 技术文档当前版本号同步到 `0.9.8.8`。

## [0.9.8.7] - 2026-03-11

### 新增
- `run_fixup.py` 新增并发执行防护：同一 `fixup_dir` 下使用 `.run_fixup.lock` 防止重复并发执行。
- `run_fixup.py` 新增状态账本：`.fixup_state_ledger.json` 记录已执行脚本指纹，避免“执行成功但移动失败”后的重复执行。
- `table_data_presence_check=auto` 增强二次探针：对 `NUM_ROWS=0` 的源/目标表进行回表确认，降低统计信息滞后带来的误判。
- 新增 `table_data_presence_zero_probe_workers`（默认 1，最大 32），用于控制 Oracle 零行二次探针并发度。

### 变更
- `run_fixup.py` 迭代模式每轮自动清理 auto-grant 阻断缓存，避免临时阻断跨轮遗留。
- VIEW DDL 清洗由正则前缀匹配改为 token 扫描，降低注释/字符串命中导致的误清洗风险。
- `SqlPunctuationMasker` 补齐 Q-quote 掩码路径，标点清洗不再误伤 Q-quote 字面量。
- `mask_sql_for_scan` 不再掩码双引号标识符，降低列清单与别名解析偏差。
- `blacklist_rules.json` 在关键路径采用严格解析（fail-fast），解析失败会终止流程，防止规则失效后静默放行。
- `report_sql_<ts>.txt` 改为轻量入口文件，仅写入 `report_id` 与 HOW TO 手册入口，不再内嵌 HOW TO 正文。
- HOW TO 手册与主程序运行期解耦：主程序只提示客户去读 HOW TO，不再把 HOW TO 内容编进主流程输出。
- 运行期导航增强：主报告增加“本次建议处理顺序”，`fixup_scripts/README_FIRST.txt` 作为 fixup 根目录导航。
- 触发器字符串字面量 remap 收紧：仅自动改写完整匹配 `SCHEMA.OBJECT` 的单引号字面量；`SCHEMA.OBJECT.COLUMN` 保守保留并输出提醒明细。
- DDL 清洗治理改为证据门禁：`AUTONOMOUS_TRANSACTION`、`SERIALLY_REUSABLE`、`STORAGE`、`TABLESPACE` 默认保留，类型改写单独标记为 `semantic_rewrite`。

### 修复
- `run_fixup.py` 备份失败后的文件移动行为收敛，避免覆盖既有 `done/` 脚本引发审计信息丢失。
- `run_fixup.py` 重编译成功判定增强：不再仅依赖进程返回码，改为编译后再次校验 INVALID 状态。
- `clean_storage_clauses` 对引号 TABLESPACE 名称的处理增强，降低清洗漏网。
- `run_fixup.py` 错误识别补齐 `OBE-* / PLS-* / SP2-*`，`fixup_errors_*.txt` 更容易命中首错。
- `PUBLIC` 额外授权回收审计修复：源端已声明但因兼容性过滤的授权，不再误生成 `REVOKE_PUBLIC`。

### 文档
- README / readme_lite / readme_config / docs 主文档统一同步到当前实现；旧版 release note / version diff / 规划草稿从 `docs/` 目录清理。

## [0.9.8.6] - 2026-02-27

### 新增
- 授权延后机制：当目标对象当前不存在且本轮不会创建时，授权从 `grants_miss` 分流并标记为 `DEFERRED_TARGET_MISSING_NOT_PLANNED`。
- 新增 `deferred_grants_detail_<ts>.txt`，用于明确后续需补执行的授权清单。
- 新增 `fixup_scripts/grants_deferred/README.txt` 兜底提醒（即使 deferred SQL 未生成也会保留执行指引）。

### 变更
- `run_fixup.py` 默认将 `grants_deferred` 纳入安全跳过目录；对象补齐后需显式 `--only-dirs grants_deferred` 执行。
- `run_fixup.py` 执行层级新增 `name_collision` 前置层，约束/索引同名修复先于 `constraint/index` 执行。
- 同名约束修复按 OB 版本门控：低版本自动回退 `DROP + ADD`，避免 `RENAME CONSTRAINT` 兼容问题。

### 修复
- 黑名单依赖阻断口径优化：`LONG/LOB_OVERSIZE` 作为风险项不再一刀切阻断依赖对象检查与修补。
- 默认排除 `MLOG$_*` 物化视图日志表（EXCLUDED），不参与缺失/不一致校验与 fixup 生成，减少系统派生噪声。

## [0.9.8.5] - 2026-02-13

### 新增
- `run_fixup.py` 新增 `--version` 输出，便于现场快速确认执行器版本。
- 新增 `prod_diagnose.py`（只读生产诊断器）：支持报告口径验真、实库复核、fixup 失败归因与 OCR 友好输出。
- `prod_diagnose.py` 新增 `--focus-object` + `--deep`，支持单对象全链路深挖。

### 变更
- 版本号改回脚本内置常量，移除对 `tool_version.py` 的运行时依赖，兼容“仅替换单个脚本”的历史分发方式。
- README / readme_config / ARCHITECTURE / ADVANCED_USAGE / TECHNICAL_SPECIFICATION / DEPLOYMENT 同步到 `0.9.8.5`。

## [0.9.8.4] - 2026-02-10

### 新增
- 交付前正确性基线文档化：统一补充 `py_compile` + `unittest` + 可选联调命令。
- report_to_db 增强：`report_db_store_scope=full` 下新增 `DIFF_REPORT_ARTIFACT_LINE`，run 目录 txt 报告逐行入库，支持数据库侧 100% 文本复盘。

### 修复
- TRIGGER 扩展校验修复：按 `OWNER.TRIGGER_NAME` 比较，避免同表跨 schema 同名触发器误报 `EXTRA_TRIGGER`。
- CONSTRAINT 扩展校验降噪：在签名层与对比层双重忽略 OceanBase 自动 `*_OBNOTNULL_*` 约束，减少 `NOT NULL` 命名差异噪声。

### 变更
- README / readme_config / ARCHITECTURE / ADVANCED_USAGE / TECHNICAL_SPECIFICATION / DEPLOYMENT 同步到当前实现并升级至 `0.9.8.4`。
- 修正文档中过期描述：`init_users_roles.py` 已为交互输入初始口令，不再写死默认口令。

## [0.9.8.3] - 2026-02-05
- report_to_db 覆盖范围扩展（支持 full 模式更多表）
- 新增明细行化表 DIFF_REPORT_DETAIL_ITEM，细粒度差异入库
- 新增 report_db_store_scope 与 report_db_detail_item_enable 配置
- 新增报告分析视图（ACTIONS / OBJECT_PROFILE / TRENDS / PENDING_ACTIONS / GRANT_CLASS / USABILITY_CLASS）
- 新增写库失败追踪与闭环表（WRITE_ERRORS / RESOLUTION）
- HOW_TO_READ_REPORTS_IN_OB_latest.txt SQL 校验修正与完善（历史快照按时间戳文件保留）

## [未发布]
- 暂无。

## [0.9.8.2] - 2026-02-03

### 新增
- VIEW/SYNONYM 可用性校验（可选）：目标端 `SELECT * FROM <obj> WHERE 1=2` 验证可用性，支持源端对照/超时/并发/抽样控制，并输出明细报告。
- VIEW 兼容规则增强：识别 X$ 系统对象引用并判定不支持（用户自建 X$ 对象除外）。
- VIEW 修补授权拆分：新增 `view_prereq_grants/`（依赖对象前置授权）与 `view_post_grants/`（创建后授权）。

### 变更
- VIEW DDL 清洗移除 `FORCE` 关键字，避免创建不可用视图。
- run_fixup 顺序加入 view_prereq_grants / view_post_grants。
- PUBLIC/`__public` 语义统一：报告与比对以 PUBLIC 展示。
- FK 约束比对补齐 `UPDATE_RULE`。
- obclient SQL 改为 stdin 传入，特殊字符更稳定。
- 致命错误收敛为异常链路，避免并发任务直接 `sys.exit`。

## [0.9.8.1] - 2026-01-29

### 新增
- SQLcl DDL 格式化：可选启用对 fixup 输出进行格式化，支持多类型与批量控制。
- DDL 格式化报告：新增 `ddl_format_report_<timestamp>.txt`，汇总格式化结果与跳过原因。
- SYS_C* 额外列清理：支持 `fixup_drop_sys_c_columns=true` 生成 `ALTER TABLE ... FORCE`。

### 变更
- DDL 格式化报告使用 `|` 分隔，便于直接导入 Excel。
- report 目录支持 per_run 布局，默认输出到 `main_reports/run_<timestamp>/`。
- collect_source_object_stats 统计 SQL 模板统一，减少简版/全量输出偏差风险。
- config.ini.template 去重并新增重复键检测测试。

### 修复
- 修复 run_fixup 迭代执行累计失败统计不准确的问题。
- 修复 VIEW remap 时表别名被误替换为 `SCHEMA.ALIAS` 的问题。
- 修复 OB 侧 CHAR_USED 缺失导致 VARCHAR 语义误判的问题（默认 BYTE，并结合 DATA_LENGTH/CHAR_LENGTH 推断）。

## [0.9.8] - 2026-01-09

### 新增
- 触发器状态对比：新增 VALID/INVALID + ENABLED/DISABLED 差异输出，统一到 trigger_status_report.txt。
- 不支持/阻断对象分类：缺失对象新增支持状态统计与明细输出（unsupported_objects_detail_*）。
- VIEW 兼容规则：识别 SYS.OBJ$、DBLINK、缺失系统视图等场景并给出原因。
- 报告拆分模式：新增 report_detail_mode=full/split/summary 控制主报告体积与明细输出。
- Fixup 分流目录：tables_unsupported/ 与 unsupported/ 输出不支持对象 DDL（默认不执行）。

### 变更
- OMS 缺失规则输出自动排除不支持/阻断的 TABLE/VIEW。
- 检查汇总新增“不支持/阻断”列，主报告更易定位需改造对象。
- run_fixup 默认跳过 tables_unsupported/ 与 unsupported/ 目录。

### 修复
- 触发器状态对比补充 object_statuses，避免 INVALID 触发器漏报。

### 归档
- 扩展对象并发校验开关、synonym_fixup_scope、report_dir_layout、sequence_remap_policy、trigger_qualify_schema、missed_tables_views_for_OMS 与 fixup 跳过汇总在本版本统一整理归档。

## [0.9.7] - 2026-01-08

### 新增
- VIEW 链路自动修复：基于 `VIEWs_chain_*.txt` 生成每个 VIEW 的计划与 SQL 并自动执行。
- VIEW 依赖链报告：缺失 VIEW 输出依赖链、对象存在性与授权状态。
- 授权修剪与错误报告：授权脚本逐行执行，成功行自动移除，错误汇总输出到 `fixup_scripts/errors/`。
- 授权缺失输出：`fixup_scripts/grants_miss/` + `fixup_scripts/grants_all/` 分离缺失与全量授权。
- GRANT 目标存在性过滤：目标端不存在的用户/角色授权会被跳过并提示。

### 变更
- VIEW DDL 来源切换为 DBMS_METADATA；dbcat 不再导出 VIEW。
- VIEW DDL 清理保留 `FORCE/NO FORCE`，移除 Oracle-only 修饰，`WITH CHECK OPTION` 仅在 OB >= 4.2.5.7 保留。
- run_fixup 语句级执行并继续失败语句，默认超时提升到小时级并支持禁用超时。
- 授权推导支持 VIEW/同义词链路的 `WITH GRANT OPTION` 补齐。

### 修复
- VIEW DDL 行内注释吞行与被拆分列名的修复逻辑更稳健。

## [0.9.6] - 2026-01-07

### 新增
- `run_fixup.py` 支持迭代执行模式（`--iterative/--max-rounds/--min-progress`），自动重试依赖失败脚本并输出错误分类建议。
- VIEW 缺失修补脚本生成前加入简单拓扑排序，提升依赖场景成功率。

### 变更
- PUBLIC 同义词 DDL 生成去除冗余 `ALTER SESSION`。
- FK 引用目标的 remap 校验逻辑优化，确保源端引用按 remap 规则对齐。
- SYS_NC 索引列名标准化覆盖更多 Oracle 自动命名形式。

### 修复
- 序列元数据缺失场景不再因错误字段引用导致崩溃。
- 触发器比较逻辑修复：源端无触发器时可正确识别目标端多余触发器。

## [0.9.5] - 2026-01-07

### 新增
- Run summary section appended to the report with total/phase runtimes, executed vs skipped actions, key findings, attention items, and next-step suggestions.
- End-of-run structured summary in runtime logs.
- Project homepage and issue tracker links in report output and CLI help/startup logs.
- 支持 trigger_list 过滤缺失触发器脚本，仅生成清单内的 TRIGGER，并输出 trigger_miss.txt 记录无效/不存在条目。

### 变更
- Trigger list fallback now generates full trigger fixups when the list is missing, unreadable, or empty.
- Log output formatting unified for clearer phase-based progress and summaries.

## [0.9.4] - 2026-01-07

### 变更
- 缺失 VIEW 的 fixup DDL 优先使用 dbcat 导出，dbcat 未命中时才使用 DBMS_METADATA 兜底。
- 黑名单表清单新增 LONG/LONG RAW 转换校验状态输出，区分已校验与缺失/类型不匹配。

## [0.9.3] - 2026-01-06

### 新增
- 支持读取 `OMS_USER.TMP_BLACK_TABLE`，输出黑名单缺失表清单 `main_reports/blacklist_tables.txt`（按 schema 分组、附原因）。
- `tables_views_miss/` 输出自动排除黑名单表，确保规则可直接供 OMS 使用。

### 变更
- 检查汇总增加黑名单缺失 TABLE 单独统计，`TABLE` 缺失计数仅统计支持迁移的表。
- 缺失列补齐时将 `LONG/LONG RAW` 自动映射为 `CLOB/BLOB`，避免错误修补。

## [0.9.2] - 2026-01-05

### 变更
- 触发器/视图默认保持源 schema（仅显式 remap 才改变），触发器脚本附带跨 schema 授权。
- `check_primary_types`/`check_extra_types` 贯穿 remap 推导、依赖校验与元数据加载范围。
- MATERIALIZED VIEW 默认仅打印不校验，PACKAGE/PACKAGE BODY 纳入有效性对比并支持缺失修补 DDL。
- 无法自动推导的对象会单独汇总并在报告中提示，避免误回退。

## [0.9.0] - 2025-12-23

### 安全与可靠性
- **DDL 重写引擎重构**: 引入 `SqlMasker`，彻底解决正则替换时误伤字符串/注释的风险。
- **视图依赖解析升级**: 新增 Token 级解析器，完美支持 `FROM A, B` 等复杂 SQL 语法的表依赖提取。
- **PL/SQL 智能推导**: 增强 `remap_plsql_object_references`，支持本地未限定引用的自动 Schema 补全。

### 文档
- **文档重构**: 整合分散的 markdown 文档为 `ADVANCED_USAGE.md`、`DEPLOYMENT.md`、`ARCHITECTURE.md`。

## [0.8.8] - 2025-12-11

### 变更
- 全局版本号更新为 `0.8.8`，README/DESIGN/发布文档同步。
- PUBLIC 同义词默认用元数据批量获取，过滤仅保留指向 `source_schemas` 的目标，FOR 子句按 remap 重写并清理 NONEDITIONABLE。
- 触发器 DDL 的 ON 子句使用 remap 后的表名，修补脚本文件名与内容保持一致。
- PL/SQL fixup 清理补充：移除紧邻 `/` 之前的单独分号行，避免多余 `;/` 组合。

### 修复
- OceanBase 索引列获取不再虚构 `UNKNOWN` 索引：只有 DBA_INDEXES 中存在的索引才追加列，防止误报唯一性/缺失。
- PUBLIC 同义词 DDL 生成不会给对象名重复加 `PUBLIC.` 前缀。

## [0.8.1] - 2025-12-10

### 修复
- **修复 "too many values to unpack (expected 2)" 错误**：
  - 问题：生产环境中对象名包含多个点号（如 `SCHEMA.PACKAGE.PROCEDURE`）导致 `split('.')` 返回超过2个元素
  - 影响：VIEW 处理和 OTHER_OBJECTS 任务在大量对象时崩溃
  - 修复方案：
    - ✅ 所有 `split('.')` 改为 `split('.', 1)` 确保只分割成2部分
    - ✅ 添加 `try-except ValueError` 防御性代码捕获异常
    - ✅ 添加长度检查 `if len(parts) != 2` 作为额外保护
  - 涉及位置：
    - `get_relevant_replacements()` 函数（第5938-5939行）
    - 缺失对象处理（第6135-6136行）
    - Schema映射处理（第1490-1491行、第1547行）
    - 依赖对象处理（第6904-6907行）
    - 表对比处理（第2860-2866行、第3402-3408行、第3535-3541行）

- **修复 OceanBase 版本检测失败**：
  - 问题：使用 `SELECT VERSION()` 在 OceanBase Oracle 模式下报错
  - 影响：无法获取版本号，导致 VIEW DDL 清理策略失效
  - 修复：改用 `SELECT OB_VERSION() FROM DUAL` 并修正版本号解析逻辑
  - 涉及函数：`get_oceanbase_version()`, `get_oceanbase_info()`

- **移除不必要的元数据缺失警告**：
  - 问题：当源端 Oracle 表无索引/约束元数据时，打印大量警告信息
  - 影响：日志噪音过多，影响问题定位
  - 修复：移除 "源端 Oracle 该表无索引元数据" 和 "源端 Oracle 该表无约束元数据" 警告
  - 原因：这些情况是正常的（表可能确实没有索引/约束），不应作为警告

- **修复同名索引但 SYS_NC 列名不同的误报**：
  - 问题：Oracle 和 OceanBase 对隐藏列命名不同（如 `SYS_NC00023$` vs `SYS_NC38$`）
  - 影响：同一个索引被同时报告为"缺失"和"多余"
  - 修复：添加 SYS_NC 列名标准化逻辑，识别同名索引并从告警中剔除
  - 实现：
    - `normalize_sys_nc_columns()`: 将 SYS_NC 列标准化为通用形式
    - `has_same_named_index()`: 检查是否存在同名索引
    - `is_sys_nc_only_diff()`: 检查是否仅 SYS_NC 列名不同

- **增强 OMS 索引过滤逻辑**：
  - 问题：原逻辑要求索引列精确匹配4个 OMS 列，过于严格
  - 影响：包含额外业务列的 OMS 索引无法被正确识别
  - 修复：改为检查索引名以 `_OMS_ROWID` 结尾且包含所有4个 OMS 列作为子集
  - 函数：`is_oms_index()`

- **修复 `non_view_missing_objects` 变量作用域错误**：
  - 问题：变量在使用前未定义，导致 `UnboundLocalError`
  - 影响：修补脚本生成失败
  - 修复：将 VIEW/非VIEW 对象分离逻辑移到使用前执行

### 新增
- **防御性错误处理**：
  - 在所有 `fetch_ddl_with_timing()` 调用处添加返回值长度检查
  - 在所有 `split()` 操作处添加 `try-except ValueError` 保护
  - 详细记录异常信息便于问题定位

- **IOT 表过滤**：
  - 自动跳过 `SYS_IOT_OVER_*` 开头的 IOT 溢出表
  - 避免这些系统表参与对比和修补脚本生成
  - 在日志中统计跳过的 IOT 表数量

- **注释标准化增强**：
  - 去除控制字符（`\x00-\x1f\x7f`）
  - 识别并过滤 `NULL`/`<NULL>`/`NONE` 等无效注释
  - 函数：`normalize_comment_text()`

### 变更
- **并发处理优化**：
  - 添加 `fixup_workers` 配置项，默认使用 CPU 核心数（最多12）
  - 添加 `progress_log_interval` 配置项，控制进度日志输出频率
  - 使用 `ThreadPoolExecutor` 并发生成修补脚本

- **报告宽度配置**：
  - 添加 `report_width` 配置项（默认160），避免 nohup 时被截断为80列
  - 确保报告在后台运行时也能完整显示

### 技术细节
- 版本号更新为 `0.8.1`
- 添加 `__version__` 和 `__author__` 元数据
- 导入 `threading`, `json`, `time`, `concurrent.futures` 模块
- 添加 `DBCAT_DIR_TO_TYPE` 反向映射字典

---

## [0.8.5] - 2025-12-09

### 变更
- **重构对象推导逻辑，完全依赖DBA_DEPENDENCIES**：
  - 问题：v0.8.4的 `get_object_parent_tables()` 方法仍然有局限性，只处理特定对象类型
  - 根本原因：所有对象（VIEW/PROCEDURE/FUNCTION/PACKAGE/TRIGGER/SYNONYM等）的DDL中都可能引用表
  - 解决方案：废弃 `object_parent_map`，完全依赖 `DBA_DEPENDENCIES` 进行推导
  - 优势：
    - ✅ 覆盖所有对象类型（不再有遗漏）
    - ✅ 基于实际的依赖关系（更准确）
    - ✅ 代码更简洁（减少冗余逻辑）

### 废弃
- **`get_object_parent_tables()` 函数已废弃**：
  - 保留函数签名用于向后兼容
  - 实际返回空字典，不再执行查询
  - 推导逻辑现在完全依赖 `infer_target_schema_from_dependencies()`

### 技术细节
- 简化 `resolve_remap_target()` 函数：
  - 移除对 `object_parent_map` 的依赖
  - 推导顺序：显式remap规则 → 依赖分析推导 → schema映射推导
  - 依赖分析推导现在应用于所有非TABLE对象
- `DBA_DEPENDENCIES` 已经包含了所有对象对表的引用关系
- DDL中的表名替换由 `adjust_ddl_for_object()` 统一处理

### 收益
- **更全面的覆盖**：所有对象类型都能正确推导，包括：
  - VIEW（查询中的表）
  - PROCEDURE/FUNCTION/PACKAGE（代码中的表）
  - TRIGGER（触发的表和代码中的表）
  - SYNONYM（指向的对象）
  - TYPE/TYPE BODY（可能引用的表）
- **更准确的推导**：基于Oracle的依赖关系元数据，而不是手工查询
- **更简洁的代码**：减少了100+行冗余代码

---

## [0.8.4] - 2025-12-09

### 修复
- **依附对象父表映射不完整**：
  - 问题：`get_object_parent_tables()` 只处理TRIGGER，导致INDEX/CONSTRAINT/SEQUENCE无法跟随父表的remap
  - 影响：在一对多场景下（如 MONSTER_A → TITAN_A + TITAN_B），这些对象无法正确推导目标schema
  - 修复：扩展函数以处理所有依附对象：
    - ✅ TRIGGER: 通过 DBA_TRIGGERS 查询父表
    - ✅ INDEX: 通过 DBA_INDEXES 查询父表
    - ✅ CONSTRAINT: 通过 DBA_CONSTRAINTS 查询父表
    - ✅ SEQUENCE: 通过分析触发器代码中的 .NEXTVAL 引用推断父表

### 增强
- **SEQUENCE智能推导**：
  - 分析触发器代码中的序列使用（如 SEQ_NAME.NEXTVAL）
  - 将序列关联到使用它的表
  - 支持带schema前缀和不带schema前缀的序列引用

### 技术细节
- 修改 `get_object_parent_tables()` 函数：
  - 添加 DBA_INDEXES 查询获取索引的父表
  - 添加 DBA_CONSTRAINTS 查询获取约束的父表
  - 添加触发器代码分析获取序列的父表
  - 使用正则表达式匹配 `SCHEMA.SEQ_NAME.NEXTVAL` 模式

---

## [0.8.3] - 2025-12-09

### 修复
- **约束和索引统计错误**：
  - 问题：检查汇总中显示Oracle有63个约束，OceanBase有97个约束，数量不匹配
  - 原因：统计时Oracle只统计remap规则中涉及的表的约束，而OceanBase统计了目标schema下所有表的约束
  - 修复：修改 `compute_object_counts()` 函数，确保两端都只统计remap规则中涉及的表的约束和索引
  - 影响：INDEX统计也应用了相同的修复逻辑

### 技术细节
- 修改 `compute_object_counts()` 函数：
  - 从 `full_object_mapping` 中提取所有涉及TABLE的源表和目标表
  - 统计约束/索引时，只统计这些表的约束/索引
  - 确保Oracle和OceanBase使用相同的过滤逻辑

---

## [0.8.2] - 2025-12-09

### 新增
- **索引和约束命名冲突检测与自动解决**：
  - 自动检测同一目标schema下的索引/约束名称冲突
  - **智能识别重命名表**：
    - 自动检测目标端的重命名表（如 `ORDERS_RENAME_20251118`）
    - 识别其索引/约束与即将创建的原表名冲突
    - 支持多种重命名模式：RENAME/BACKUP/BAK/OLD/HIST/HISTORY/ARCHIVE/ARC/TMP/TEMP
    - 支持有无下划线的格式（`_RENAME_20251118` 或 `_RENAME20251118`）
    - 支持多种日期格式：YYYYMMDD(8位)、YYMMDD(6位)、YYMM(4位)
  - 智能重命名策略：
    - 优先提取表名中的重命名后缀（关键词+日期）
    - 否则使用表名后缀作为区分标识
    - 确保新名称不超过30字符限制
    - 如仍冲突则添加数字后缀
  - 应用场景：
    - 多个源schema的不同表remap到同一目标schema
    - 表被重命名后原表重建（**核心场景**）
    - 目标端已存在同名索引/约束
  - 自动重命名CREATE TABLE中内联的约束
  - 自动重命名独立的CREATE INDEX和ALTER TABLE ADD CONSTRAINT语句
  - 详细日志输出冲突检测和重命名信息

### 变更
- **修补脚本生成增强**：
  - 在生成INDEX/CONSTRAINT脚本前执行冲突检测
  - 为冲突对象生成带重命名标记的脚本文件
  - 脚本头部注释说明原名和重命名原因
  - 特别标注"来自重命名表"的冲突

### 技术细节
- 新增 `extract_table_suffix_for_renaming()` 函数：提取表名中的重命名后缀，支持多种模式
- 增强 `detect_naming_conflicts()` 函数：
  - 识别重命名表并提取原始表名
  - 检测重命名表的索引/约束与即将创建的表冲突
  - 输出详细的冲突来源信息
- 增强 `generate_conflict_free_name()` 函数：使用提取的重命名后缀生成新名称
- 新增 `rename_embedded_constraints_indexes()` 函数：重命名CREATE TABLE DDL中的内联约束/索引
- 修改INDEX和CONSTRAINT生成逻辑，应用重命名映射
- 修改TABLE生成逻辑，处理内联约束/索引的重命名

---

## [0.8.1] - 2025-12-09

### 修复
- **性能问题**：修复fixup脚本生成阶段显示错误的耗时
  - 问题：每个对象显示20-30秒，但实际已从缓存加载
  - 原因：dbcat批次总耗时被记录到每个对象
  - 修复：将批次总耗时平均分配给批次中的每个对象
  
### 变更
- **日志优化**：
  - 缓存加载时使用实际读取耗时（通常<0.01秒）
  - 只在非缓存或耗时较长（>0.1秒）时输出详细日志
  - 减少日志噪音，提升可读性

### 新增
- **性能调优文档**：新增 `PERFORMANCE_TUNING.md`
  - 详细说明性能问题的根本原因
  - 提供完整的性能调优建议
  - 包含故障排查和最佳实践

### 技术细节
- 修改 `_run_dbcat_chunk()` 函数，计算平均耗时
- 修改 `fetch_ddl_with_timing()` 函数，区分缓存和运行耗时
- 优化日志输出条件

---

## [0.8] - 2025-12-09

### 新增
- **智能Schema推导**：基于依赖分析的一对多映射自动推导
  - 分析对象引用的表，选择出现次数最多的目标schema
  - 支持VIEW/PROCEDURE/FUNCTION/PACKAGE等独立对象的智能推导
  - 输出详细的推导日志 `[推导]`
  
- **多对一映射序列比对修复**：
  - 修复了多对一场景下错误报告"多余序列"的bug
  - 正确处理多个源schema映射到同一目标schema的情况

- **DDL对象名替换增强**：
  - 修复了PACKAGE/PROCEDURE等对象的END语句名称替换
  - 正确处理主对象的裸名引用（不带schema前缀）

- **一对多场景警告**：
  - 自动检测一对多映射场景并输出警告
  - 提示用户哪些对象需要显式配置

### 变更
- **Remap推导逻辑优化**：
  - 优先级：显式规则 > 依赖分析 > schema映射
  - 支持所有映射场景（多对一、一对一、一对多）

- **文档完善**：
  - 新增 `REMAP_INFERENCE_GUIDE.md` 详细说明推导能力
  - 更新 README.md 反映最新功能
  - 新增 `AUDIT_REPORT.md` 程序一致性审核报告

### 修复
- 修复序列比对在多对一映射场景下的误报
- 修复DDL中主对象名的裸名替换问题
- 修复依赖关系字段名错误（`type` → `object_type`）

### 技术细节
- 新增 `infer_target_schema_from_dependencies()` 函数
- 更新 `resolve_remap_target()` 支持依赖分析参数
- 更新 `build_schema_mapping()` 输出一对多警告
- 更新 `adjust_ddl_for_object()` 处理主对象裸名

---

## [0.7] - 2025-12-08

### 新增
- 表/列注释一致性检查
- 依赖关系校验和重编译脚本生成
- 跨schema授权建议（GRANT脚本）
- TABLE列长度校验和ALTER修补

### 变更
- 采用"一次转储，本地对比"架构
- 批量查询DBA_*视图，避免循环调用
- dbcat缓存复用机制

### 修复
- 性能优化：减少数据库调用次数
- 内存优化：本地数据结构对比

---

## [0.6] - 2025-12-07

### 新增
- 基础对象类型检测（TABLE/VIEW/PROCEDURE/FUNCTION等）
- Remap规则验证
- 基础修补脚本生成

### 变更
- 初始版本架构设计

---

## Future Roadmap

### Planned for 0.9
- [ ] 性能监控和统计
- [ ] 更详细的差异报告（列类型、默认值等）
- [ ] 支持更多数据库对象类型（DIRECTORY、DB_LINK等）
- [ ] 交互式修补脚本审核工具

### Planned for 1.0
- [ ] GUI界面
- [ ] 配置模板管理
- [ ] 批量场景支持
- [ ] 完整的单元测试覆盖

---

## Version Numbering

版本号格式：`MAJOR.MINOR`

- **MAJOR**: 重大架构变更或不兼容更新
- **MINOR**: 新功能添加或重要bug修复
- nested table storage 兼容收口：Oracle `DBA_NESTED_TABLES` 命中的 storage table 不再被当作普通可修补 TABLE；主报告归入“不支持/阻断/待确认”，fixup 改走 unsupported/manual 路径，并显式标记 `NESTED_TABLE_STORAGE`。
- TABLE fixup 统计分层补齐：`fixup_skip_summary_<ts>.txt`、主报告和 report_db 现在也会对 TABLE 显示 `compare / selected / runnable / generated / filtered / blocked / generation_failed`，避免“检查汇总一个数、TABLE 脚本目录另一个数”。
- 默认关闭 report_db：`report_to_db` 的默认值从 `true` 调整为 `false`，本地文本报告继续默认生成；需要数据库侧落库时再显式开启，避免大批量运行被 report_db 入库拖慢。
