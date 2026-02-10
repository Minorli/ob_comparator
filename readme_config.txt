配置说明 (config.ini)
版本：0.9.8.3（更新日期：2026-02-09）
本文件为完整配置说明书，覆盖所有可配置项（含最近新增功能）。

通用约定
- 布尔值：true/false/1/0/yes/no（大小写不敏感）。
- 列表值：逗号分隔，空格会被忽略，内部统一转大写。
- 空值：表示使用内置默认值或关闭该功能（具体见各项说明）。
- 路径：支持相对路径与绝对路径；相对路径以当前工作目录为基准。

1) 配置段 (Sections)
- [ORACLE_SOURCE]：源端 Oracle 连接。
- [OCEANBASE_TARGET]：目标端 OceanBase 连接（obclient）。
- [SETTINGS]：对比范围、修补生成、dbcat、黑名单与性能开关等。

2) [ORACLE_SOURCE]
- user：Oracle 用户名（必填，无默认）。
- password：Oracle 密码（必填，无默认）。
- dsn：Oracle DSN，格式 host:port/service_name（必填）。

3) [OCEANBASE_TARGET]
- executable：obclient 可执行文件路径（必填）。
- host：OceanBase 主机地址（必填）。
- port：OceanBase 端口（必填）。
- user_string：完整的 obclient -u 参数（必填）。
- password：OceanBase 密码（必填）。

4) [SETTINGS]

核心与映射
- source_schemas：源端 schema 列表（必填）。默认：无；为空将直接退出。
  说明：逗号分隔，大小写不敏感；只扫描这些 schema 的对象与依赖。
- remap_file：Remap 规则文件路径。默认：空（按 1:1 映射）。
  说明：规则格式为 `SRC_SCHEMA.OBJECT = TGT_SCHEMA.OBJECT`，支持注释与空行。
  注意：文件不存在会报警但继续。

输出与日志
- report_dir：主报告输出目录。默认：main_reports。
- report_dir_layout：报告目录布局。默认：per_run。
  可选值：flat（输出到 report_dir 根目录）、per_run（输出到 report_dir/run_<timestamp>）。
- report_detail_mode：报告内容模式。默认：split。
  可选值：full（主报告包含全部明细）、split（主报告仅概要，细节拆分为 *_detail_*.txt）、summary（仅概要，不生成细节文件）。
  说明：split 模式下的明细文件采用 `|` 分隔并包含 `# total/# 字段说明` 头，格式与 package_compare 相同，便于 Excel 直接分隔导入。
  说明：若存在不支持的约束（如 DEFERRABLE / CHECK SYS_CONTEXT / 自引用外键），会额外输出 constraints_unsupported_detail_<timestamp>.txt（不受 report_detail_mode 影响）。
- report_to_db：是否将报告存储到 OceanBase（obclient 方式）。默认：true。
  说明：开启后仍会保留本地文本报告，写库失败时是否中断由 report_db_fail_abort 控制。
  说明：开启后会在 run 目录输出 report_sql_<timestamp>.txt（预填 report_id 的 SQL 模板），并尝试创建只读分析视图（actions/profile/trends/pending/grant/usability）。
  说明：当 report_db_store_scope=full 时，会将 run 目录下所有 txt 逐行写入 DIFF_REPORT_ARTIFACT_LINE，实现 txt 内容 100% 可查询覆盖。
- report_db_schema：报告存库 schema。默认：空（使用 OCEANBASE_TARGET 连接用户）。
- report_retention_days：报告保留天数。默认：90；设为 0 表示不自动清理。
- report_db_fail_abort：报告写库失败是否中止主流程。默认：false。
- report_db_store_scope：写库范围（summary/core/full）。默认：full。
  - summary：仅写入汇总与计数。
  - core：写入汇总/计数/明细/授权/可用性/Package/Trigger。
  - full：写入全部（含依赖/VIEW 链/映射/黑名单/fixup/OMS 规则/工件目录）。
- report_db_detail_mode：写入明细范围。默认：missing,mismatched,unsupported。
  可选值：missing,mismatched,unsupported,ok,skipped,all（all 等同全量）。
  说明：建议仅保存关键明细，避免海量写库压力。
- report_db_detail_max_rows：写入明细最大行数。默认：0（不限制）。
- report_db_detail_item_enable：是否写入明细行化表 DIFF_REPORT_DETAIL_ITEM。默认：空（full 时自动启用）。
- report_db_detail_item_max_rows：明细行化表最大行数。默认：0（不限制）。
- report_db_insert_batch：写库批量大小（INSERT ALL）。默认：200。
- report_db_save_full_json：是否保存完整报告 JSON。默认：false。
  说明：开启后会写入主报告 JSON（可能较大，影响性能）。
  说明：report_to_db 写入表范围受 report_db_store_scope 控制：
  - summary: DIFF_REPORT_SUMMARY / DIFF_REPORT_COUNTS
  - core: summary + DIFF_REPORT_DETAIL / DIFF_REPORT_GRANT / DIFF_REPORT_USABILITY / DIFF_REPORT_PACKAGE_COMPARE / DIFF_REPORT_TRIGGER_STATUS
  - full: core + DIFF_REPORT_DETAIL_ITEM / DIFF_REPORT_ARTIFACT / DIFF_REPORT_ARTIFACT_LINE / DIFF_REPORT_DEPENDENCY / DIFF_REPORT_VIEW_CHAIN / DIFF_REPORT_REMAP_CONFLICT / DIFF_REPORT_OBJECT_MAPPING / DIFF_REPORT_BLACKLIST / DIFF_REPORT_FIXUP_SKIP / DIFF_REPORT_OMS_MISSING
  - 备注：DIFF_REPORT_WRITE_ERRORS / DIFF_REPORT_RESOLUTION 为写库追踪与闭环表，report_to_db 启用后默认创建。
        缺失/不支持明细可通过 DIFF_REPORT_DETAIL 查询；若需行化细节，使用 DIFF_REPORT_DETAIL_ITEM。
- fixup_dir：修补脚本输出目录。默认：fixup_scripts。
- fixup_dir_allow_outside_repo：是否允许 fixup_dir 指向项目目录外。默认：true。
- fixup_max_sql_file_mb：run_fixup 单文件最大读取大小（MB）。默认：50；<=0 表示不限制。
- fixup_force_clean：强制清理 fixup_dir（即使为项目外绝对路径）。默认：true。
  说明：开启后会删除 fixup_dir 下旧脚本；请确保路径配置正确，避免误删。
- log_dir：运行日志目录。默认：logs。
- log_level：控制台日志级别。默认：auto。可选：AUTO/DEBUG/INFO/WARNING/ERROR/CRITICAL。
  说明：AUTO 在 TTY 使用 INFO，非 TTY 使用 WARNING；日志文件固定 DEBUG。
- report_width：报告宽度（字符数）。默认：160。
  说明：非交互/重定向环境下可避免 Rich 报告被截断为 80 列。

超时设置
- cli_timeout：dbcat CLI 超时（秒）。默认：600。
- obclient_timeout：obclient 超时（秒），用于元数据与 SQL 执行。默认：60。
- fixup_cli_timeout：run_fixup 执行 SQL 超时（秒）。默认：3600；0 表示不设超时。
  说明：fixup_cli_timeout 仅影响 run_fixup 执行阶段，不影响生成阶段。

校验范围与依赖
- check_primary_types：限制主对象类型。默认：空（全部主对象）。
  可选值：TABLE, VIEW, MATERIALIZED VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, SYNONYM, JOB, SCHEDULE, TYPE, TYPE BODY。
  注意：MATERIALIZED VIEW 默认仅打印不校验。
- check_extra_types：限制扩展对象检查。默认：空（全部扩展对象）。
  可选值：INDEX, CONSTRAINT, SEQUENCE, TRIGGER。
- check_status_drift_types：状态漂移检查范围。默认：trigger,constraint。
  可选值：TRIGGER, CONSTRAINT。
  说明：仅在对应类型已被 check_extra_types 启用时生效；用于检查“对象存在但状态不一致”。
- check_dependencies：是否校验依赖与生成依赖报告。默认：true。
- print_dependency_chains：输出依赖链路拓扑（仅当 check_dependencies=true）。默认：true。
  说明：开启后生成 dependency_chains_*.txt 与 dependency_detail_*.txt。
- check_comments：是否比对表/列注释。默认：true。
  说明：依赖 DBA_TAB_COMMENTS / DBA_COL_COMMENTS。
- check_column_order：是否校验列顺序。默认：false。
  说明：仅在启用时比较列顺序，自动过滤 OMS/自动列/SYS_NC 等噪声列。
- check_object_usability：是否校验 VIEW/SYNONYM 可用性。默认：false。
  说明：通过 `SELECT * FROM <obj> WHERE 1=2` 验证对象能否解析/查询；不会返回数据。
  说明：可用性明细会结合依赖/不支持分类输出更精准根因（如依赖缺失、权限不足、同义词指向对象）。
  说明：被标记为不支持/阻断的对象会以 SKIPPED 记录入明细，并写明阻断原因。
- check_source_usability：是否同时校验源端可用性。默认：true。
  说明：用于判定“目标端不可用是否为预期行为”（源端也不可用）。
- usability_check_timeout：可用性校验单对象超时（秒）。默认：10。
  说明：超时会标记为 TIMEOUT，不视为不可用。
- usability_check_workers：可用性校验并发线程数。默认：10。
- max_usability_objects：可用性校验抽样阈值。默认：0（不抽样）。
- usability_sample_ratio：可用性校验抽样比例（0~1）。默认：0（不抽样）。
  说明：仅当 max_usability_objects>0 且 usability_sample_ratio>0 时才启用抽样。
  说明：被抽样跳过的对象不会写入明细报告，仅计入汇总的“跳过”数量。
- column_visibility_policy：列可见性(INVISIBLE)处理策略。默认：auto。
  可选值：auto（元数据可用时校验并生成修补）、enforce（强制校验/修补）、ignore（跳过可见性校验）。
- 说明：OB 侧 CHAR_USED 缺失时默认按 BYTE 语义处理；若 DATA_LENGTH > CHAR_LENGTH 则推断为 CHAR 语义，避免长度语义误判。
- infer_schema_mapping：是否启用 schema 推导（多对一/一对多场景）。默认：true。
  说明：用于 remap 未显式覆盖对象的目标 schema 推导。
- sequence_remap_policy：SEQUENCE 目标 schema 推导策略。默认：source_only。
  可选值：infer（依赖+主流表推导）、source_only（保持源 schema）、dominant_table（仅主流表推导）。
  说明：仅影响 SEQUENCE 缺失与修补脚本的目标 schema。

黑名单（不支持表过滤）
- blacklist_mode：黑名单来源模式。默认：auto。
  可选值：auto（TMP_BLACK_TABLE + 规则文件）、table_only、rules_only、disabled。
- blacklist_rules_path：黑名单规则 JSON 路径。默认：blacklist_rules.json（内置规则）。
  注意：blacklist_mode=auto/rules_only 时需确保该文件随工具部署，否则规则将被跳过。
  规则文件说明：每条 rule 使用 `enabled`（true/false）控制是否启用；程序兼容旧 `tag`（enabled/disabled）字段。运行时还会叠加 blacklist_rules_enable / blacklist_rules_disable。
- blacklist_rules_enable：仅启用指定规则（逗号分隔）。默认：空（全量规则）。
- blacklist_rules_disable：禁用指定规则（逗号分隔）。默认：空。
- blacklist_name_patterns：表名黑名单关键字（逗号分隔，字面包含匹配）。默认：_RENAME。
  说明：会自动转义 `%/_/!`，避免被当作通配符；命中后 black_type=NAME_PATTERN。
- blacklist_name_patterns_file：表名黑名单关键字文件（每行一个关键字，支持 #/; 注释）。默认：空。
- blacklist_name_patterns 与 blacklist_name_patterns_file：会合并去重；用于渲染规则中的 `{{name_pattern_clause}}`。
- blacklist_lob_max_mb：LOB 体积阈值（MB），超过则标记为 LOB_OVERSIZE。默认：512。

修补脚本生成（Fixup）
- generate_fixup：是否生成修补脚本。默认：true。
- generate_extra_cleanup：是否生成“目标端多余对象”的清理候选。默认：false。
  说明：仅输出注释候选文件 `fixup_scripts/cleanup_candidates/extra_cleanup_candidates.txt`，不会被 run_fixup 自动执行；用于人工审核后再处理。
- generate_status_fixup：是否生成“状态漂移”修补脚本。默认：true。
  说明：仅处理已存在对象的状态差异，不负责对象创建；输出目录为 fixup_scripts/status/。
- status_fixup_types：状态漂移修补脚本类型。默认：trigger,constraint。
  可选值：TRIGGER, CONSTRAINT。
- constraint_status_sync_mode：约束状态同步模式。默认：enabled_only。
  可选值：enabled_only（仅同步 ENABLED/DISABLED）、full（额外同步 VALIDATED/NOT VALIDATED）。
- trigger_validity_sync_mode：触发器有效性同步模式。默认：compile。
  可选值：off（不处理 VALID/INVALID）、compile（当源 VALID 且目标 INVALID 时生成 COMPILE）。
- fixup_drop_sys_c_columns：是否对目标端额外 SYS_C* 列生成 ALTER TABLE FORCE。默认：true。
  说明：仅对“目标端多出来且列名匹配 SYS_C\\d+”的列生成 FORCE 清理；其余多余列仍保持注释建议。
- generate_interval_partition_fixup：是否生成 interval 分区补齐脚本。默认：true。
- interval_partition_cutoff：interval 分区补齐截止日期（YYYYMMDD）。默认：20280301。
- interval_partition_cutoff_numeric：数值型 interval 分区补齐上限（仅数值分区键生效）。默认：空（不补齐数值 interval）。注意：必须为正数。
- fixup_schemas：仅为指定目标 schema 生成修补脚本。默认：空（全量）。
- fixup_types：仅为指定对象类型生成修补脚本。默认：空（全量）。
  可选值：TABLE, VIEW, MATERIALIZED VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY,
           SYNONYM, JOB, SCHEDULE, TYPE, TYPE BODY, SEQUENCE, TRIGGER, INDEX, CONSTRAINT。
  注意：fixup_types 包含 INDEX/CONSTRAINT/TRIGGER 时，需要 check_primary_types 含 TABLE 才能生成。
- fixup_idempotent_mode：修补脚本幂等模式。默认：replace。
  可选值：off（不处理）、replace（CREATE OR REPLACE）、guard（存在则跳过创建）、drop_create（存在则 DROP 再创建）。
- fixup_idempotent_types：幂等模式作用对象类型（逗号分隔）。默认：空（使用安全默认集）。
  默认集：VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER, TYPE, TYPE BODY, SYNONYM。
- fixup_workers：修补脚本生成并发数。默认：min(12, CPU)。
- progress_log_interval：修补生成进度日志间隔（秒）。默认：10；最小 1。

授权与权限脚本
- generate_grants：是否生成授权脚本并附加到修补 DDL。默认：true。
  注意：generate_grants 仅控制授权脚本与注入，修补脚本仍由 generate_fixup 控制。
- grant_tab_privs_scope：DBA_TAB_PRIVS 抽取范围。默认：owner。
  可选值：owner（仅源 schema 所拥有对象）、owner_or_grantee（兼容旧逻辑）。
- grant_merge_privileges：合并同一对象的多权限授权。默认：true。
- grant_merge_grantees：合并同一权限的多 grantee 授权。默认：true。
- grant_supported_sys_privs：支持的系统权限清单（逗号分隔）。默认：空（自动探测）。
- grant_supported_object_privs：支持的对象权限清单（逗号分隔）。默认：空（内置白名单）。
- grant_include_oracle_maintained_roles：是否生成 ORACLE_MAINTAINED 角色。默认：false。
- fixup_auto_grant：run_fixup 自动补权限。默认：true。
  说明：基于 dependency_chains 与 VIEWs_chain 预判依赖授权，执行前自动应用 grants_miss/grants_all 中的授权。
- fixup_auto_grant_types：自动补权限对象类型（逗号分隔）。默认：
  VIEW, MATERIALIZED VIEW, SYNONYM, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TYPE, TYPE BODY。
  说明：仅对这些对象执行自动补权限；其他对象仍按原流程执行。
- fixup_auto_grant_fallback：无匹配授权脚本时是否自动生成 GRANT。默认：true。
  说明：关闭后仅使用 grants_miss/grants_all，找不到授权则记录提示并继续执行。
- fixup_auto_grant_cache_limit：自动补权限缓存大小（条目数）。默认：10000。
  说明：限制 run_fixup 查询缓存占用，0/负数表示不限制。

同义词与触发器
- synonym_check_scope：同义词校验范围。默认：public_only。
  可选值：public_only（仅 PUBLIC 参与 missing/extra/汇总统计）、all（PUBLIC+私有全量校验）。
  说明：该开关影响“校验与统计口径”；不影响同义词 fixup 输出范围。
- synonym_fixup_scope：同义词修补范围。默认：public_only。
  可选值：all（PUBLIC+私有）、public_only（仅 PUBLIC）。
- trigger_list：触发器清单文件（每行 SCHEMA.TRIGGER_NAME）。默认：空。
  注意：配置后仅生成列表内触发器，并输出 trigger_status_report.txt 报告；清单读取失败会回退全量触发器。
- trigger_qualify_schema：触发器 DDL 是否强制补全 schema 前缀。默认：true。

DDL 清洗
- ddl_punct_sanitize：清洗 PL/SQL DDL 中的全角标点。默认：true。
- ddl_hint_policy：hint 清洗策略。默认：keep_supported。
  可选值：drop_all / keep_supported / keep_all / report_only。
- ddl_hint_allowlist：额外允许的 hint（逗号分隔）。默认：空。
- ddl_hint_denylist：强制删除的 hint（逗号分隔）。默认：空。
- ddl_hint_allowlist_file：额外允许 hint 文件路径（每行一个）。默认：空。

DDL 格式化 (SQLcl)
- ddl_format_enable：是否启用 SQLcl 格式化（后处理，仅影响输出 DDL 可读性）。默认：false。
  说明：格式化发生在全部清洗/替换完成之后，不影响校验与修补逻辑。
- ddl_formatter：格式化器选择。默认：sqlcl。可选值：sqlcl / none。
- ddl_format_types：需要格式化的对象类型列表。默认：空（当 ddl_format_enable=true 时默认 VIEW）。
  可选值：TABLE, VIEW, MATERIALIZED VIEW, INDEX, SEQUENCE, SYNONYM, PROCEDURE, FUNCTION,
           PACKAGE, PACKAGE BODY, TRIGGER, TYPE, TYPE BODY, CONSTRAINT, TABLE_ALTER, JOB, SCHEDULE。
- sqlcl_bin：SQLcl 根目录或 bin/sql 可执行文件路径（开启格式化必填）。
- sqlcl_profile_path：SQL Developer 格式化规则文件（可选）。不存在会告警并忽略。
- ddl_format_fail_policy：格式化失败策略。默认：fallback。
  可选值：fallback（失败保留原 DDL，不中断运行）/ error（失败抛错，保留原 DDL）。
- ddl_format_batch_size：每批次格式化对象数量。默认：200（适当增大可减少 SQLcl 启动开销）。
- ddl_format_timeout：每批次格式化超时（秒）。默认：60；0 表示不设超时。
- ddl_format_max_lines：单个 DDL 最大行数（超过则跳过）。默认：30000；0 表示不限制。
- ddl_format_max_bytes：单个 DDL 最大字节数（超过则跳过）。默认：2000000；0 表示不限制。
  说明：格式化会自动去除 PL/SQL 的尾部 "/" 再执行 SQLcl，结束后恢复。
  报告：main_reports/ddl_format_report_<timestamp>.txt。

视图兼容性
- view_compat_rules_path：视图兼容性规则 JSON 路径（可选）。默认：空（使用内置规则）。
- view_dblink_policy：视图 DBLINK 处理策略。默认：block。
  可选值：block（遇到 @ 视为不支持）、allow（允许 DBLINK）。
- view_constraint_cleanup：VIEW 列清单约束清洗策略。默认：auto。
  可选值：auto（仅清洗 RELY DISABLE / DISABLE / NOVALIDATE）、force（强制清洗所有列清单约束）、off（不清洗，视为不支持）。
  说明：仅处理 CREATE VIEW 列清单内的约束声明；清洗仅影响输出 DDL，不改变依赖/映射逻辑。

扩展对象校验性能调优
- extra_check_workers：扩展对象校验并发数。默认：min(16, CPU)。
- extra_check_chunk_size：扩展对象校验单批表数量。默认：200（最小 1）。
- extra_check_progress_interval：扩展对象校验进度日志间隔（秒）。默认：10（最小 1）。

Oracle Client
- oracle_client_lib_dir：Oracle Instant Client 目录（需包含 libclntsh.so）。默认：空（使用 Thin 模式）。

dbcat 配置
- dbcat_bin：dbcat 安装目录或可执行文件路径。默认：空（未配置时会告警）。
- dbcat_from：dbcat 源端 profile。默认：空（建议 oracle19c）。
- dbcat_to：dbcat 目标端 profile。默认：空（建议 oboracle422）。
- dbcat_output_dir：dbcat 输出缓存目录。默认：dbcat_output。
- dbcat_chunk_size：dbcat 每批对象数量。默认：150。
- dbcat_parallel_workers：dbcat 并发数。默认：4。
- dbcat_no_cal_dep：是否关闭 dbcat 依赖计算。默认：false。
- dbcat_query_meta_thread：dbcat 元数据查询线程数。默认：0（关闭）。
- dbcat_progress_interval：dbcat 进度心跳间隔（秒）。默认：15；<=0 关闭。
- cache_parallel_workers：dbcat 缓存读取并发。默认：1。
- dbcat_cleanup_run_dirs：是否清理每次运行的 dbcat 临时目录。默认：true。
- java_home：JAVA_HOME 路径（dbcat 需要）。默认：读取环境变量。

5) 备注
- 如果某个键缺失，将使用程序内置默认值。
- 仅当 generate_fixup=true 时，dbcat 与 fixup 相关配置才会生效。
- 仅当 check_dependencies=true 时，依赖链路相关输出才会生成。

6) 交付前最小验收（建议固定执行）
- 语法检查：`python3 -m py_compile $(git ls-files '*.py')`
- 单元测试：`.venv/bin/python -m unittest discover -v`
- 可选联调（需真实 Oracle/OB）：`RUN_INTEGRATION_TESTS=1 .venv/bin/python -m unittest test_integration_visibility.py -v`
- 建议保留并随包分发：`blacklist_rules.json`（blacklist_mode=auto/rules_only 时需要）
