配置说明 (config.ini)
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
- fixup_dir：修补脚本输出目录。默认：fixup_scripts。
- log_dir：运行日志目录。默认：logs。
- log_level：控制台日志级别。默认：INFO。可选：DEBUG/INFO/WARNING/ERROR/CRITICAL。
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
- check_dependencies：是否校验依赖与生成依赖报告。默认：true。
- print_dependency_chains：输出依赖链路拓扑（仅当 check_dependencies=true）。默认：true。
  说明：开启后生成 dependency_chains_*.txt 与 dependency_detail_*.txt。
- check_comments：是否比对表/列注释。默认：true。
  说明：依赖 DBA_TAB_COMMENTS / DBA_COL_COMMENTS。
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
- blacklist_rules_enable：仅启用指定规则（逗号分隔）。默认：空（全量规则）。
- blacklist_rules_disable：禁用指定规则（逗号分隔）。默认：空。
- blacklist_lob_max_mb：LOB 体积阈值（MB），超过则标记为 LOB_OVERSIZE。默认：512。

修补脚本生成（Fixup）
- generate_fixup：是否生成修补脚本。默认：true。
- generate_interval_partition_fixup：是否生成 interval 分区补齐脚本。默认：false。
- interval_partition_cutoff：interval 分区补齐截止日期（YYYYMMDD）。默认：20280301。
- interval_partition_cutoff_numeric：数值型 interval 分区补齐上限（仅数值分区键生效）。默认：空（不补齐数值 interval）。注意：必须为正数。
- fixup_schemas：仅为指定目标 schema 生成修补脚本。默认：空（全量）。
- fixup_types：仅为指定对象类型生成修补脚本。默认：空（全量）。
  可选值：TABLE, VIEW, MATERIALIZED VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY,
           SYNONYM, JOB, SCHEDULE, TYPE, TYPE BODY, SEQUENCE, TRIGGER, INDEX, CONSTRAINT。
  注意：fixup_types 包含 INDEX/CONSTRAINT/TRIGGER 时，需要 check_primary_types 含 TABLE 才能生成。
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

同义词与触发器
- synonym_fixup_scope：同义词修补范围。默认：all。
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

扩展对象校验性能调优
- extra_check_workers：扩展对象校验并发数。默认：min(4, CPU)。
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
