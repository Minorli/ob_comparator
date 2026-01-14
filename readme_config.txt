配置说明 (config.ini)
本文件说明 config.ini 中支持的所有设置。任何未配置的项都将使用内置默认值。

1) 配置段 (Sections)
- [ORACLE_SOURCE]: 源端 Oracle 连接。
- [OCEANBASE_TARGET]: 目标端 OceanBase 连接 (obclient)。
- [SETTINGS]: 对比工具行为、修复生成和工具设置。

2) [ORACLE_SOURCE]
- user: Oracle 用户名。
- password: Oracle 密码。
- dsn: Oracle DSN，例如 host:port/service_name。

3) [OCEANBASE_TARGET]
- executable: obclient 可执行文件路径。
- host: OceanBase 主机地址。
- port: OceanBase 端口。
- user_string: 完整的 obclient -u 参数值 (例如 root@sys#cluster)。
- password: OceanBase 密码。

4) [SETTINGS]

核心设置 (Core)
- source_schemas: 要扫描的源端 schema 列表（逗号分隔）。
- remap_file: Remap 规则文件路径。留空表示 1:1 映射。

超时设置 (Timeouts)
- cli_timeout: dbcat CLI 超时时间（秒）（建议 300-1800）。
- obclient_timeout: obclient 超时时间（秒）（建议 60-600）。
- fixup_cli_timeout: run_fixup 执行 SQL 超时时间（秒）；0 表示不设超时（建议 3600）。

输出设置 (Output)
- fixup_dir: 修复脚本输出目录（相对或绝对路径）。
- report_dir: 报告输出目录（相对或绝对路径）。
- log_dir: run_*.log 日志文件的存放目录。
- log_level: 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)。
- report_width: 丰富输出的报告宽度（避免被截断）。

修复生成 (Fixup generation)
- generate_fixup: 是否生成修复脚本 (true/false/1/0/yes/no)。
- generate_grants: 是否生成授权脚本并附加到修复 DDL (true/false/1/0/yes/no)。
  授权脚本输出到 fixup_scripts/grants_all/ 与 fixup_scripts/grants_miss/，包含对象权限与系统/角色权限。
- grant_tab_privs_scope: DBA_TAB_PRIVS 抽取范围。
  可选值: owner / owner_or_grantee
  owner: 仅加载 source_schemas 拥有的对象权限（默认，性能更佳）。
  owner_or_grantee: 兼容旧逻辑，额外加载 grantee 在 source_schemas 的对象授权。
- grant_merge_privileges: 合并同一对象的多权限授权 (true/false/1/0/yes/no)。
- grant_merge_grantees: 合并同一权限的多 grantee 授权 (true/false/1/0/yes/no)。
- grant_supported_sys_privs: 支持的系统权限清单（逗号分隔）。
  留空表示自动从 OceanBase 探测；填写后以配置为准。
- grant_supported_object_privs: 支持的对象权限清单（逗号分隔）。
  留空表示使用默认白名单 (SELECT/INSERT/UPDATE/DELETE/REFERENCES/EXECUTE)。
- grant_include_oracle_maintained_roles: 是否生成 ORACLE_MAINTAINED 角色 (true/false/1/0/yes/no)。
  默认 false；仅在 DBA_ROLES 可读取时生效。
- fixup_workers: 修复生成并发数；留空 or 0 则使用 min(12, CPU)。
- progress_log_interval: 进度日志间隔（秒）(>=1)。
- dbcat_chunk_size: 每批次 dbcat 处理的对象数量（建议 100-300）。
- fixup_schemas: 仅为指定目标 schema 生成修复脚本；留空表示全部。
- fixup_types: 仅为指定对象类型生成修复脚本；留空表示全部。
  有效值:
    TABLE, VIEW, MATERIALIZED VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY,
    SYNONYM, JOB, SCHEDULE, TYPE, TYPE BODY, SEQUENCE, TRIGGER, INDEX, CONSTRAINT
- trigger_list: 可选触发器清单过滤文件（每行 SCHEMA.TRIGGER_NAME）。
  配置后仅生成列表内触发器脚本，并输出 main_reports/trigger_miss.txt 记录无效/不存在/非缺失项。

检查范围 (Check scope)
- check_primary_types: 限制主要对象类型（留空表示全部）。
  有效值:
    TABLE, VIEW, MATERIALIZED VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY,
    SYNONYM, JOB, SCHEDULE, TYPE, TYPE BODY
  注意: MATERIALIZED VIEW 默认仅打印不校验。
  说明: 该范围会影响元数据加载、Remap 推导、对比与修复脚本生成。
- check_extra_types: 限制额外对象检查。
  有效值: INDEX, CONSTRAINT, SEQUENCE, TRIGGER
  说明: 该范围会影响依附对象的加载与推导范围。
- check_dependencies: true/false/1/0/yes/no. 启用依赖检查（授权生成由 generate_grants 控制）。
- print_dependency_chains: true/false/1/0/yes/no. 打印依赖链（仅当 check_dependencies=true 时生效）。
- check_comments: true/false/1/0/yes/no. 对比表/列注释。
- infer_schema_mapping: true/false/1/0/yes/no. 在一对多映射场景中启用 schema 推导。
- ddl_punct_sanitize: true/false/1/0/yes/no. 清洗 PL/SQL DDL 中的全角标点（默认开启）。

Oracle 客户端 (Oracle client)
- oracle_client_lib_dir: Oracle Instant Client 目录（必须包含 libclntsh.so）。

dbcat 工具 (dbcat)
- dbcat_parallel_workers: dbcat 并发数 (>=1)。
- dbcat_bin: dbcat 安装目录 or 可执行文件路径。
- dbcat_output_dir: dbcat 输出目录。
- dbcat_from: dbcat 源端 profile（取决于 dbcat 版本）。
- dbcat_to: dbcat 目标端 profile（取决于 dbcat 版本）。
- dbcat_no_cal_dep: 是否关闭依赖计算以加速抽取 (true/false)。
- dbcat_query_meta_thread: dbcat 元数据查询线程数 (>=1)。
- dbcat_progress_interval: dbcat 进度心跳间隔（秒，<=0 关闭）。
- java_home: JAVA_HOME 路径（dbcat 需要）。

5) 默认值 (Defaults)
如果某个键缺失，程序将使用其内部默认值。参见 config.ini.template 了解默认值。
