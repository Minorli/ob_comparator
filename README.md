# OceanBase Comparator Toolkit

> 当前版本：V0.9.8.8  
> 面向 Oracle → OceanBase (Oracle 模式) 的结构一致性校验与修补脚本生成工具  
> 核心理念：一次转储、本地对比、脚本审计优先

## 近期更新（0.9.8.8）
- 授权文件可读性增强：owner 级 `*.grants.sql` 在 `OBJECT_TYPE: TABLE` 段内继续细分 `TABLE_OBJECT_GRANTS` 与 `TABLE_COLUMN_GRANTS`，便于快速区分整表授权和列级授权。
- 该细分仅影响授权文件渲染，不改变 grant 采集、过滤、合并、fixup 执行目录和执行语义。
- README / `readme_config.txt` / OpenSpec release 元数据已同步到 `0.9.8.8`。

## 核心能力
- **对象覆盖完整**：TABLE/VIEW/MVIEW/PLSQL/TYPE/JOB/SCHEDULE + INDEX/CONSTRAINT/SEQUENCE/TRIGGER。
- **Dump-Once 架构**：Oracle Thick Mode + 少量 obclient 调用，元数据一次性落本地内存。
- **Remap 推导**：支持显式规则、依附对象跟随、依赖推导、schema 回退策略。
- **依赖与授权**：基于 DBA_DEPENDENCIES/DBA_*_PRIVS 生成缺失依赖与授权脚本。
- **DDL 清洗与兼容**：VIEW DDL 走 DBMS_METADATA，PL/SQL 语法清洗与 Hint 过滤。
- **DDL 输出格式化**：可选 SQLcl 格式化 fixup DDL（不影响校验与修补逻辑）。
- **修补脚本执行器**：支持 smart-order、迭代重试、VIEW 链路自动修复、错误报告。
- **报告体系**：Rich 控制台 + 纯文本快照 + 细节分拆报告（可配置）。
- **空表风险识别**：可选识别“源端有数据但目标端空表”的高风险场景（不做 `COUNT(*)`）。
- **不支持对象识别**：黑名单/依赖阻断对象单独统计与分流输出。

## 新增能力总览（按模块）
- **报告入库**：`report_to_db` 支持 `summary/core/full` 多级落库与行化明细，支持数据库侧直接排查（含 HOW TO SQL 手册）。
- **迁移聚焦报告**：按“可修补缺失 vs 不支持/阻断”拆分，保留全量明细并可在主报告快速定位。
- **可用性校验**：支持 VIEW/SYNONYM 查询可用性检查（`WHERE 1=2`）与根因输出。
- **表数据风险校验**：`table_data_presence_check` 识别“源端有数据、目标空表”风险；`auto` 为统计口径，`on` 为严格回表。
- **对象时间截断**：`object_created_before` 可按 `CREATED` 截止时间冻结校验范围，支持缺失 CREATED 策略（`strict/include_missing/exclude_missing`），并输出 `objects_after_cutoff_detail_<ts>.txt` 与 report_db 对齐明细。
- **run_fixup 增强**：支持 `--iterative`、`--view-chain-autofix`、语句级继续执行、授权修剪与错误报告。
- **run_fixup 安全门禁**：默认跳过 `fixup_scripts/table/`，需显式 `--allow-table-create` 才执行建表脚本。
- **状态漂移修复**：支持 TRIGGER/CONSTRAINT 的状态差异检测与状态修补脚本生成。
- **约束策略增强**：支持缺失约束的 `safe_novalidate/source/force_validate` 策略；仅当源端最终语义需要 `VALIDATED` 时，才生成后置 `validate_later` 脚本。
- **视图兼容治理**：支持 VIEW 兼容规则、DBLINK 策略、列清单约束清洗与依赖链修复。
- **DDL 清洗与格式化**：支持 `ddl_cleanup_detail_<ts>.txt` 审计明细、全角标点清洗、hint 策略清洗、SQLcl 格式化（可按类型/体积/超时控制）；语义改写会在脚本头写 `DDL_REWRITE` 注释。
- **黑名单与排除机制**：支持规则引擎、名称模式、显式排除清单（`exclude_objects_file`）与依赖联动过滤。
- **版本门控**：按 OB 版本动态处理 MVIEW、interval 分区等能力差异，降低跨版本误报。

## 适用场景
- Oracle → OceanBase 迁移后的结构一致性审计
- OMS 仅迁移表结构/数据后，补齐非表对象与授权
- 多 schema Remap、依赖复杂、VIEW 链较长的迁移项目

## 环境与依赖
- Python 3.7+
- Oracle Instant Client 19c+（oracledb Thick Mode）
- obclient（目标 OceanBase 可连接）
- JDK + dbcat（DDL 批量提取）
- SQLcl（可选，用于 DDL 格式化）
- 运行账号需具备 DBA_* 视图访问权限（Oracle 与 OB）
- 安全说明：工具运行时不会把 OB/dbcat 密码作为明文参数暴露在 `ps` 命令中（配置文件仍按当前方式保留密码项）。

## 快速开始

### 1) 安装依赖
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) 生成配置
```bash
cp config.ini.template.txt config.ini
```

最小示例（只列关键项）：
```ini
[ORACLE_SOURCE]
user = scott
password = tiger
dsn = 127.0.0.1:1521/orclpdb1

[OCEANBASE_TARGET]
executable = /usr/bin/obclient
host = 127.0.0.1
port = 2883
user_string = root@sys#obcluster
password = xxx

[SETTINGS]
source_schemas = SCOTT,HR
remap_file = remap_rules.txt
synonym_check_scope = public_only
synonym_fixup_scope = public_only
sequence_remap_policy = source_only
trigger_qualify_schema = true
report_dir_layout = per_run
report_detail_mode = split
report_to_db = true
table_data_presence_check = auto
object_created_before =
object_created_before_missing_created_policy = strict
table_data_presence_auto_max_tables = 20000
table_data_presence_chunk_size = 500
config_hot_reload_mode = off
config_hot_reload_interval_sec = 5
config_hot_reload_fail_policy = keep_last_good
oracle_client_lib_dir = /opt/instantclient_19_28
dbcat_bin = /opt/dbcat-2.5.0-SNAPSHOT
dbcat_output_dir = dbcat_output
java_home = /usr/lib/jvm/java-11
```
完整配置说明见 `readme_config.txt`。

### 3) 运行对比
```bash
python3 schema_diff_reconciler.py
# 配置缺项可用向导
python3 schema_diff_reconciler.py --wizard
```

### 4) 审核并执行修复
```bash
# 先审核 fixup_scripts/ 下的 SQL
python3 run_fixup.py --smart-order --recompile
```

主程序跑完后，优先看两处：
- `main_reports/run_<ts>/manual_actions_required_<ts>.txt`
  说明：这是本次仍需人工处理/确认的统一清单，先看它，再展开其他 detail/fixup。
- `main_reports/run_<ts>/oracle_privilege_family_detail_<ts>.txt`
  说明：这是 Oracle 权限族覆盖清单。`grants_miss/` 只代表当前 runnable grants，不等于全部 Oracle 权限已经闭环。
- `report_<ts>.txt` 里的 `执行结论` 和 `本次建议处理顺序`
- `fixup_scripts/README_FIRST.txt`
  说明：这个文件会按本次实际生成的目录解释哪些可以先看、哪些默认不要直接执行。

说明：`run_fixup.py` 启动时也会读取最新的 `manual_actions_required_<ts>.txt`，并按本次执行目录提示相关人工项。

说明：若本次运行命中了你还没见过的新行为，运行总结里会额外出现 `本次相关变化提醒`，且同一提醒默认只展示一次。

## 交付前正确性自检
建议在发版/交付前固定执行以下检查：
```bash
# 1) 语法检查
python3 -m py_compile $(git ls-files '*.py')

# 2) 运行主程序（建议先在测试库验证）
.venv/bin/python schema_diff_reconciler.py config.ini

# 3) 修补执行器冒烟（不执行真实 SQL）
.venv/bin/python run_fixup.py config.ini --glob "__NO_MATCH__"
```

## Remap 规则速记
- **显式规则优先级最高**，未写规则的对象按默认推导。
- **TABLE 必须显式**：表的 remap 建议只写表规则。
- **VIEW/MVIEW/TRIGGER/PACKAGE** 默认保持原 schema，需显式 remap 才改。
- **INDEX/CONSTRAINT/SEQUENCE** 默认跟随父表。
- **PROCEDURE/FUNCTION/TYPE/SYNONYM** 可通过依赖推导目标 schema。

示例：
```
SRC_A.ORDERS = OB_A.ORDERS
SRC_A.VW_REPORT = OB_A.VW_REPORT
SRC_A.TRG_ORDER = OB_A.TRG_ORDER
```

## run_fixup 执行模式

默认安全策略：`run_fixup` 会默认跳过 `fixup_scripts/table/`，避免误创建空表。
如确认要执行建表脚本，请显式加 `--allow-table-create`。

**标准执行**（一次运行）：
```bash
python3 run_fixup.py --smart-order --recompile
```

**迭代执行**（推荐用于 VIEW/依赖复杂场景）：
```bash
python3 run_fixup.py --iterative --smart-order --recompile --max-rounds 10
```

**VIEW 链路自动修复**（依赖链驱动）：
```bash
python3 run_fixup.py --view-chain-autofix
```

**显式允许执行建表脚本**（仅在确认需要时使用）：
```bash
python3 run_fixup.py --smart-order --recompile --allow-table-create
```

## 额外工具
- `init_users_roles.py`：以 Oracle 为准创建用户/角色并同步系统权限与角色授权。
> 注意：`init_users_roles.py` 运行时会交互输入用户初始密码，不再在脚本中写死明文初始密码。
- `prod_diagnose.py`：生产排障只读诊断器，优先定位“误报/口径漂移/fixup 失败根因”。
  - 自动取最新报告：
    `python3 prod_diagnose.py config.ini`
  - 指定 report_id：
    `python3 prod_diagnose.py config.ini --report-id <report_id>`
  - 单对象深挖（映射/依赖/可用性/授权/fixup失败）：
    `python3 prod_diagnose.py config.ini --report-id <report_id> --focus-object VIEW:SCHEMA.OBJ --deep`
  - 输出文件：
    `triage_summary_*.txt` / `triage_detail_*.txt` / `triage_fixup_failures_*.txt` / `triage_false_positive_candidates_*.txt`
    （`--deep` 时增加 `triage_focus_deep_*.txt`）

## 主要输出
- `main_reports/run_<ts>/report_<ts>.txt`：完整对比报告（默认 per_run）
- `main_reports/run_<ts>/report_index_<ts>.txt`：报告索引，包含 `GUIDE` 行，提示先看哪些文件
- `main_reports/run_<ts>/package_compare_<ts>.txt`：PACKAGE/PKG BODY 明细
- `main_reports/run_<ts>/remap_conflicts_<ts>.txt`：Remap 冲突清单
- `main_reports/run_<ts>/VIEWs_chain_<ts>.txt`：VIEW 依赖链报告
- `main_reports/run_<ts>/blacklist_tables.txt`：黑名单表清单
- `main_reports/run_<ts>/filtered_grants.txt`：过滤授权清单
- `main_reports/run_<ts>/manual_actions_required_<ts>.txt`：本次必须人工处理/确认的统一清单（聚合 unsupported/deferred/review-first 项）
- `main_reports/run_<ts>/grant_capability_detail_<ts>.txt`：本次授权动态规则库明细（含目标端目录权限别名，如 `DEBUG -> OTHERS`）
- `main_reports/run_<ts>/oracle_privilege_family_detail_<ts>.txt`：Oracle 权限族覆盖明细（区分 `RUNNABLE / MANUAL_ONLY`，当前 `DBA_COL_PRIVS` 已纳入 runnable grants，ACL/AQ/XS/Resource Manager 等仍先做盘点）
- `main_reports/run_<ts>/target_extra_grants_detail_<ts>.txt`：目标端额外对象授权明细（含 PUBLIC 扩权风险）
- `main_reports/run_<ts>/ddl_cleanup_detail_<ts>.txt`：DDL 清理/改写明细（区分 `format_only / syntax_compat / environment_detach / semantic_rewrite`，并标记 `evidence_level`）
- `main_reports/run_<ts>/trigger_status_report.txt`：触发器清单/状态差异报告
- `main_reports/run_<ts>/triggers_non_table_detail_<ts>.txt`：源端非表触发器明细（如 `BEFORE DROP ON DATABASE`、`INSTEAD OF ... ON VIEW`），这些对象不会按普通 `trigger/` DDL 自动生成
- `main_reports/run_<ts>/triggers_temp_table_unsupported_detail_<ts>.txt`：临时表触发器不支持明细（`TRIGGER_ON_TEMP_TABLE_UNSUPPORTED`）；对应 DDL 仅输出到 `fixup_scripts/unsupported/trigger/` 供人工改造参考，不进入普通 `trigger/`
- `main_reports/run_<ts>/triggers_view_reference_detail_<ts>.txt`：触发器中引用视图的提醒明细（保留视图语义，不改写为表）
- `main_reports/run_<ts>/triggers_literal_object_path_detail_<ts>.txt`：触发器中 `SCHEMA.OBJECT.COLUMN` 字符串路径提醒明细（默认不自动改写）
- `main_reports/run_<ts>/table_data_presence_detail_<ts>.txt`：表数据存在性风险明细（源有数据/目标空表）
- `main_reports/run_<ts>/objects_after_cutoff_detail_<ts>.txt`：创建时间范围过滤对象明细（`FILTERED_BY_CREATED_AFTER_CUTOFF` / `FILTERED_BY_MISSING_CREATED`）
- `main_reports/run_<ts>/case_sensitive_identifiers_detail_<ts>.txt`：大小写敏感(双引号)标识符明细
- `main_reports/run_<ts>/sys_c_force_candidates_detail_<ts>.txt`：SYS_C* FORCE 候选表明细（用于评估是否开启 `fixup_drop_sys_c_columns`）
- `main_reports/run_<ts>/missing_objects_detail_<ts>.txt`：缺失对象支持性明细（report_detail_mode=split）
- `main_reports/run_<ts>/unsupported_objects_detail_<ts>.txt`：不支持/阻断对象明细（report_detail_mode=split）
- `main_reports/run_<ts>/extra_mismatch_detail_<ts>.txt`：扩展对象差异明细（report_detail_mode=split）
- `main_reports/run_<ts>/column_nullability_detail_<ts>.txt`：现有列空值语义差异明细（含 `NOT NULL`、`NOT NULL ENABLE NOVALIDATE` 与反向漂移；其中 `ENABLE NOVALIDATE` 补位会在 `table_alter` 中默认输出可执行约束 SQL）
- 普通 `NOT NULL` 收紧默认改为 `plain_not_null_fixup_mode=runnable_if_no_nulls`：先探测目标端是否存在 `NULL`，仅无 `NULL` 时输出可执行 `MODIFY ... NOT NULL`；如需恢复纯 review-first，可改回 `review_only`
- `main_reports/run_<ts>/column_identity_detail_<ts>.txt`：现有列 identity 差异明细（含 `ALWAYS / BY DEFAULT / BY DEFAULT ON NULL` 模式差异；首版为 review-first）
- `main_reports/run_<ts>/column_default_detail_<ts>.txt`：现有列默认值差异明细（仅列级语义，不等同 `DEFAULT ON NULL`）
- `main_reports/run_<ts>/dependency_detail_<ts>.txt`：依赖差异明细（report_detail_mode=split）
- `*_detail_*.txt` 明细文件采用 `|` 分隔，并包含 `# total/# 字段说明` 头，格式与 `package_compare` 一致，便于 Excel 直接分隔导入。
- `main_reports/run_<ts>/missed_tables_views_for_OMS/`：OMS 缺失 TABLE/VIEW 规则
- `fixup_scripts/`：修补脚本输出（执行前需人工审核）
- `fixup_scripts/README_FIRST.txt`：fixup 根目录导航，说明本次生成目录的用途与默认执行边界；若存在人工项，会先指向 `manual_actions_required_<ts>.txt`
- `fixup_scripts/grants_miss/`：缺失授权脚本
- `fixup_scripts/grants_all/*.grants.sql` / `fixup_scripts/grants_miss/*.grants.sql`：现在既可能包含普通对象授权，也可能包含列级授权，如 `GRANT UPDATE (COL) ON OWNER.TABLE TO USER`
- `fixup_scripts/grants_revoke/`：目标端额外 PUBLIC 授权回收建议（默认仅 PUBLIC 自动给出 REVOKE）
- `fixup_scripts/grants_all/*.grants.sql` / `fixup_scripts/grants_miss/*.grants.sql`：对象/列授权文件在同一 owner 文件内按 `OBJECT_TYPE` 分段；当 `OBJECT_TYPE=TABLE` 时，还会细分 `TABLE_OBJECT_GRANTS` 与 `TABLE_COLUMN_GRANTS`，便于人工审核
- `fixup_scripts/tables_unsupported/`：不支持 TABLE 的 DDL（默认不执行）
- `fixup_scripts/unsupported/`：不支持/阻断对象 DDL（默认不执行）
- `fixup_scripts/view_chain_plans/`：VIEW 链路修复计划
- `fixup_scripts/errors/`：run_fixup 错误报告

## 触发器专项说明
- 触发器中的真实对象引用会继续按现有规则做 schema 补全和 remap。
- 如果触发器字符串字面量完整等于 `SCHEMA.OBJECT`，也会按 remap 自动改写，例如 `'LIFEBASE.T1' -> 'BASEDATA.T1'`。
- 如果字符串字面量是 `SCHEMA.OBJECT.COLUMN` 三段式路径，程序默认不自动改写，避免把列名、协议文本或日志内容误改坏；这类情况会输出到 `triggers_literal_object_path_detail_<ts>.txt` 供人工确认。
- 如果源端触发器不是普通表触发器（例如 `BEFORE DROP ON DATABASE`、`INSTEAD OF ... ON VIEW`），程序不会再静默漏掉；会输出到 `triggers_non_table_detail_<ts>.txt`，并在 `manual_actions_required_<ts>.txt` 中显式提醒。
- 触发器中的 `PRAGMA AUTONOMOUS_TRANSACTION` 现在会保留，不再被清洗掉。

## DDL 清理治理
- `ddl_cleanup_detail_<ts>.txt` 会把每条清理/保留动作拆成 `STATUS/RULE/CATEGORY/EVIDENCE_LEVEL/CHANGE_COUNT`，便于区分“格式整理”和“真实兼容性改写”。
- 若某条 cleanup 规则的样本提取退化，`NOTE` 中会出现 `SAMPLE_EXTRACTION_DEGRADED: ...`，表示统计仍成立，但样本展示已降级。
- `PRAGMA AUTONOMOUS_TRANSACTION`、`PRAGMA SERIALLY_REUSABLE`、`STORAGE(...)` 现在默认保留；`TABLESPACE` 不再按“语法不支持”自动删除。
- `LONG -> CLOB`、`LONG RAW/BFILE -> BLOB`、`INTERVAL` 分区处理属于语义改写，会在 fixup SQL 头部追加 `DDL_REWRITE: ...` 注释，并同步进入 `ddl_cleanup_detail_<ts>.txt`。
- `TYPE ... NOT PERSISTABLE` 当前按源端语义保留，不会被默认清洗成普通 TYPE，也不会因为该子句差异额外制造 TYPE mismatch 噪声。

## 黑名单规则
- 默认启用 `blacklist_rules.json` 规则并尝试读取 `OMS_USER.TMP_BLACK_TABLE`（`blacklist_mode=auto`）。
- 可通过 `blacklist_mode` 切换来源（table_only/rules_only/disabled），或用 `blacklist_rules_enable/disable` 精细控制规则。
- LOB 体积阈值由 `blacklist_lob_max_mb` 控制（默认 512MB）。
- 当使用 `blacklist_mode=auto` 或 `rules_only` 时，请确保 `blacklist_rules.json` 随工具部署；缺失时规则会被跳过。
- 推荐用 `exclude_objects_file` 维护“明确不参与校验”的对象清单（`TYPE|SCHEMA|OBJECT`）；未配置时不生效，继续原有逻辑。

## 常见配置片段
**只看表结构，不生成修复：**
```ini
check_primary_types = TABLE
generate_fixup = false
check_dependencies = false
```

**全量比对 + 修复脚本：**
```ini
check_primary_types =
check_extra_types = INDEX,CONSTRAINT,SEQUENCE,TRIGGER
generate_fixup = true
generate_grants = true
```

**扩展对象校验加速：**
```ini
extra_check_workers = 16
extra_check_chunk_size = 200
extra_check_progress_interval = 10
```

**授权脚本压缩：**
```ini
grant_tab_privs_scope = owner
grant_merge_privileges = true
grant_merge_grantees = true
```

## 已知限制与注意事项
- **字符串/注释中的特殊语法**：DDL 清洗与脚本拆分主要面向常见语法，遇到复杂 `q'[...]'` 或极端注释格式可能需要人工调整。
- **初始化口令策略**：用户/角色初始化口令为运行时输入，请在操作流程中统一口令与改密策略。
- **配置含 `%` 字符**：部分环境下 `configparser` 会对 `%` 做插值，建议避免直接使用或改为转义。

## 项目结构速览
| 路径 | 说明 |
| --- | --- |
| `schema_diff_reconciler.py` | 主程序：对比、推导、报告、fixup 生成 |
| `run_fixup.py` | 修复脚本执行器（smart-order/迭代/view-chain） |
| `init_users_roles.py` | 用户/角色初始化 |
| `docs/` | 详细文档 |
| `readme_config.txt` | 配置项完整说明 |

## 更多文档
1) `readme_config.txt`：配置项与默认值
2) `docs/README.md`：docs 目录入口与阅读顺序
3) `docs/ADVANCED_USAGE.md`：Remap 推导与 run_fixup 高级说明
4) `docs/ARCHITECTURE.md`：架构设计与实现细节
5) `docs/DEPLOYMENT.md`：离线部署与跨平台运行
6) `docs/TECHNICAL_SPECIFICATION.md`：技术规格说明

---
© 2025 Minor Li.
