# OceanBase Comparator Toolkit

> 当前版本：V0.9.8.9  
> 面向 Oracle → OceanBase (Oracle 模式) 的结构一致性校验与修补脚本生成工具  
> 核心理念：一次转储、本地对比、脚本审计优先

## 近期更新（0.9.8.9）
- VIEW 授权依赖补强：对 `ORA-01720` 敏感的 VIEW 授权链，程序会生成 `view_prereq_grants/`；如果目标端 VIEW 已存在且 prerequisite grants 是后补的，还会额外生成 `view_refresh/`。`run_fixup` 在执行 `view_post_grants/` 命中 `ORA-01720` 时，也会先应用 prerequisite grants，必要时执行匹配的 `view_refresh/`，再重试最终 VIEW grant。
- 黑名单表重纳管增强：`blacklist_target_existing_policy=rehydrate_if_present` 已进入正式版本；当目标端已存在人工改造后的承接表时，可恢复后续 compare/fixup，并自动保护黑名单改造列不被写回 Oracle 原始语义。
- 触发器边界更准确：`INSTEAD OF ... ON VIEW` 触发器已纳入正常 compare/fixup；`DATABASE/SCHEMA` 级事件触发器继续保留为人工处理。
- Oracle 派生表降噪补齐：`RUPD$_*`、`SNAP$_*` 与既有 `MLOG$_*` 一样按系统工件从 compare/fixup 中排除。
- identity 跨 schema 授权增强：会识别 OB identity 底层 `ISEQ$$_...`，对源端已授 `INSERT` 的 identity 表额外检查目标端 sequence `SELECT` 是否缺失，并输出 `identity_sequence_grant_detail_<ts>.txt` 与 `grants_miss/` 补授权脚本。
- `remap_root_closure + trigger_list` 收口：`trigger_list` 现在可接受源端名或 remap 后目标名；未解析条目只进入报告不再 fatal。若触发器依赖的目标表尚未创建，首轮只生成 TABLE 脚本，不生成 TRIGGER 脚本，并在 `fixup_skip_summary_<ts>.txt` 中解释跳过原因。
- README / `readme_config.txt` / 技术文档当前版本号已同步到 `0.9.8.9`。

## 核心能力
- **对象覆盖完整**：TABLE/VIEW/MVIEW/PLSQL/TYPE/JOB/SCHEDULE + INDEX/CONSTRAINT/SEQUENCE/TRIGGER。
- **Dump-Once 架构**：Oracle Thick Mode + 少量 obclient 调用，元数据一次性落本地内存。
- **Remap 推导**：支持显式规则、依附对象跟随、依赖推导、schema 回退策略。
- **源范围收缩模式**：支持 `source_object_scope_mode=remap_root_closure`，仅以 `remap_file` 中显式 TABLE/VIEW 为根种子，按依赖、反向依赖、附属关系扩展闭包；闭包外对象整体忽略，`trigger_list` 可作为显式 keep set。
- **scoped 文本补盲**：支持 `remap_scope_text_fallback_mode=safe`，仅从 `DBA_SOURCE` / `DBA_VIEWS.TEXT` / `DBA_MVIEWS.QUERY` / scheduler 文本等受控来源补盲；支持受控 dynamic SQL、纯字符串拼接 SQL 以及 same-schema 未带前缀调用补盲，不走全 schema DDL grep。变量拼接 SQL 仅报告，不自动纳入。
- **黑名单表重纳管**：支持 `blacklist_target_existing_policy=rehydrate_if_present`；当源端黑名单表已在目标端被人工改造并创建为 TABLE 时，可恢复后续 compare/fixup，但会自动保护黑名单改造列，避免把 Oracle 原始类型/长度/default/nullability 再写回 OB。
- **Target Scope 一等公民**：目标端受管 schema 不再等同于 `source_schemas`；会按 remap/full mapping 自动推导，哪怕 remap 到全新的目标 schema，也会继续进入 compare/fixup/report。
- **Mapping 分层可审计**：`object_mapping_<ts>.txt` 表示本轮受管 managed mapping；若 closure 内部还发现了未纳入 compare/fixup 的 related object，会额外输出 `object_mapping_discovery_<ts>.txt`。
- **依赖与授权**：基于 DBA_DEPENDENCIES/DBA_*_PRIVS 生成缺失依赖与授权脚本。
- **identity sequence 授权感知**：跨 schema identity 表会额外检查 OB 侧 `ISEQ$$_...` 的 `SELECT` 授权 readiness，避免“表 grant 齐了但 INSERT 仍失败”。
- **DDL 清洗与兼容**：VIEW DDL 走 DBMS_METADATA，PL/SQL 语法清洗与 Hint 过滤。
- **DDL 输出格式化**：可选 SQLcl 格式化 fixup DDL（不影响校验与修补逻辑）。
- **修补脚本执行器**：支持 smart-order、迭代重试、VIEW 链路自动修复、错误报告。
- **报告体系**：Rich 控制台 + 纯文本快照 + 细节分拆报告（可配置）。
- **空表风险识别**：可选识别“源端有数据但目标端空表”的高风险场景（不做 `COUNT(*)`）。
- **不支持对象识别**：黑名单/依赖阻断对象单独统计与分流输出。

## 新增能力总览（按模块）
- **报告入库**：`report_to_db` 支持 `summary/core/full` 多级落库与行化明细，支持数据库侧直接排查（含 HOW TO SQL 手册）。
  `DIFF_REPORT_DETAIL` / `DIFF_REPORT_DETAIL_ITEM` 现在会保留 `RISKY` 作为独立 `report_type`；但主报告与 `unsupported_objects_detail_<ts>.txt` 仍继续按“缺失(不支持/阻断/待确认)”汇总展示。
- **迁移聚焦报告**：按“可修补缺失 vs 不支持/阻断”拆分，保留全量明细并可在主报告快速定位。
- **可用性校验**：支持 VIEW/SYNONYM 查询可用性检查（`WHERE 1=2`）与根因输出。
- **表数据风险校验**：`table_data_presence_check` 识别“源端有数据、目标空表”风险；`auto` 为统计口径，`on` 为严格回表。
- **对象时间截断**：`object_created_before` 可按 `CREATED` 截止时间冻结校验范围，支持缺失 CREATED 策略（`strict/include_missing/exclude_missing`），并输出 `objects_after_cutoff_detail_<ts>.txt` 与 report_db 对齐明细。
- **run_fixup 增强**：支持 `--iterative`、`--view-chain-autofix`、语句级继续执行、授权修剪与错误报告。
- **run_fixup 安全门禁**：默认跳过 `fixup_scripts/table/`，需显式 `--allow-table-create` 才执行建表脚本。
- **状态漂移修复**：支持 TRIGGER/CONSTRAINT 的状态差异检测与状态修补脚本生成。
- **约束策略增强**：支持缺失约束的 `safe_novalidate/source/force_validate` 策略；仅当源端最终语义需要 `VALIDATED` 时，才生成后置 `validate_later` 脚本。对“缺失 TABLE 首次创建”场景，若源端 `FK/CHECK` 为 `ENABLED + NOT VALIDATED`，会同轮追加 `fixup/status/constraint/*.status.sql` 恢复 `ENABLE NOVALIDATE` 语义。
- **约束状态修复默认更严格**：`constraint_status_sync_mode` 默认改为 `full`，现有 `FK/CHECK` 的 `VALIDATED / NOT VALIDATED` 漂移会默认进入状态校验与状态修复脚本；`PK/UK` 的 `VALIDATED / NOT VALIDATED` 漂移也会进入状态漂移报告，但仍不生成 `ENABLE/[NO]VALIDATE` SQL。
- **OB FK 元数据增强**：当 OceanBase `DBA_CONSTRAINTS` 退化到 basic/degraded 模式时，会对受影响 FK 定向回填 `R_OWNER/R_CONSTRAINT_NAME`，避免仅按本地列集合误判 FK 已匹配。
- **视图兼容治理**：支持 VIEW 兼容规则、DBLINK 策略、列清单约束清洗与依赖链修复。
- **DDL 清洗与格式化**：支持 `ddl_cleanup_detail_<ts>.txt` 审计明细、全角标点清洗、hint 策略清洗、SQLcl 格式化（可按类型/体积/超时控制）；语义改写会在脚本头写 `DDL_REWRITE` 注释。
- **黑名单与排除机制**：支持规则引擎、名称模式、显式排除清单（`exclude_objects_file`）与依赖联动过滤。
- **版本门控**：按 OB 版本动态处理 MVIEW、interval 分区等能力差异，降低跨版本误报。
- **GTT 改造模式**：支持把 Oracle 全局临时表按普通 TABLE 受管并输出改造 DDL，或保留原始 GTT 语义；两种模式下都不会进入 OMS 数据迁移规则。

## 适用场景
- Oracle → OceanBase 迁移后的结构一致性审计
- OMS 仅迁移表结构/数据后，补齐非表对象与授权
- 多 schema Remap、依赖复杂、VIEW 链较长的迁移项目

## 环境与依赖
- Python 3.7+
- 运行时兼容基线按 Python 3.7 控制；项目代码中的泛型注解统一使用 `typing` 别名，不引入 `list[...]` / `frozenset[...]` 这类 3.9+ 运行时写法
- Oracle Instant Client 19c+（oracledb Thick Mode）
- obclient（目标 OceanBase 可连接）
- JDK + dbcat（DDL 批量提取）
- SQLcl（可选，用于 DDL 格式化）
- 运行账号需具备 DBA_* 视图访问权限（Oracle 与 OB）
- 安全说明：工具运行时不会把 OB/dbcat 密码作为明文参数暴露在 `ps` 命令中（配置文件仍按当前方式保留密码项）。

## 贡献方式

- 贡献规范见 [CONTRIBUTING.md](./CONTRIBUTING.md)。
- 新能力、高风险修复、兼容性策略变化建议使用 `issue + branch + PR`。
- 小范围低风险维护仍可直接提交，但应继续使用 `Refs #N` 关联已有 area issue，保留 GitHub 历史。

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
source_object_scope_mode = full_source
remap_scope_text_fallback_mode = off
scope_integrity_check = true
scope_integrity_check_depth = direct
scope_integrity_advisory_check = false
scope_integrity_fk_check = false
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

`dbcat_output/cache/` 扁平缓存以实际对象文件为准；如果 `cache/index.json` 漏项，但对应 `SCHEMA/TYPE/OBJ.sql` 存在，主程序会继续命中该缓存并自动修复索引。
`main_reports/run_<ts>/managed_target_scope_detail_<ts>.txt` 会列出本轮实际受管的目标 schema，明确哪些 schema 是仅由 remap 导出的新目标范围。
`main_reports/run_<ts>/object_mapping_<ts>.txt` 现在表示本轮真正参与 compare/fixup 的受管映射；若存在 closure 内部额外发现但未纳入 managed scope 的对象，会额外输出 `object_mapping_discovery_<ts>.txt` 供审计。

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
- **TRIGGER 头兼容**：即使源端 DDL 来自 `DBMS_METADATA`，带 `EDITIONABLE/NONEDITIONABLE TRIGGER` 头，也会正确 remap `ON <table>`，不会把关键字 `ON` 误改成对象名；`trigger_qualify_schema=false` 的 legacy 最小 remap 模式也兼容该头部。
- **非 TRIGGER 的 PL/SQL qualified ref**：`PROCEDURE/FUNCTION/PACKAGE/TYPE` 中已写成 `SCHEMA.OBJECT` 的引用，若对象名带双引号或带 `$` / `#`，也会按 remap 正常改写。
- **INDEX/CONSTRAINT/SEQUENCE** 默认跟随父表。
- **PROCEDURE/FUNCTION/TYPE/SYNONYM** 可通过依赖推导目标 schema。
- **目标范围说明**：`source_schemas` 只定义源端扫描范围；目标端实际受管 schema 由 remap/full mapping 自动推导，不要求预先写进 `config.ini`。

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
- `main_reports/run_<ts>/object_mapping_discovery_<ts>.txt`：discovery-only 对象映射（closure 内发现但未纳入 managed compare/fixup 的对象）
- `main_reports/run_<ts>/VIEWs_chain_<ts>.txt`：VIEW 依赖链报告
- `main_reports/run_<ts>/blacklist_tables.txt`：黑名单表清单
- `main_reports/run_<ts>/blacklist_rehydrated_detail_<ts>.txt`：黑名单表重纳管明细（目标端已存在且进入 rehydrate 的表、改造承接列、manual 边界）
- `main_reports/run_<ts>/filtered_grants.txt`：过滤授权清单
- `main_reports/run_<ts>/manual_actions_required_<ts>.txt`：本次必须人工处理/确认的统一清单（聚合 unsupported/deferred/review-first 项）
- `main_reports/run_<ts>/grant_capability_detail_<ts>.txt`：本次授权能力标定明细（含目标端目录权限别名，如 `DEBUG -> OTHERS`）
- `main_reports/run_<ts>/oracle_privilege_family_detail_<ts>.txt`：Oracle 权限族覆盖明细（区分 `RUNNABLE / MANUAL_ONLY`，当前 `DBA_COL_PRIVS` 已纳入 runnable grants，ACL/AQ/XS/Resource Manager 等仍先做盘点）
- `main_reports/run_<ts>/target_extra_grants_detail_<ts>.txt`：目标端额外对象授权明细（含 PUBLIC 扩权风险）
- `main_reports/run_<ts>/unsupported_grant_detail_<ts>.txt`：不进入 runnable grant 闭环的授权明细（含 Oracle 维护角色在目标端不存在、目标角色目录不可确认、OB 不支持权限等）
- `main_reports/run_<ts>/ddl_cleanup_detail_<ts>.txt`：DDL 清理/改写明细（区分 `format_only / syntax_compat / environment_detach / semantic_rewrite`，并标记 `evidence_level`）
- `main_reports/run_<ts>/trigger_status_report.txt`：触发器清单/状态差异报告
- `main_reports/run_<ts>/triggers_non_table_detail_<ts>.txt`：源端非表触发器明细（如 `BEFORE DROP ON DATABASE`）；`DATABASE/SCHEMA` 级事件触发器不会按普通 `trigger/` DDL 自动生成
- `main_reports/run_<ts>/triggers_temp_table_unsupported_detail_<ts>.txt`：临时表触发器不支持明细（`TRIGGER_ON_TEMP_TABLE_UNSUPPORTED`）；对应 DDL 仅输出到 `fixup_scripts/unsupported/trigger/` 供人工改造参考，不进入普通 `trigger/`
- `main_reports/run_<ts>/triggers_view_reference_detail_<ts>.txt`：触发器中引用视图的提醒明细（保留视图语义，不改写为表）
- `main_reports/run_<ts>/triggers_literal_object_path_detail_<ts>.txt`：触发器中 `SCHEMA.OBJECT.COLUMN` 字符串路径提醒明细（默认不自动改写）
- `main_reports/run_<ts>/table_data_presence_detail_<ts>.txt`：表数据存在性风险明细（源有数据/目标空表）
- `main_reports/run_<ts>/objects_after_cutoff_detail_<ts>.txt`：创建时间范围过滤对象明细（`FILTERED_BY_CREATED_AFTER_CUTOFF` / `FILTERED_BY_MISSING_CREATED`）
- `main_reports/run_<ts>/case_sensitive_identifiers_detail_<ts>.txt`：大小写敏感(双引号)标识符明细
- `main_reports/run_<ts>/sys_c_force_candidates_detail_<ts>.txt`：SYS_C* FORCE 候选表明细（用于评估是否开启 `fixup_drop_sys_c_columns`）
- `main_reports/run_<ts>/missing_objects_detail_<ts>.txt`：缺失对象支持性明细（report_detail_mode=split）
- `main_reports/run_<ts>/unsupported_objects_detail_<ts>.txt`：不支持/阻断对象明细（report_detail_mode=split）
- `main_reports/run_<ts>/source_scope_detail_<ts>.txt`：源对象范围明细（`source_object_scope_mode=remap_root_closure` 时的 roots/closure/filter 诊断，必要时追加 `INTEGRITY_CRITICAL` / `INTEGRITY_WARNING`）
- `main_reports/run_<ts>/scope_integrity_detail_<ts>.txt`：scope 完整性明细（blocking `VIEW/MVIEW` + 可选 advisory-family `INFO`）
- `main_reports/run_<ts>/scope_integrity_remap_candidates_<ts>.txt`：可直接补入 remap 文件的候选对象清单
- `main_reports/run_<ts>/extra_mismatch_detail_<ts>.txt`：扩展对象差异明细（report_detail_mode=split）
- `main_reports/run_<ts>/column_nullability_detail_<ts>.txt`：现有列空值语义差异明细（含 `NOT NULL`、`NOT NULL ENABLE NOVALIDATE` 与反向漂移；其中 `ENABLE NOVALIDATE` 补位会在 `table_alter` 中默认输出可执行约束 SQL）
- OceanBase 侧等价 `CHECK (<col> IS NOT NULL)` 识别依赖 `DBA_CONSTRAINTS` 条件文本；当前版本已改为按 chunk 保留成功的 `SEARCH_CONDITION`，并在退化时按表/约束回填，避免因个别 owner 查询失败而误生 `NOT NULL ENABLE NOVALIDATE` DDL
- OceanBase 自动生成的 `*_OBCHECK_*` / `*_OBNOTNULL_*` 约束会继续从普通约束噪声中抑制，但其 `IS NOT NULL` 语义仍会保留给 `TABLE` compare 使用，避免目标端已存在等价约束时重复生成 `table_alter` DDL
- 源端系统命名的单列 `IS NOT NULL` CHECK 仅在 `ENABLED` 时走列语义建模；若源端这类约束为 `DISABLED`，当前会按普通 CHECK 参与缺失 compare/fixup，并生成 `ADD CONSTRAINT ... CHECK (...) DISABLE`
- 当目标端同一列存在多份等价单列 `IS NOT NULL` CHECK、而源端仅保留一份语义时，扩展约束 compare 会把多余约束列为 `extra mismatch`，并在 `fixup/cleanup_candidates/extra_cleanup_candidates.txt` 的 `SAFE_DUPLICATE_NOTNULL_DROP_SQL` 区域输出未注释的 `DROP CONSTRAINT` 候选，同时生成 `fixup/cleanup_safe/constraint/*.sql`；`generate_extra_cleanup` 默认开启，但 `cleanup_safe/` 默认不会被 `run_fixup` 执行，需显式 `--only-dirs cleanup_safe/constraint`
- 当 `extra_constraint_cleanup_mode=semantic_fk_check` 时，compare 后仍判定为 target-only 的 `FK/CHECK` 会额外生成到 `fixup/cleanup_semantic/constraint/*.sql`；这类 SQL 同样属于 destructive cleanup，默认不会被 `run_fixup` 自动执行，需显式 `--only-dirs cleanup_semantic/constraint`
- VIEW 依赖 remap 现在会处理 `FROM/JOIN` 中的带引号 qualified 引用（如 `"SRC"."T1"`），并支持 `TABLE(...)`、`XMLTABLE(...)`、`JSON_TABLE(...)` 等特殊构造中的受管对象 token 重写；当同义词终点无法安全解析时，会保留原始引用并输出诊断日志，不再 fallback 到目标同义词名，也不会做盲目的 schema-only 改写
- scoped text matching（`JOB_ACTION` / `safe text fallback`）已改为索引化匹配：先从文本提取标识符 token，再只对命中 token 的候选对象做精确 regex；日志会额外输出 `[PERF] ... pattern_index / scan / round` 计数，便于区分“建索引慢”还是“扫文本慢”
- 列默认值 compare 继续按大小写不敏感语义比较（字符串字面量除外）；并额外对数值字面量、`DATE '...'`、`-(1)` 这类负号包裹数字、以及字符串字面量外部的尾部注释做语义归一，避免 `.98/0/DATE '1990-01-01'/user` 这类目标端字典表现差异被误报成 drift。报告和 review-first fixup SQL 仍尽量保留源端显示形式，但会剥掉 `USER--更新人` 这类非语义尾注释，避免把注释残片写回 SQL
- `main_reports/run_<ts>/column_visibility_skipped_detail_<ts>.txt`：`column_visibility_policy=auto` 且 `INVISIBLE_COLUMN` 元数据不完整时的跳过明细，说明哪些表本轮未做 INVISIBLE compare/fixup
- OceanBase 列元数据现在联合 `DBA_TAB_COLUMNS` 与 `DBA_TAB_COLS` 取长补短；标准字段优先保留 `DBA_TAB_COLUMNS`，可选标记/缺失值由 `DBA_TAB_COLS` 补齐，降低单视图元数据不一致带来的漏检
- 普通 `NOT NULL` 收紧默认改为 `plain_not_null_fixup_mode=runnable_if_no_nulls`：先探测目标端是否存在 `NULL`，仅无 `NULL` 时输出可执行 `MODIFY ... NOT NULL`；如需恢复纯 review-first，可改回 `review_only`
- `main_reports/run_<ts>/column_identity_detail_<ts>.txt`：现有列 identity 差异明细（含 `ALWAYS / BY DEFAULT / BY DEFAULT ON NULL` 模式差异；首版为 review-first）
- `main_reports/run_<ts>/column_identity_option_detail_<ts>.txt`：现有列 identity 细项差异明细（首批覆盖 `START WITH / INCREMENT BY / CACHE`；仅在 identity 模式一致时比较，首版为 review-first）
- `main_reports/run_<ts>/identity_sequence_grant_detail_<ts>.txt`：identity 表跨 schema 授权明细（目标端 `ISEQ$$_...` 定位结果、缺失 grant、人工确认项）
- `main_reports/run_<ts>/sequence_restart_detail_<ts>.txt`：sequence 值同步规划明细（Oracle/OB `LAST_NUMBER`、是否生成 restart、跳过原因）
- `main_reports/run_<ts>/fatal_error_matrix_<ts>.txt`：fatal 场景矩阵（哪些错误会直接终止、当前是否相关、如何修复）
- `main_reports/run_<ts>/column_default_on_null_detail_<ts>.txt`：现有列 `DEFAULT ON NULL` 语义差异明细（双向 compare；首版为 review-first）
- `main_reports/run_<ts>/column_default_detail_<ts>.txt`：现有列默认值差异明细（仅列级语义，不等同 `DEFAULT ON NULL`）
- `main_reports/run_<ts>/dependency_detail_<ts>.txt`：依赖差异明细（report_detail_mode=split）
- `*_detail_*.txt` 明细文件采用 `|` 分隔，并包含 `# total/# 字段说明` 头，格式与 `package_compare` 一致，便于 Excel 直接分隔导入。
- `main_reports/run_<ts>/missed_tables_views_for_OMS/`：OMS 缺失 TABLE/VIEW 规则；当 `source_object_scope_mode=remap_root_closure` 时，这里默认只导出 `remap_file` 中显式 TABLE/VIEW roots，不再把 closure 中的依赖对象一起导出
- `fixup_scripts/`：修补脚本输出（执行前需人工审核）
- `fixup_scripts/README_FIRST.txt`：fixup 根目录导航，说明本次生成目录的用途与默认执行边界；若存在人工项，会先指向 `manual_actions_required_<ts>.txt`
- `fixup_scripts/grants_miss/`：缺失授权脚本
- `fixup_scripts/grants_all/*.grants.sql` / `fixup_scripts/grants_miss/*.grants.sql`：现在既可能包含普通对象授权，也可能包含列级授权，如 `GRANT UPDATE (COL) ON OWNER.TABLE TO USER`
- `fixup_scripts/grants_revoke/`：目标端额外 PUBLIC 授权回收建议（默认仅 PUBLIC 自动给出 REVOKE）
- `fixup_scripts/grants_all/*.grants.sql` / `fixup_scripts/grants_miss/*.grants.sql`：对象/列授权文件在同一 owner 文件内按 `OBJECT_TYPE` 分段；当 `OBJECT_TYPE=TABLE` 时，还会细分 `TABLE_OBJECT_GRANTS` 与 `TABLE_COLUMN_GRANTS`，便于人工审核
- Oracle 维护角色授权现在会在每次运行时动态读取目标端 `DBA_ROLES` 做对比；只有目标端当前确实存在该角色时，才会保留到 `grants_all/grants_miss`。像 `EXP_FULL_DATABASE`、`DATAPUMP_*`、`SELECT_CATALOG_ROLE -> OB_CATALOG_ROLE` 这类角色若目标端不存在，会移到 `filtered_grants.txt` / `unsupported_grant_detail_<ts>.txt`，不再混进 runnable grant 文件
- 如果当前运行拿不到目标端 `DBA_ROLES`，Oracle 维护角色授权会统一降级成 report/manual，不会回退成默认放行
- `sequence_sync_mode=last_number` 开启后，会额外生成 `fixup_scripts/sequence_restart/`；脚本使用 `ALTER SEQUENCE ... RESTART START WITH <oracle_last_number>`，不采用固定 `+100` 偏移，并默认由 `run_fixup` 跳过。若本轮缺失 sequence 的 `CREATE SEQUENCE` 脚本已自带正确 `START WITH`，则不会重复生成 restart
- `fixup_scripts/tables_unsupported/`：不支持 TABLE 的 DDL（默认不执行）
- `fixup_scripts/unsupported/`：不支持/阻断对象 DDL（默认不执行）
- `fixup_scripts/view_chain_plans/`：VIEW 链路修复计划
- `fixup_scripts/errors/`：run_fixup 错误报告
- 若 SYNONYM 的终点对象不在本次迁移范围（含同义词链最终落到范围外对象），程序会把该 SYNONYM 标记为阻断，并写入 `unsupported_objects_detail_<ts>.txt` / `unsupported_synonym_detail_<ts>.txt`；同时不会再生成 `fixup_scripts/synonym/` DDL
- 即使 SYNONYM 对象本身存在显式 remap，程序仍会继续校验同义词链最终落点是否受管；显式 remap 不会绕过 terminal target scope 校验
- PUBLIC 同义词元数据会保留 `PUBLIC -> PUBLIC -> terminal object` 链路所需的中间节点，避免链式 PUBLIC 同义词被过早过滤
- PUBLIC 同义词进入 compare/fixup 前，会按“终点是否落在受管源范围”做终点语义过滤；Oracle 自带 `PUBLIC.ORACLE` / `PUBLIC.COM` / `PUBLIC.JAVAX` 等系统链路若终点不在本次迁移范围，将被自动排除
- 目标端额外授权审计现在按“本轮受管 target object 集合”收敛；即使某个旧 schema 仍因其他对象留在 `managed target scope` 内，也不会再把该 schema 下未受管对象的授权误判成 extra grant

## 触发器专项说明
- 触发器中的真实对象引用会继续按现有规则做 schema 补全和 remap。
- 如果触发器字符串字面量完整等于 `SCHEMA.OBJECT`，也会按 remap 自动改写，例如 `'LIFEBASE.T1' -> 'BASEDATA.T1'`。
- 如果字符串字面量是 `SCHEMA.OBJECT.COLUMN` 三段式路径，程序默认不自动改写，避免把列名、协议文本或日志内容误改坏；这类情况会输出到 `triggers_literal_object_path_detail_<ts>.txt` 供人工确认。
- 如果源端触发器是 `DATABASE/SCHEMA` 级事件触发器（例如 `BEFORE DROP ON DATABASE`），程序不会再静默漏掉；会输出到 `triggers_non_table_detail_<ts>.txt`，并在 `manual_actions_required_<ts>.txt` 中显式提醒。`INSTEAD OF ... ON VIEW` 会按普通受管触发器参与 compare/fixup。
- 当 `source_object_scope_mode=remap_root_closure` 且配置了 `trigger_list` 时，`trigger_list` 支持填写源端名或 remap 后目标名；若条目无法在源端或显式 remap 规则中解析，只会写入 `source_scope_detail_<ts>.txt` / `trigger_status_report.txt`，不会再中止整轮运行。
- 在 scoped trigger 场景下，如果触发器依赖的目标 TABLE/VIEW 尚未创建，首轮只会生成依赖对象脚本；TRIGGER 自身会在 `fixup_skip_summary_<ts>.txt` 中标记为 `base_table_missing` 或同类跳过原因，待依赖补齐后 rerun 再生成 trigger DDL。
- 对 `view_post_grants/` 中的 `GRANT SELECT/INSERT/UPDATE/DELETE ON <view> TO <grantee>`，若目标端因缺少底层 `WITH GRANT OPTION` 命中 `ORA-01720`，`run_fixup` 会按失败语句里的真实 privilege 自动补底层依赖授权；若 fixup 目录存在匹配的 `view_refresh/`，会先刷新 VIEW 再重试最终 VIEW grant，不再只把它记成普通权限不足。
- `grants_miss/` 现在会继续剔除明显不可执行的授权：目标对象当前不存在且本轮不会创建，或目标对象当前已是 `INVALID` 的授权，不再混进 runnable grants；这类会转入 `grants_deferred/` / `unsupported_grant_detail_<ts>.txt`。
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
