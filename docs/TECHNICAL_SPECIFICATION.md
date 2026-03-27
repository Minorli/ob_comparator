# Oracle → OceanBase 结构一致性校验与修复引擎
## 技术规格说明 (Technical Specification)

**版本**：0.9.8.8  
**日期**：2026-03-13  
**适用场景**：Oracle → OceanBase（Oracle 模式）迁移后的结构一致性校验、对象补全、DDL 兼容性修复。

---

## 1. 系统定位
本工具是迁移后的结构一致性审计与修复脚本生成引擎。核心理念是 **“Dump-Once, Compare-Locally + Script-First”**：
- 所有元数据一次性拉取到本地内存进行比对
- 修补方案输出为可审计 SQL，不自动执行
- 依赖、授权、DDL 清洗形成闭环

## 2. 输入与控制面

### 2.1 配置文件
- `config.ini`：连接信息、超时、输出目录、功能开关
- `remap_rules.txt`：显式 remap 规则
- `trigger_list`：触发器过滤清单（可选）

### 2.2 外部依赖
- Oracle Instant Client + `oracledb` Thick Mode
- OceanBase `obclient`
- JDK + `dbcat`
- SQLcl（可选，用于 DDL 格式化）
- Python 运行时兼容基线保持 3.7，项目代码中的泛型注解使用 `typing` 兼容别名，不引入 3.9+ 内建泛型下标运行时语法

### 2.3 可配置开关（核心）
- `check_primary_types` / `check_extra_types`
- `generate_fixup` / `generate_grants`
- `check_dependencies` / `check_comments`
- `infer_schema_mapping` / `ddl_punct_sanitize` / `ddl_hint_policy`
- `ddl_format_enable` / `ddl_format_types` / `sqlcl_bin`（可选格式化）
- `report_dir_layout` / `report_detail_mode`（报告目录与明细拆分）
- `fixup_drop_sys_c_columns`（SYS_C* 额外列清理策略）

### 2.4 默认规则与硬编码说明
为保证大多数迁移场景的稳定性，程序内置了一批默认规则（如 OMS 列过滤、SYS_NC/SYS_C 处理、视图兼容性判断等）。  
当前认为这些规则在 **Oracle → OceanBase（Oracle 模式）** 场景下适用面较广，默认保持开启且不强制暴露为配置项。  
若未来 OceanBase 版本或业务场景发生明显变化，可按需将具体规则配置化并保持默认值不变，以避免破坏现有行为。

---

## 3. 元数据采集

### 3.1 Oracle 侧
- 通过 Thick Mode 批量读取 `DBA_*`：对象、列、索引、约束、触发器、序列、依赖、权限、注释。
- IN 列表分批执行，规避 1000 项限制。

### 3.2 OceanBase 侧
- 通过 `obclient` 一次性读取 `DBA_*` 视图。
- 结果缓存至 `ObMetadata`，避免循环访问。

---

## 4. 映射与推导

### 4.1 Remap 规则优先级
1) 显式 remap 规则
2) 默认保持原 schema 的类型
3) 依附对象跟随父表
4) 依赖推导
5) schema 映射回退

### 4.2 默认保持原 schema 的类型
- VIEW / MATERIALIZED VIEW / TRIGGER / PACKAGE / PACKAGE BODY

### 4.3 依附对象
- INDEX / CONSTRAINT / SEQUENCE / SYNONYM
- 跟随父表 schema（只改 schema，不改对象名）

### 4.4 依赖推导
- PROCEDURE / FUNCTION / TYPE / SYNONYM
- 基于依赖目标 schema 频次统计推导

### 4.5 受管目标 Scope
- `source_schemas` 只定义源端扫描范围，不直接等于目标端受管 schema。
- 主流程会基于 `full_object_mapping` 统一推导 `managed target scope`，作为目标端元数据、依赖、fixup、授权审计、report_db 的共同输入。
- remap 到“配置中不存在的新目标 schema”时，这些 schema 仍属于本轮受管范围。
- 会输出 `managed_target_scope_detail_<ts>.txt`，列出：
  - 本轮受管目标 schema
  - 是否也出现在 `source_schemas`
  - 由哪些源 schema 推导而来
  - 该目标 schema 下的受管对象数

---

## 5. 对比规则

### 5.1 TABLE
- 列集合对比（忽略 OMS_* 和隐藏列）
- VARCHAR/VARCHAR2 长度窗口校验
- 现有列 `NULLABLE` / `NOT NULL` 语义漂移校验（按列语义处理，不依赖系统命名 `SYS_C... IS NOT NULL` 约束名）
- 覆盖系统命名 `SYS_C... IS NOT NULL` 且 `ENABLED + NOT VALIDATED` 的 `NOT NULL ENABLE NOVALIDATE` 语义补位；该类进入 `TABLE mismatch`，并在 `table_alter` 中默认输出可执行 `ADD CONSTRAINT ... ENABLE NOVALIDATE`，同时保留严格 `NOT NULL` 的 review-first 注释
- OB 侧等价 `CHECK (<col> IS NOT NULL)` suppress 依赖 `DBA_CONSTRAINTS.SEARCH_CONDITION[_VC]`；元数据加载采用按 chunk 保留成功结果 + 退化 owner/table/constraint 定向回填，避免个别 chunk 失败导致整批 `SEARCH_CONDITION` 丢失
- OceanBase 自动 `*_OBCHECK_*` / `*_OBNOTNULL_*` 约束在普通约束 compare 中继续降噪，但其单列 `IS NOT NULL` 语义必须保留给 `TABLE` compare 使用
- 当目标端同一列存在多份 enabled 的等价单列 `IS NOT NULL` CHECK、且源端仅保留一份等价语义时，约束 compare 会把多余约束识别为 `extra mismatch`；`generate_extra_cleanup` 默认开启，会在 `cleanup_candidates/extra_cleanup_candidates.txt` 中输出 `SAFE_DUPLICATE_NOTNULL_DROP_SQL` 原始清理语句，并额外生成 `cleanup_safe/constraint/*.sql`；`cleanup_safe/` 默认不进入 `run_fixup` 自动执行，需显式按目录执行
- 当 `extra_constraint_cleanup_mode=semantic_fk_check` 时，compare 后仍判定为 target-only 的 `FK/CHECK` 会额外输出到 `cleanup_candidates/extra_cleanup_candidates.txt` 的 `SEMANTIC_EXTRA_CONSTRAINT_DROP_SQL` 区域，并生成 `cleanup_semantic/constraint/*.sql`；该目录默认不进入 `run_fixup` 自动执行，需显式按目录执行
- 源端系统命名的单列 `IS NOT NULL` CHECK 仅在 `ENABLED` 时按列语义降噪；若该类约束为 `DISABLED`，则按普通 CHECK 参与缺失 compare、unsupported 过滤与 metadata fallback fixup，生成的缺失约束脚本会保留 `DISABLE` 状态
- 列默认值 compare 继续使用 canonical case-insensitive normalize；但 `column_default_detail`、主报告 mismatch 文本与 `table_alter` review-first SQL 中的默认值表达式会保留源端 display form，避免仅因函数名大小写而制造输出噪声
- 普通 `NOT NULL` 收紧默认采用 `plain_not_null_fixup_mode=runnable_if_no_nulls`；会先探测目标端是否存在 `NULL`，仅无 `NULL` 时输出可执行 `MODIFY ... NOT NULL`，否则继续保留注释
- 当 `column_visibility_policy=auto` 且两端 `INVISIBLE_COLUMN` 元数据不完整时，不改变 compare/fixup 结论，但会输出独立 `column_visibility_skipped_detail` 说明本轮跳过范围
- identity 列模式差异校验（`GENERATED ALWAYS` / `BY DEFAULT` / `BY DEFAULT ON NULL`），以 TABLE DDL 提取为主，不只依赖 `IDENTITY_COLUMN`
- 当 identity 模式一致时，还会比较稳定细项子集 `START WITH / INCREMENT BY / CACHE`，并输出独立 `column_identity_option_detail`；首版仅 review-first，不生成 runnable identity SQL
- `DEFAULT ON NULL` 语义漂移校验采用字典 + TABLE DDL 兜底提取；支持双向 compare，并输出独立 `column_default_on_null_detail`
- 现有列 `DATA_DEFAULT` 语义漂移校验（按列语义处理；`NULL` 与无默认值按等价处理）
- OB 侧 CHAR_USED 缺失时，按 NLS_LENGTH_SEMANTICS（默认 BYTE）回退，并结合 DATA_LENGTH/CHAR_LENGTH 推断 CHAR 语义
- OB 列字典读取联合 `DBA_TAB_COLUMNS` 与 `DBA_TAB_COLS`；前者作为标准字段基线，后者补齐 `HIDDEN_COLUMN / VIRTUAL_COLUMN` 等附加元数据与缺失值
- LONG/LONG RAW 自动映射为 CLOB/BLOB

### 5.2 VIEW / PLSQL / TYPE / SYNONYM / JOB / SCHEDULE
- 存在性校验
- VIEW 兼容性分析：SYS.OBJ$ / X$ 系统对象视为不支持（用户自建 X$ 对象除外）
- VIEW 依赖 remap 会同时处理 unquoted / quoted qualified 引用；当同义词终点无法安全解析时，不做 schema-only 盲改，而是优先 fallback 到受管目标同义词对象本身；若仍无法确认，则保留原引用并输出诊断日志
- 当 `source_object_scope_mode=remap_root_closure` 时，源对象范围不再按 `source_schemas` 全量纳入，而是仅从 `remap_file` 中显式 TABLE/VIEW roots 出发，按依赖/附属关系扩展闭包；闭包外对象（含其相关 INDEX/CONSTRAINT/SYNONYM/SEQUENCE/TRIGGER）整体不进入 compare/fixup/report，`trigger_list` 可作为显式 keep set 保留触发器及其父对象
- scoped mode 会输出 `source_scope_detail_<ts>.txt`，记录 remap roots、显式 trigger keep、闭包纳入对象和被过滤对象，供客户核对“不多对象、不少对象”边界
- 当 `blacklist_target_existing_policy=rehydrate_if_present` 时，若源端阻断型黑名单 TABLE 在目标端已真实存在，则会进入“重纳管”模式：表本体恢复 compare/fixup，但黑名单改造列不会再自动回写 Oracle 原始语义；依赖这些列的 INDEX/CONSTRAINT 转为 manual/report-only，TRIGGER 在 v1 中继续保持人工处理
- PUBLIC 同义词按 Oracle 语义处理（OB `__public` 归一化为 `PUBLIC`）
- 若 SYNONYM 的终点对象不在本次迁移范围（含同义词链最终落到范围外对象），该 SYNONYM 会被分类为 `BLOCKED`，写入 unsupported/detail 报告，且不生成 normal synonym fixup DDL
- 同义词的“源端终点对象是否受管”与“目标端 schema 是否受管”分开处理；不会再把 `source_schemas` 误当 target allowlist 使用
- SYNONYM 对象自身的显式 remap 只影响目标对象命名，不会豁免终点对象受管性校验；若 terminal source target 不在源端受管范围，仍会判为 `SYNONYM_TARGET_OUT_OF_SCOPE`
- PUBLIC 同义词元数据预加载会保留 `TABLE_OWNER='PUBLIC'` 的中间节点，用于解析 `PUBLIC -> PUBLIC -> ...` 链式同义词
- PUBLIC 同义词进入 compare/fixup 前，会额外按 terminal source scope 过滤；只有终点落在受管源范围内的 `PUBLIC` 链路会被纳入，Oracle 系统 `PUBLIC` 链路不会再批量进入校验
- target extra grant audit 以 `managed target scope` 派生的“受管 target object 集合”为准；owner 只用于目录取数，不再把同 schema 下未受管对象的授权误判为 extra grant
- `constraint_status_sync_mode` 默认值为 `full`；现有 `FK/CHECK` 的 `VALIDATED / NOT VALIDATED` 状态漂移会默认进入状态修复逻辑，`PK/UK` 的 `VALIDATED / NOT VALIDATED` 漂移也会进入状态漂移报告，但仍不生成 `ENABLE/[NO]VALIDATE` SQL
- 对“缺失 TABLE 首次创建”场景，若源端 `FK/CHECK` 为 `ENABLED + NOT VALIDATED`，fixup 会额外输出 `status/constraint/*.status.sql` 的后置 `ENABLE NOVALIDATE`，避免 `CREATE TABLE` 兼容清洗后把目标端默认建成 `VALIDATED`

### 5.2.1 Report DB 语义
- `DIFF_REPORT_DETAIL` / `DIFF_REPORT_DETAIL_ITEM` 对支持性分类采用：
  - `SUPPORTED -> report_type='MISSING'`
  - `UNSUPPORTED/BLOCKED -> report_type='UNSUPPORTED'`
  - `RISKY -> report_type='RISKY'`
- `DIFF_REPORT_ACTIONS_V` 会把 `report_type='RISKY'` 归类到 `REVIEW`
- 文本主报告与 `unsupported_objects_detail_*.txt` 仍保持“缺失(不支持/阻断/待确认)”聚合口径，不单独拆出主报告 `RISKY` 计数列

### 5.3 PACKAGE / PACKAGE BODY
- 有效性校验（`DBA_ERRORS` 摘要）

### 5.4 INDEX
- 按列序列 + 唯一性匹配
- 兼容 SYS_NC 列名差异

### 5.5 CONSTRAINT
- PK/UK/FK 按列序列匹配
- 忽略 `_OBNOTNULL_` 约束
- FK 额外比对 `DELETE_RULE` / `UPDATE_RULE`
- OB `DBA_CONSTRAINTS` 若退化到 basic/degraded 模式，FK 会定向回填 `R_OWNER/R_CONSTRAINT_NAME` 后再解析被引用表，避免仅按本地列集合接受语义不等价的目标 FK
- 自引用外键已按普通 FK compare/fixup 路径处理，不再一刀切视为不支持

### 5.6 SEQUENCE
- 按 schema 映射比较集合

### 5.7 TRIGGER
- 目标存在性 + 触发事件与状态
- 触发器头部 remap 兼容 `CREATE OR REPLACE [NON]EDITIONABLE TRIGGER`
- `trigger_qualify_schema=false` 的 legacy 最小 remap 分支同样兼容该头部，至少保证 `ON <table>` remap 不受影响
- `ON <table>` 目标表 remap 与事件头保护解耦，避免将关键字 `ON/OR` 误当对象名补 schema

### 5.8 PROCEDURE/FUNCTION/PACKAGE/TYPE
- 非 TRIGGER 的 PL/SQL 对象继续使用 `remap_plsql_object_references()`
- 已限定 `SCHEMA.OBJECT` 引用支持：
  - 双引号形式 `"SCHEMA"."OBJECT"`
  - 对象名带 `$` / `#`
- 注释与字符串字面量仍通过 `SqlMasker` 保护，不参与该类 remap

### 5.9 对象可用性（可选）
- VIEW/SYNONYM 可用性校验：`SELECT * FROM <obj> WHERE 1=2`
- 支持源端对照（判断预期不可用）、超时保护与并发执行
- 明细输出根因与建议（依赖缺失/权限不足/不支持阻断），与依赖链/不支持分类联动

---

## 6. 依赖与授权

### 6.1 依赖校验
- `DBA_DEPENDENCIES` 构建期望依赖集合
- 与 OB 实际依赖比对，输出缺失/多余依赖

### 6.2 授权生成
- 基于 `DBA_TAB_PRIVS`、`DBA_SYS_PRIVS`、`DBA_ROLE_PRIVS`
- 支持权限合并与白名单过滤
- 视图权限拆分：依赖对象授权输出到 `view_prereq_grants/`，视图自身授权输出到 `view_post_grants/`
- 视图链路要求 `WITH GRANT OPTION` 的场景会单独标注缺失
- 依赖推导不再对 PUBLIC 生成授权，仅保留源端显式 PUBLIC 授权
- 输出 `grants_miss/` 与 `grants_all/`

---

## 7. 修补脚本生成

### 7.1 DDL 获取
- dbcat（批量导出）
- DBMS_METADATA（VIEW 兜底；TRIGGER 缺失 dbcat 时亦可回退）
- `dbcat_output/cache/` 扁平缓存优先按实际对象文件命中；若 `cache/index.json` 漏项但文件存在，运行时会自动修复索引并继续使用 cache

### 7.2 DDL 清洗与兼容
- Hint 策略过滤
- PL/SQL 结尾修正
- 仅对已实证不兼容的 Oracle 语法做自动清洗
- `PRAGMA AUTONOMOUS_TRANSACTION` / `PRAGMA SERIALLY_REUSABLE` 默认保留
- `STORAGE(...)` / `TABLESPACE ...` 默认保留，并在 `ddl_cleanup_detail_<ts>.txt` 中记录为 preserved
- `LONG/LONG RAW/BFILE` 等类型改写标记为 `semantic_rewrite`
- VIEW 行内注释修复
- VIEW FORCE 关键字清理（CREATE OR REPLACE FORCE VIEW -> CREATE OR REPLACE VIEW）

### 7.3 输出目录
- `fixup_scripts/table/` / `table_alter/`
- `fixup_scripts/view_prereq_grants/`
- `fixup_scripts/view/`
- `fixup_scripts/view_post_grants/`
- `fixup_scripts/compile/`
- `fixup_scripts/grants_miss/`

---

## 8. run_fixup 执行语义

### 8.1 Smart Order
按依赖层级执行（sequence → table → grants → view → code → index/constraint → trigger）。
默认安全策略下，`table` 目录会被 run_fixup 排除；仅在显式 `--allow-table-create` 时执行。

### 8.2 Iterative 模式
失败脚本自动重试，直至收敛或达到最大轮次。

### 8.3 VIEW 链路自动修复
依据 `VIEWs_chain_*.txt` 生成计划并执行，每个 VIEW 独立输出 plan/sql。

### 8.4 错误报告
失败语句汇总到 `fixup_scripts/errors/`，便于集中排查。

### 8.5 安全与保护
- `fixup_max_sql_file_mb` 限制单个 SQL 文件读取大小，超限脚本会被跳过并记录错误。
- `fixup_dir_allow_outside_repo=false` 时，run_fixup 不允许 fixup_dir 指向项目外目录。
- 成功执行脚本移动到 `done/` 前会尝试备份同名文件；备份失败会阻断覆盖，避免历史结果被冲掉。
- 自动补权限缓存支持 `fixup_auto_grant_cache_limit` 控制，避免长时间运行内存膨胀。
- 默认跳过 `fixup_scripts/table/`，防止误创建空表；需要显式 `--allow-table-create` 才可执行建表脚本。
- `run_fixup` 采用 `.run_fixup.lock` 防止同目录并发重入。
- `run_fixup` 采用 `.fixup_state_ledger.json` 防止“已执行但移动失败”脚本被重复执行。

---

## 9. 报告体系
- `run_<ts>/report_<ts>.txt`：主报告
- `run_<ts>/report_sql_<ts>.txt`：轻量入口文件，仅包含 `report_id` 与 HOW TO 手册入口
- `HOW_TO_READ_REPORTS_IN_OB_latest.txt` / 当前快照文件：数据库侧排障手册，供人工查阅
- `run_<ts>/package_compare_<ts>.txt`：包对比明细
- `run_<ts>/remap_conflicts_<ts>.txt`：推导冲突
- `run_<ts>/VIEWs_chain_<ts>.txt`：VIEW 链路
- `unsupported_<TYPE>_detail_*.txt`：按类型不支持明细（含 ROOT_CAUSE，如 VIEW_X$ 及命中对象）
- `filtered_grants.txt`：过滤权限
- `grant_capability_detail_<ts>.txt`：动态授权规则库明细（支持结果、目录别名、最终决策）
- `triggers_non_table_detail_<ts>.txt`：非表触发器明细（当前主要为 DATABASE/SCHEMA 级事件触发器；`INSTEAD OF ... ON VIEW` 已进入普通 compare/fixup）

---

## 10. 性能与可靠性
- 大部分逻辑在内存执行，避免高频 DB 访问。
- 可配置超时与并发线程数。
- dbcat 输出缓存复用，减少重复扫描。
- 扁平 cache 索引漏项不会再直接导致 metadata fallback；对象文件存在时会自修复 `cache/index.json`。
- `table_data_presence_check=auto` 对 `NUM_ROWS=0` 会做二次探针确认，降低统计信息滞后造成的误判。
- `table_data_presence_zero_probe_workers` 控制 Oracle 零行探针并发（默认 1，最大 32）。

---

## 11. 安全与审计
- 主程序只执行 SELECT。
- 修补脚本需人工审核执行。
- 输出包含完整变更线索与摘要。

---

## 12. 已知限制
- 对极端复杂 DDL 仍建议人工复核清洗结果（例如多层动态 SQL 与复杂嵌套注释组合）。
- `init_users_roles.py` 通过交互输入初始口令，仍建议上线后执行统一改密策略。

---

## 13. 交付前验证基线
- 语法检查：`python3 -m py_compile $(git ls-files '*.py')`
- 单元测试：`.venv/bin/python -m unittest discover -v`
- 可选联调：在测试库执行 `schema_diff_reconciler.py` 与 `run_fixup.py --glob "__NO_MATCH__"` 验证整体链路。
- 当前版本不再在文档中硬编码测试条数；交付时应记录本次实际执行命令、通过/失败结果与是否完成实库验证。
