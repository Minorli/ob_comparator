# 高级使用指南

本手册聚焦六类高级能力：Remap 推导、授权生成、DDL 清洗、报告入库、run_fixup 高级执行、表数据风险校验。

---

## 1. Remap 推导与对象归属

### 1.1 规则优先级（高 → 低）
1) 显式 remap 规则（`remap_rules.txt`）
2) 不参与推导的类型：保持源 schema
3) 依附对象：跟随父表 remap
4) 依赖推导：根据 `DBA_DEPENDENCIES` 推断
5) schema 映射回退：一对一/多对一场景下使用主流 schema

### 1.2 默认保持原 schema 的类型
- VIEW / MATERIALIZED VIEW
- TRIGGER
- PACKAGE / PACKAGE BODY

> 这类对象需要显式 remap 才会改变 schema，但其内部引用仍会按 remap 规则替换。

### 1.3 依附对象跟随父表
- INDEX / CONSTRAINT / SEQUENCE / SYNONYM
- 规则：**只变 schema，不改对象名**

示例：
`SRC_A.IDX_ORDERS` 的父表 remap 到 `OB_A.ORDERS` → 结果为 `OB_A.IDX_ORDERS`

### 1.4 依赖推导适用类型
- PROCEDURE / FUNCTION
- TYPE / TYPE BODY
- SYNONYM（非 PUBLIC）

推导逻辑：统计依赖对象的目标 schema 频次；唯一则采用，多重则进入冲突清单。

### 1.5 冲突处理
当推导冲突或缺失时：
- 写入 `main_reports/remap_conflicts_*.txt`
- 该对象将跳过本轮修复（避免误判）

建议补齐显式 remap 后重跑。

---

## 2. 检查范围与修补范围控制

### 2.1 检查范围
- `check_primary_types`：主对象检查范围（TABLE/VIEW/PLSQL/TYPE/JOB/SCHEDULE）
- `check_extra_types`：扩展对象检查范围（INDEX/CONSTRAINT/SEQUENCE/TRIGGER）

### 2.2 修补范围
- `fixup_schemas`：仅生成指定目标 schema 的脚本
- `fixup_types`：仅生成指定对象类型脚本

### 2.3 触发器清单
- `trigger_list`：仅生成清单内 TRIGGER
- 缺失/非法条目会输出到 `main_reports/trigger_status_report.txt`

---

## 3. 授权生成与过滤策略

### 3.1 开关与范围
- `generate_grants`：生成授权脚本开关
- `grant_tab_privs_scope`：
  - `owner`：仅抽取源 schema 拥有的对象权限（推荐）
  - `owner_or_grantee`：兼容旧逻辑，规模更大

### 3.2 压缩策略
- `grant_merge_privileges = true`：合并同对象多权限
- `grant_merge_grantees = true`：合并同权限多 grantee

### 3.3 兼容过滤
- `grant_supported_sys_privs` / `grant_supported_object_privs`：白名单覆盖
- `grant_include_oracle_maintained_roles`：是否保留 Oracle 维护角色
- 过滤结果输出到 `main_reports/filtered_grants.txt`

### 3.4 VIEW 与同义词下探
- VIEW 被授予非 owner 时，会补齐 owner 对依赖对象的 `WITH GRANT OPTION`
- 同义词链路会下钻到最终目标对象生成授权

---

## 4. DDL 清洗策略

### 4.1 Hint 策略
- `ddl_hint_policy`：
  - `drop_all`：全部移除
  - `keep_supported`：保留 OB 支持的 Hint（默认）
  - `keep_all`：全部保留（仍受 denylist 控制）
  - `report_only`：保留全部，仅输出未知 Hint 报告

### 4.2 PL/SQL 语法修复
- 清理冗余分号/斜杠结尾
- 清理 Oracle 特有语法、PRAGMA
- 全角标点转半角（`ddl_punct_sanitize=true`）

### 4.3 VIEW DDL 修复
- 修复行内注释吞行
- 修复拆分列名（需列元数据命中）
- 版本感知：OB < 4.2.5.7 移除 `WITH CHECK OPTION`

### 4.4 DDL 输出格式化（SQLcl，可选）
- `ddl_format_enable=true` 启用 SQLcl 格式化，仅影响输出文件，不影响校验/修补逻辑。
- 默认仅格式化 VIEW；通过 `ddl_format_types` 指定其他类型（TABLE/PLSQL/INDEX 等）。
- 受 `ddl_format_max_lines` / `ddl_format_max_bytes` 限制，大对象会自动跳过。
- 性能调优：`ddl_format_batch_size`、`ddl_format_timeout` 控制批量与超时。
- 依赖：配置 `sqlcl_bin`；可选 `sqlcl_profile_path` 载入 SQL Developer 格式化规则。
- 报告输出：`main_reports/ddl_format_report_<timestamp>.txt`。

---

## 5. 报告与输出布局

### 5.1 目录布局
- `report_dir_layout=per_run`：每次运行输出到 `main_reports/run_<timestamp>`（默认）。
- `report_dir_layout=flat`：输出到 `main_reports/` 根目录（兼容旧流程）。

### 5.2 明细拆分
- `report_detail_mode=split`：主报告仅概要，明细拆分到 `*_detail_<timestamp>.txt`。
- 明细文件默认使用 `|` 分隔并带 `# 字段说明` 头，方便 Excel 直接导入。

### 5.3 报告存库（默认开启，obclient）
- `report_to_db=true` 后，会将报告写入 OceanBase（仅 obclient；不影响本地报告文件）。
- 运行后会在 run 目录输出 `report_sql_<timestamp>.txt`（预填 report_id 的 SQL 模板）。
- 当 `report_db_store_scope=core/full` 时，会尝试创建只读分析视图：
  `DIFF_REPORT_ACTIONS_V` / `DIFF_REPORT_OBJECT_PROFILE_V` / `DIFF_REPORT_TRENDS_V`
  `DIFF_REPORT_PENDING_ACTIONS_V` / `DIFF_REPORT_GRANT_CLASS_V` / `DIFF_REPORT_USABILITY_CLASS_V`。
- 支持 `report_db_schema` 指定存储 schema；留空使用目标连接用户。
- 写库范围由 `report_db_store_scope` 控制：
  - `summary`：仅写入 SUMMARY / COUNTS
  - `core`：summary + DETAIL / GRANT / USABILITY / TABLE_PRESENCE / PACKAGE / TRIGGER
  - `full`：core + ARTIFACT / ARTIFACT_LINE / DEPENDENCY / VIEW_CHAIN / REMAP_CONFLICT / OBJECT_MAPPING / BLACKLIST / FIXUP_SKIP / OMS_MISSING
  - `DIFF_REPORT_WRITE_ERRORS` / `DIFF_REPORT_RESOLUTION` 为写库追踪与闭环表，report_to_db 启用后默认创建
  - `full` 下会将 run 目录 txt 逐行写入 `DIFF_REPORT_ARTIFACT_LINE`，可直接在库中复盘全部文本报告
- 明细范围与规模控制：
  - `report_db_detail_mode=missing,mismatched,unsupported`（建议保留核心差异）
  - `report_db_detail_max_rows=0`（不限制；按需设置）
  - `report_db_detail_item_enable=`（空=auto，full 时启用行化表）
  - `report_db_detail_item_max_rows=0`（行化表不限制；按需设置）
  - `report_db_insert_batch=200`（INSERT ALL 批量）
- 清理策略：`report_retention_days=90`（0 表示不自动清理）。
- 写库表清单：
  - `DIFF_REPORT_SUMMARY` / `DIFF_REPORT_COUNTS` / `DIFF_REPORT_DETAIL` / `DIFF_REPORT_GRANT`
  - `DIFF_REPORT_DETAIL_ITEM`（明细行化表，便于逐项查询，store_scope=full 时写入）
  - `DIFF_REPORT_USABILITY`（可用性校验明细）
  - `DIFF_REPORT_TABLE_PRESENCE`（表数据存在性风险明细：源有数据/目标空表）
  - `DIFF_REPORT_PACKAGE_COMPARE`（PACKAGE/PKG BODY 对比摘要）
  - `DIFF_REPORT_TRIGGER_STATUS`（触发器状态差异）
  - `DIFF_REPORT_ARTIFACT`（报告工件目录）
  - `DIFF_REPORT_ARTIFACT_LINE`（报告文本逐行明细，含空行）
  - `DIFF_REPORT_DEPENDENCY`（依赖链）
  - `DIFF_REPORT_VIEW_CHAIN`（VIEW fixup 链路）
  - `DIFF_REPORT_REMAP_CONFLICT`（remap 冲突）
  - `DIFF_REPORT_OBJECT_MAPPING`（全量对象映射）
  - `DIFF_REPORT_BLACKLIST`（黑名单明细）
  - `DIFF_REPORT_EXCLUDED_OBJECT`（显式排除对象明细，来自 exclude_objects_file）
  - `DIFF_REPORT_FIXUP_SKIP`（fixup 跳过汇总）
  - `DIFF_REPORT_OMS_MISSING`（OMS 缺失规则映射）
  - `DIFF_REPORT_WRITE_ERRORS`（写库失败追踪）
  - `DIFF_REPORT_RESOLUTION`（整改闭环标记）
- 缺失/不支持明细无需额外表，使用 `DIFF_REPORT_DETAIL` 按 `report_type/object_type` 查询。

示例查询：
```sql
SELECT REPORT_ID, RUN_TIMESTAMP, TOTAL_CHECKED, MISSING_COUNT, MISMATCHED_COUNT, CONCLUSION
FROM DIFF_REPORT_SUMMARY
ORDER BY RUN_TIMESTAMP DESC
FETCH FIRST 10 ROWS ONLY;
```

按类型统计（对应“检查汇总”）：
```sql
SELECT OBJECT_TYPE, ORACLE_COUNT, OCEANBASE_COUNT, MISSING_COUNT, UNSUPPORTED_COUNT, EXTRA_COUNT
FROM DIFF_REPORT_COUNTS
WHERE REPORT_ID = '<report_id>'
ORDER BY OBJECT_TYPE;
```

缺失/不支持明细（按类型过滤）：
```sql
SELECT OBJECT_TYPE, SOURCE_SCHEMA, SOURCE_NAME, TARGET_SCHEMA, TARGET_NAME, REASON
FROM DIFF_REPORT_DETAIL
WHERE REPORT_ID = '<report_id>'
  AND REPORT_TYPE IN ('MISSING','UNSUPPORTED')
  AND OBJECT_TYPE = 'VIEW';
```

可用性校验明细：
```sql
SELECT SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE, STATUS, REASON
FROM DIFF_REPORT_USABILITY
WHERE REPORT_ID = '<report_id>'
ORDER BY STATUS, SCHEMA_NAME, OBJECT_NAME;
```
说明：`REASON` 为可用性根因（如依赖缺失/权限不足/不支持阻断），更完整信息在 `DETAIL_JSON`。
```sql
SELECT SCHEMA_NAME, OBJECT_NAME, STATUS,
       JSON_VALUE(DETAIL_JSON, '$.root_cause') AS ROOT_CAUSE,
       JSON_VALUE(DETAIL_JSON, '$.recommendation') AS RECOMMENDATION
FROM DIFF_REPORT_USABILITY
WHERE REPORT_ID = '<report_id>';
```

PACKAGE 对比摘要：
```sql
SELECT SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE, DIFF_STATUS, DIFF_HASH, DIFF_PATH
FROM DIFF_REPORT_PACKAGE_COMPARE
WHERE REPORT_ID = '<report_id>'
ORDER BY SCHEMA_NAME, OBJECT_NAME;
```

触发器状态差异：
```sql
SELECT SCHEMA_NAME, TRIGGER_NAME, SRC_ENABLED, TGT_ENABLED, SRC_VALID, TGT_VALID, DIFF_STATUS
FROM DIFF_REPORT_TRIGGER_STATUS
WHERE REPORT_ID = '<report_id>'
ORDER BY SCHEMA_NAME, TRIGGER_NAME;
```

---

## 6. run_fixup 高级执行

### 6.1 依赖感知顺序（--smart-order）
示例层级：
```
sequence -> table -> table_alter -> grants -> view -> procedure -> package -> constraint -> trigger
```

### 6.2 授权文件修剪
- 授权脚本逐条执行
- 成功的 GRANT 自动从原文件移除
- 失败项保留并输出 `fixup_scripts/errors/` 错误报告

### 6.3 迭代执行（--iterative）
- 多轮重试失败脚本
- 适合依赖链复杂对象（VIEW/PLSQL）
- 支持 `--max-rounds` / `--min-progress`

### 6.4 VIEW 链路自动修复（--view-chain-autofix）
- 读取 `main_reports/VIEWs_chain_*.txt`
- 为每个 VIEW 生成 plan + SQL
- 支持从 `fixup_scripts/done/` 兜底 DDL

### 6.5 过滤执行
- `--only-dirs` / `--exclude-dirs`
- `--only-types` / `--glob`

### 6.6 建表脚本安全门禁（--allow-table-create）
- 默认安全模式会跳过 `fixup_scripts/table/`，防止误建空表。
- 即使传入 `--only-dirs table` 或 `--only-types TABLE`，未显式开启时仍会被拦截。
- 只有在明确需要执行建表脚本时，才加 `--allow-table-create`。

### 6.7 并发与重复执行防护（默认开启）
- 同一 `fixup_dir` 下，`run_fixup` 使用 `.run_fixup.lock` 防止并发重入。
- 使用 `.fixup_state_ledger.json` 记录已执行脚本指纹，避免“执行成功但移动到 done 失败”后的重复执行。
- 迭代模式每轮会清理 auto-grant 阻断缓存，避免临时阻断在后续轮次持续生效。

### 6.8 表数据风险校验（TABLE_PRESENCE）
- `table_data_presence_check=auto` 时优先读统计信息（`NUM_ROWS`），当命中 `NUM_ROWS=0` 会做二次探针确认。
- 二次探针并发由 `table_data_presence_zero_probe_workers` 控制，默认 1，最大 32。
- 当日志显示 TABLE_PRESENCE 耗时较长，可降低候选规模或临时设置 `table_data_presence_check=off`。

---

## 7. 大规模迁移建议
- Schema 过多时分批执行（100~150/批）
- 适当增大 `cli_timeout` / `obclient_timeout`
- 使用 `dbcat_output` 复用缓存
- 扩展对象校验可设置 `extra_check_workers=4`，并按规模调整 `extra_check_chunk_size`

---

## 8. 常见问题速查

**Q1: VIEW/触发器为什么没跟随表的 remap？**  
A: 这类对象默认保持原 schema，需显式 remap。

**Q2: 为什么提示“无法自动推导”？**  
A: 依赖指向多个 schema 或依赖缺失，请补充显式 remap。

**Q3: 只检查 TABLE 时为何不生成 PACKAGE？**  
A: `check_primary_types` 限制后，未包含的类型不会加载/推导/生成。

---

## 9. 交付执行模板（建议）
1) 先跑主程序生成报告与脚本：`python3 schema_diff_reconciler.py`  
2) 先看 `report_*.txt` 的执行结论与 `report_index_*.txt` 的工件索引。  
3) 如启用 `report_to_db=true`，优先用 `HOW_TO_READ_REPORTS_IN_OB_latest.txt` 查询问题。  
4) 人工审核 `fixup_scripts/` 后执行：`python3 run_fixup.py --smart-order --recompile`。  
   如需执行 `fixup_scripts/table/`，必须显式加 `--allow-table-create`。  
5) 复杂 VIEW 依赖场景再执行：`python3 run_fixup.py --view-chain-autofix`。  
6) 完成后再次运行主程序做收敛验证（确保缺失/不支持数量符合预期）。

更新时间：2026-03-01 (V0.9.8.7)
