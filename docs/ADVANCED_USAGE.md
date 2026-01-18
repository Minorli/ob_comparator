# 高级使用指南

本手册聚焦四类高级能力：Remap 推导、授权生成、DDL 清洗、run_fixup 高级执行。

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

## 5. run_fixup 高级执行

### 5.1 依赖感知顺序（--smart-order）
示例层级：
```
sequence -> table -> table_alter -> grants -> view -> procedure -> package -> constraint -> trigger
```

### 5.2 授权文件修剪
- 授权脚本逐条执行
- 成功的 GRANT 自动从原文件移除
- 失败项保留并输出 `fixup_scripts/errors/` 错误报告

### 5.3 迭代执行（--iterative）
- 多轮重试失败脚本
- 适合依赖链复杂对象（VIEW/PLSQL）
- 支持 `--max-rounds` / `--min-progress`

### 5.4 VIEW 链路自动修复（--view-chain-autofix）
- 读取 `main_reports/VIEWs_chain_*.txt`
- 为每个 VIEW 生成 plan + SQL
- 支持从 `fixup_scripts/done/` 兜底 DDL

### 5.5 过滤执行
- `--only-dirs` / `--exclude-dirs`
- `--only-types` / `--glob`

---

## 6. 大规模迁移建议
- Schema 过多时分批执行（100~150/批）
- 适当增大 `cli_timeout` / `obclient_timeout`
- 使用 `dbcat_output` 复用缓存
- 扩展对象校验可设置 `extra_check_workers=4`，并按规模调整 `extra_check_chunk_size`

---

## 7. 常见问题速查

**Q1: VIEW/触发器为什么没跟随表的 remap？**  
A: 这类对象默认保持原 schema，需显式 remap。

**Q2: 为什么提示“无法自动推导”？**  
A: 依赖指向多个 schema 或依赖缺失，请补充显式 remap。

**Q3: 只检查 TABLE 时为何不生成 PACKAGE？**  
A: `check_primary_types` 限制后，未包含的类型不会加载/推导/生成。

更新时间：2026-01-09 (V0.9.8)
