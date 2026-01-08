# Advanced Usage Guide

本手册聚焦两部分：
1) Remap 推导与对象归属规则  
2) `run_fixup.py` 的高级执行方式

---

## 一、Remap 推导与对象归属

### 1. 规则优先级（从高到低）

1) **显式 remap 规则**（`remap_rules.txt`）  
2) **不参与推导的类型**：保持原 schema  
3) **依附对象**：跟随父表的 remap 目标 schema  
4) **依赖推导**：根据引用对象的 remap 结果推导  
5) **schema 映射回退**：多对一/一对一场景下使用主流 schema

一旦出现推导冲突或无法推导，对象会被记录到 `remap_conflicts_*.txt` 中，并从本轮对比/修复中跳过。

### 2. 哪些对象会“保持原 schema”？

默认不参与推导（除非显式 remap）：
- VIEW
- MATERIALIZED VIEW
- TRIGGER
- PACKAGE / PACKAGE BODY

这意味着：
- **触发器不会自动改 schema**。如果触发器依赖的表被 remap 到其他 schema，会自动在 fixup 阶段补充授权。
- **视图不会自动改 schema**。但视图内部对表的引用仍会按 remap 规则替换。
- **MATERIALIZED VIEW / PACKAGE / PACKAGE BODY** 默认仅打印不校验（OB 不支持或默认跳过）。

### 3. 依附对象如何跟随父表？

以下对象会跟随父表的目标 schema：
- INDEX
- CONSTRAINT
- SEQUENCE（优先根据依赖对象推导）
- SYNONYM（优先跟随指向对象或依赖对象）

原则：**只变更 schema，不改对象名**。  
示例：`SRC_A.IDX_ORDERS` 的父表 remap 到 `OB_A.ORDERS` → 结果是 `OB_A.IDX_ORDERS`。

### 4. 依赖推导适用于哪些对象？

以下类型会通过 `DBA_DEPENDENCIES` 推导目标 schema：
- PROCEDURE / FUNCTION
- TYPE / TYPE BODY
- SYNONYM（非 PUBLIC）

依赖推导规则：
- 统计对象依赖的表/视图/序列等目标 schema  
- 若目标 schema 唯一 → 采用  
- 若多个 schema 且同等出现 → 认为冲突，写入 `remap_conflicts_*.txt`

### 5. 一对一 / 多对一 / 一对多场景示例

#### 多对一（Many-to-One）
```
SRC_A.T1 -> OB_A.T1
SRC_B.T2 -> OB_A.T2
```
- TABLE：显式 remap  
- VIEW / TRIGGER / PACKAGE：保持原 schema（除非显式 remap）  
- INDEX/CONSTRAINT/SEQUENCE：跟随父表 → 归入 OB_A  
- 依赖对象（PROC/TYPE 等）：依赖推导 → 多数会归入 OB_A

#### 一对一（One-to-One）
```
SRC_A.* -> OB_A.*
```
规则同上，只是推导更稳定。

#### 一对多（One-to-Many）
```
SRC_A.T1 -> OB_A.T1
SRC_A.T2 -> OB_B.T2
```
此时：
- VIEW / TRIGGER 等**仍保持 SRC_A schema**  
- 依赖推导可能冲突（引用多个 schema）  
→ 冲突对象会出现在 `remap_conflicts_*.txt`，需显式 remap

### 6. Remap 冲突如何处理？

输出位置：
- `main_reports/remap_conflicts_*.txt`
- 报告中 “无法自动推导” 章节

处理方式：
1) 在 `remap_rules.txt` 显式补齐  
2) 重新运行对比

**注意**：冲突对象不会自动回退到源 schema，避免误判。

### 7. 检查范围与类型控制

`check_primary_types` 和 `check_extra_types` 会影响：
- 元数据加载  
- Remap 推导  
- 对比与报告  
- fixup 生成范围

示例：仅检查表
```ini
check_primary_types = TABLE
check_extra_types =
```
此时 PACKAGE/VIEW 等不会参与推导或校验。

完整可选值见 `readme_config.txt` 与 `config.ini.template`。

### 8. 禁用推导（可选）

如需完全依赖显式 remap，可关闭推导：
```ini
infer_schema_mapping = false
```

### 9. 黑名单表与 OMS 规则输出

当源端存在 `OMS_USER.TMP_BLACK_TABLE` 时：
- 缺失 TABLE 会先与黑名单匹配；黑名单表不会进入 `main_reports/tables_views_miss/`（按 schema 输出 `*_T.txt` / `*_V.txt`）。
- 被过滤的表会写入 `main_reports/blacklist_tables.txt`，按 schema 分组并注明 `BLACK_TYPE`/`DATA_TYPE`、原因与 LONG 转换校验状态。
- `LONG/LONG RAW` 列在补列 DDL 中会自动映射为 `CLOB/BLOB`。

### 10. 授权脚本优化（大量 GRANT）

当源端存在几十万条授权时，可通过以下配置降低抽取与执行成本：
```ini
# 仅抽取 source_schemas 拥有的对象权限
grant_tab_privs_scope = owner
# 合并授权语句
grant_merge_privileges = true
grant_merge_grantees = true
```
过滤掉的不兼容权限会写入 `main_reports/filtered_grants.txt`，便于人工复核。
授权脚本会过滤掉目标端不存在的用户/角色（PUBLIC 除外），并在日志中提示缺失名单，
请先创建后重新生成授权脚本。
如需覆盖权限白名单或保留 Oracle 维护角色，可使用：
```ini
grant_supported_sys_privs = CREATE SESSION,CREATE TABLE
grant_supported_object_privs = SELECT,INSERT,UPDATE,DELETE,REFERENCES,EXECUTE
grant_include_oracle_maintained_roles = false
```
如需保留旧逻辑，可设置：
```ini
grant_tab_privs_scope = owner_or_grantee
```

#### VIEW 授权与同义词下探
- 当 VIEW 被授予非 owner 时，会补齐 view owner 对依赖对象的 `WITH GRANT OPTION` 授权。
- VIEW 依赖同义词时，会下钻到最终对象生成授权，避免因同义词导致的权限缺失。

#### VIEW DDL 清洗
- 修复 VIEW DDL 中“行内注释吞行”问题（DBMS_METADATA/转换后 DDL 均适用）。
- 仅在命中视图列元数据时，才合并被拆分的列名（避免误修别名）。
- OceanBase 版本 < 4.2.5.7 时移除 `WITH CHECK OPTION`，高版本保留。

---

## 二、run_fixup.py 高级用法

`run_fixup.py` 负责执行 `fixup_scripts/` 下的 SQL，并支持：
- 依赖感知排序（`--smart-order`）
- 自动重编译 INVALID 对象（`--recompile`）
- 目录/类型/文件名过滤
- 授权脚本逐行执行，失败行保留到原文件
- 错误报告输出到 `fixup_scripts/errors/`

### 1. 推荐执行方式
```bash
python3 run_fixup.py --smart-order --recompile
```

### 2. 过滤执行

仅执行部分目录：
```bash
python3 run_fixup.py --only-dirs table,table_alter,grants_miss
```

按对象类型过滤（自动映射到目录）：
```bash
python3 run_fixup.py --only-types TABLE,VIEW,PROCEDURE
```

按文件名过滤：
```bash
python3 run_fixup.py --glob "*SCHEMA_A*.sql"
```

排除目录：
```bash
python3 run_fixup.py --exclude-dirs trigger,job
```

### 3. 执行顺序

默认顺序（标准优先级）：
```
sequence -> table -> table_alter -> constraint -> index -> view -> ...
```

`--smart-order` 启用依赖感知排序：
```
Layer 0: sequence
Layer 1: table
Layer 2: table_alter
Layer 3: grants_miss (默认；可手动指定 grants_all)
Layer 4: view, synonym
Layer 5: materialized_view
Layer 6: procedure, function
Layer 7: package, type
Layer 8: package_body, type_body
Layer 9: constraint, index
Layer 10: trigger
Layer 11: job, schedule
```

### 4. 自动重编译

`--recompile` 会在执行完成后：
- 查询 `DBA_OBJECTS` 中 `INVALID` 对象  
- 尝试 `ALTER ... COMPILE`  
- 最多重试 `--max-retries` 次（默认 5）

示例：
```bash
python3 run_fixup.py --smart-order --recompile --max-retries 10
```

### 4.1 超时控制

`run_fixup.py` 使用 `fixup_cli_timeout` 控制 SQL 执行超时（单位秒）：
- `fixup_cli_timeout = 3600`：单条语句最长 1 小时
- `fixup_cli_timeout = 0`：不设置超时（可能阻塞）

### 5. 幂等执行

`run_fixup.py` 具有幂等性：
- 成功的脚本会移动到 `fixup_scripts/done/`
- 再次执行时只处理失败项

### 6. 迭代执行模式（推荐用于VIEW）

**新增功能（V0.9.7+）**: 支持多轮迭代执行，自动重试失败的脚本。

#### 基本用法
```bash
# 启用迭代模式
python3 run_fixup.py --iterative --smart-order --recompile

# 自定义迭代参数
python3 run_fixup.py --iterative --max-rounds 10 --min-progress 1
```

#### 参数说明
- `--iterative`: 启用多轮执行模式
- `--max-rounds N`: 最大迭代轮次（默认10）
- `--min-progress N`: 每轮最小成功数，低于此值停止（默认1）

#### 工作原理
1. **第1轮**: 执行所有脚本
2. **后续轮次**: 自动重试失败的脚本
3. **收敛检测**: 当无进展或达到最大轮次时停止
4. **智能分类**: 区分可重试错误（缺少依赖）和永久性错误（语法/权限问题）

#### 典型场景：VIEW依赖链
```
VIEW_C 依赖 VIEW_B 依赖 VIEW_A

第1轮: VIEW_A 成功，VIEW_B和VIEW_C失败（依赖不存在）
第2轮: VIEW_B 成功，VIEW_C 失败
第3轮: VIEW_C 成功
```

**效果**: VIEW成功率从0.5%提升至93%+

#### 错误分类与建议
迭代模式会自动分析失败原因并提供可操作建议：

```
=== 失败原因分析 ===
❌ 依赖对象不存在: 30 个 (可在后续轮次重试)
   建议: 这些脚本会在依赖对象创建后自动重试成功

❌ 权限不足: 5 个
   建议: 检查并执行 fixup_scripts/grants_miss/ 下的授权脚本

✓ 对象已存在: 10 个 (可忽略)
❌ 数据冲突/唯一约束违反: 3 个
   建议: 清理重复数据后重试相关DDL
```

#### 与其他参数组合
```bash
# 仅处理VIEW，迭代执行
python3 run_fixup.py --iterative --only-types VIEW --max-rounds 5

# 迭代+过滤目录
python3 run_fixup.py --iterative --only-dirs view,procedure --max-rounds 8

# 迭代+文件名过滤
python3 run_fixup.py --iterative --glob "*SCHEMA_A*.sql" --max-rounds 10
```

#### 何时使用迭代模式？
- ✅ **推荐**: 有复杂依赖关系的VIEW
- ✅ **推荐**: 跨schema引用的对象
- ✅ **推荐**: 大量互相依赖的PROCEDURE/FUNCTION
- ❌ **不推荐**: 仅处理TABLE（通常无依赖问题）
- ❌ **不推荐**: 已知有语法错误的脚本（迭代无法修复）

### 7. VIEW 链路自动修复（--view-chain-autofix）

该模式会读取最新的 `main_reports/VIEWs_chain_*.txt`，为每个缺失 VIEW 生成修复计划与 SQL，
并按依赖顺序执行。授权语句只从 `grants_miss/`、`grants_all/` 中精准挑选匹配项，不会全量执行。

输出目录：
- `fixup_scripts/view_chain_plans/`：每个 VIEW 的修复计划
- `fixup_scripts/view_chain_sql/`：每个 VIEW 的修复 SQL

默认行为：
- 已存在 VIEW 会自动跳过执行（计划/SQL 仍会输出并标记 SKIPPED）
- 依赖 DDL 缺失时会从 `fixup_scripts/done/` 兜底查找
- 授权缺失且 grants 中找不到匹配项时自动生成对象授权语句
- 输出执行结果：SUCCESS / PARTIAL / FAILED / BLOCKED / SKIPPED

示例：
```bash
python3 run_fixup.py --view-chain-autofix
```

注意事项：
- 未找到链路文件时会直接退出（需先生成 fixup + 依赖链报告）
- 依赖链存在环或缺失 DDL 会标记 BLOCKED 并跳过自动执行


---

## 三、常见问题速查

**Q1: VIEW/触发器为什么没跟随表的 remap？**  
A: 这类对象默认保持原 schema。若需要迁移，请显式 remap。

**Q2: 为什么提示“无法自动推导”？**  
A: 依赖指向多个 schema 或依赖缺失。请补充显式 remap。

**Q3: 我设置了 `check_primary_types=TABLE`，为什么没有推导 PACKAGE？**  
A: 检查范围被限制为 TABLE，其他类型不会加载也不会推导。

---

更新时间：2026-01-08 (V0.9.7: VIEW 链路修复与授权修剪增强)
