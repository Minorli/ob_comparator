# schema_diff_reconciler.py 全面代码审查报告（V2）

**审查范围**: 程序逻辑、业务逻辑、代码逻辑、代码质量、文档  
**排除范围**: 密码问题、程序过大问题  
**初次审查**: 2025-01  
**本次更新**: 2025-02（基于 V0.9.8.3，30828 行）  

---

## 审查摘要

共发现 **52 个问题**，按严重程度分布如下：

| 级别 | 数量 | 说明 |
|------|------|------|
| P0 - 严重缺陷 | 7 | 可能导致数据错误、DDL 损坏或功能失效 |
| P1 - 重要问题 | 16 | 影响正确性或健壮性 |
| P2 - 一般问题 | 18 | 影响可维护性或存在潜在风险 |
| P3 - 改进建议 | 11 | 代码质量和风格优化 |

> 标记 **[NEW]** 表示本次更新新增的问题

---

## P0 - 严重缺陷（7个）

### P0-01: `clean_extra_dots` 与 `clean_for_loop_*` 管线冲突，PL/SQL `..` 范围运算符被破坏 [NEW]

**位置**: 第 18042-18058 行、第 18874-18891 行  
**问题**: `clean_extra_dots` 的正则将标识符之间的连续点号（如 `A..B`）替换为单点（`A.B`），但 PL/SQL 的 `..` 是合法范围运算符。

在 `DDL_CLEANUP_RULES['PLSQL_OBJECTS']` 管线中，`clean_for_loop_collection_attr_range`（第 18018 行）先将 `.FIRST.var` 修复为 `.FIRST..var`，而后续的 `clean_extra_dots` 又将 `FIRST..collection` 破坏回 `FIRST.collection`。  
**影响**: 所有包含集合属性范围的 PL/SQL DDL 会被损坏导致**编译失败**。  
**建议**: `clean_extra_dots` 正则改为仅匹配三个及以上连续点号 `\.{3,}`，或调整管线顺序。

### P0-02: 序列迁移遗漏 LAST_NUMBER / RESTART WITH

**位置**: 序列元数据查询及 fixup 生成  
**问题**: 序列比对完全忽略了 `LAST_NUMBER`（Oracle 当前值），迁移后序列可能从初始值重新开始。  
**影响**: **主键重复**风险。  
**建议**: 查询 `DBA_SEQUENCES.LAST_NUMBER`，生成 `ALTER SEQUENCE ... RESTART WITH` 语句。

### P0-03: `is_index_expression_token` 正则转义错误

**位置**: 第 1670 行  
```python
return bool(re.search(r"[()\s'\"+\-*/]|\\bCASE\\b", token, flags=re.IGNORECASE))
```
**问题**: `\\bCASE\\b` 在 raw string 中是字面量 `\b`，非单词边界。应为 `\bCASE\b`。  
**影响**: CASE 表达式函数索引无法被正确识别，索引比对出错。

### P0-04: `compare_version` 解析失败静默返回相等

**位置**: 第 16180-16198 行  
**问题**: OB 版本号含非数字后缀（如 `4.2.1-bp1`）时 `int(x)` 异常，返回 0（相等），导致所有版本判断失效。  
**建议**: 先用 `re.findall(r'\d+', version)` 提取数字段。

### P0-05: 报告数据库保留期清理仅删除 summary 表

**位置**: 第 27963-27969 行  
**问题**: `retention_days` 清理仅删 summary 表，detail/counts/usability 等十余张子表的关联数据不会被清理。  
**影响**: 报告数据库空间持续膨胀。

### P0-06: `clean_extra_semicolons` 不区分字符串常量 [NEW]

**位置**: 第 18027-18039 行  
```python
cleaned = re.sub(r';+', ';', ddl)
```
**问题**: 在整个 DDL 文本上全局替换，PL/SQL 字符串常量中的 `;;`（如 `v_sql := 'cmd1;;cmd2'`）会被错误修改。  
**影响**: 字符串常量内容被静默损坏。

### P0-07: `add_custom_cleanup_rule` 创建的规则永远不会生效 [NEW]

**位置**: 第 18976-19003 行  
**问题**: `apply_ddl_cleanup_rules` 遍历 `DDL_CLEANUP_RULES` 字典，找到第一个匹配 `break`。内置规则始终排在自定义规则前（字典插入序），因此自定义规则永远不会被执行。  
**影响**: `add_custom_cleanup_rule` 接口是**死代码**。  
**建议**: 将自定义规则追加到内置规则集的 `rules` 列表，或遍历所有匹配集。

---

## P1 - 重要问题（16个）

### P1-01: SQL 拼接存在注入风险

**位置**: `obclient_query_by_owner_chunks`（第 7829 行）等多处  
**问题**: OB 端查询全部使用字符串拼接（不安全），Oracle 端使用 bind placeholder（安全）。  
**建议**: 统一使用 `sql_quote_literal` 或 `replace("'", "''")`

### P1-02: `extract_trigger_table_references` 正则严重过匹配 [NEW]

**位置**: 第 18085-18124 行  
**问题**: `FROM`/`UPDATE`/`JOIN` 等正则在整个 DDL 文本执行，不排除注释、字符串常量、子查询别名。`FROM DUAL` 也会被误加入表引用集。`UPDATE` 模式没有排除 `UPDATE OF column`（触发器事件子句）。  
**影响**: `remap_trigger_table_references` 基于不准确的引用集做替换，可能在触发器 DDL 中产生错误替换。

### P1-03: `remap_trigger_table_references` 使用 `\b` 对含 `$#` 标识符失效 [NEW]

**位置**: 第 18160-18164 行  
**问题**: `\b` 将 `$` 和 `#` 视为非单词字符，对 Oracle 中常见的 `SYS$USERS`、`HR#EMP` 等标识符会产生错误的边界匹配。  
**影响**: 包含 `$`/`#` 的对象引用可能被错误替换。

### P1-04: `clean_long_types_in_table_ddl` 不区分代码区域 [NEW]

**位置**: 第 18862-18870 行  
**问题**: `\bLONG\b` → `CLOB` 全局替换不排除注释和字符串常量。DDL 注释中的 `LONG` 文本会被误修改。

### P1-05: `ensure_trigger_mappings_for_extra_checks` 忽略 schema 映射 [NEW]

**位置**: 第 12935-12965 行  
**问题**: 未映射触发器默认 1:1 映射（源=目标 schema），但在 schema 重映射场景中，触发器的目标 schema 应跟随其所属表。  
**影响**: 触发器 fixup 脚本可能生成到错误的 schema。

### P1-06: `DDL_CLEANUP_RULES` 全局可变字典的并发风险 [NEW]

**位置**: 第 18874 行  
**问题**: `add_custom_cleanup_rule` 可在运行时修改此字典。多线程 fixup 生成中如果并发修改，会导致 `RuntimeError: dictionary changed size during iteration`。

### P1-07: 错误检测启发式规则存在误判风险

**位置**: 第 7764-7768 行  
**问题**: obclient 输出中错误不一定出现在行首；正常输出以 `ORA-` 开头也可能被误判。

### P1-08: NOKEEP/NOSCALE/GLOBAL 移除过于激进

**位置**: 第 18830-18832 行  
**问题**: 全局正则替换不区分上下文，`GLOBAL` 可能出现在注释/字符串中被误删。

### P1-09: `clean_plsql_ending` 不处理带引号对象名

**位置**: 第 17935-17976 行  
**问题**: `\w+` 无法匹配 `"My_Proc"`，无名 `END;` 后多余分号也不处理。

### P1-10: `load_config` 函数过于庞大（约 490 行）

**位置**: 第 2959-3449 行  
**建议**: 定义配置 schema（dataclass），自动类型转换和默认值填充。

### P1-11: `settings` Dict 类型混杂

**问题**: 混合存储 str/int/float/bool/Set/List 等，部分配置项重复存储，后续使用需反复类型转换。

### P1-12: 配置向导 `infer_schema_mapping` 提示文本与默认值矛盾

**位置**: 第 3966 行  
**问题**: 提示说"默认 false"，但 fallback 和 setdefault 都是 `"true"`。

### P1-13: 报告 INSERT 未使用参数化查询

**问题**: 所有 INSERT 通过 f-string 拼接，未处理 NUL 字符和超长 CLOB 溢出。

### P1-14: 线程池异常处理不完整

**位置**: 第 14375-14392 行等  
**问题**: `executor.map()` 异常在迭代时抛出，无 try-except 包裹，单个 worker 异常导致整批丢失。

### P1-15: `abort_run()` 返回类型应为 `NoReturn`

**位置**: 第 128 行  
**问题**: 声明返回 `None` 但实际总是抛异常。调用点后缺少 return，mypy 会报错。

### P1-16: `obclient_run_sql` 超时日志输出全局变量而非实际超时值

**位置**: 第 7801 行  
**问题**: 日志输出 `OBC_TIMEOUT` 而非该次调用实际使用的 `timeout_val`。当调用者覆盖 timeout 时日志不准确。

---

## P2 - 一般问题（18个）

### P2-01: 过度使用宽泛的 `except Exception`（20+ 处）
多处静默吞没异常。建议至少 `log.debug` 记录。

### P2-02: f-string 与 `%s` 日志格式混用
统一使用 `log.info("msg %s", val)` 延迟格式化。

### P2-03: 规范化函数模板代码高度重复（约 10 个 `normalize_*` 函数）
建议抽取通用 `normalize_enum_config()` 工具函数。

### P2-04: 配置向导验证函数同样高度重复
可复用 `normalize_*` 函数作为验证器。

### P2-05: `NamedTuple` 数量过多（40+），部分可合并
如 `IndexMismatch`/`ConstraintMismatch`/`TriggerMismatch` 结构相似。

### P2-06: `settings` 字典键名命名不一致
`check_comments` → `enable_comment_check`，映射关系不清晰。

### P2-07: `normalize_comment_text` 将 "NULL"/"NONE" 视为空
字面量 `"NULL"` 注释会被清空，属于信息丢失。

### P2-08: 日志中 LD_LIBRARY_PATH 提示仅适用 Linux
应根据 `sys.platform` 输出平台对应提示。

### P2-09: `clean_for_loop_single_dot_range` 正则可能误伤科学记数法
`IN 1.E10` 会被错误替换为 `IN 1..E10`。

### P2-10: `run_config_wizard` 明文回显密码
应使用 `getpass.getpass()` 替代 `input()`。

### P2-11: `shutil.rmtree` 使用 `ignore_errors=True`
静默忽略删除错误，建议至少记录 warning。

### P2-12: `build_column_order_sequence` 返回语义不一致
`tuple()` vs `None` 作为空/错误 sentinel，增加调用方复杂度。

### P2-13: DFS 递归可能导致栈溢出和内存开销
每次递归创建新 path 列表和 seen 集合。建议改为迭代式。

### P2-14: `GRANT_PRIVILEGE_BY_TYPE` 中 TRIGGER 映射为 EXECUTE
Oracle 触发器不通过 `EXECUTE` 授权，可能生成无效 GRANT 语句。

### P2-15: `clean_extra_dots` 每次调用都编译正则 [NEW]
第 18053 行在函数体内 `re.compile()`，应提升为模块级常量。

### P2-16: `apply_ddl_cleanup_rules` 规则执行失败后继续使用半成品 DDL [NEW]
第 18970 行 catch Exception 后继续处理，可能产生不一致的 DDL。建议规则失败时回退到原始 DDL。

### P2-17: `split_ddl_statements` 不支持 Oracle `q'[]'` 引用语法 [NEW]
第 19077 行的语句分割器仅处理标准单/双引号，`q'[text with ; inside]'` 中的分号会导致错误切分。

### P2-18: dbcat 并行导出传递 Oracle 密码到命令行 [NEW]
第 15963 行 `'-p', ora_cfg['password']` 同样通过命令行参数暴露密码。与 P0-05 类似但在不同代码路径。

---

## P3 - 改进建议（11个）

### P3-01: 缺少单元测试
核心纯函数（`normalize_sql_expression`、`classify_unsupported_constraint`、`is_number_equivalent`）适合测试覆盖。注：`test_schema_diff_reconciler.py` 已存在 6150 行测试，但 DDL 清理管线（新增功能）缺乏覆盖。

### P3-02: 类型别名使用 `Dict` 而非 `TypedDict`
`OraConfig`/`ObConfig` 使用 `Dict[str, str]`，建议改用 `TypedDict`。

### P3-03: 常量定义分散在文件各处
`REPORT_DB_TABLES`/`GRANT_PRIVILEGE_BY_TYPE`/`DDL_OBJECT_TYPE_OVERRIDE` 等散布 700-2950 行。

### P3-04: `write_fixup_file` 分号追加逻辑不处理 PL/SQL 块
PL/SQL 块结尾应为 `/` 而非 `;`。

### P3-05: `sql_quote_literal` 仅处理单引号
未过滤 `\x00` 字符，某些数据库会截断或报错。

### P3-06: `main()` 函数过于庞大（约 890 行）
建议将各阶段拆分为独立函数。

### P3-07: 部分函数缺少 docstring
如 `normalize_column_sequence`、`build_column_order_sequence` 等。

### P3-08: 硬编码 SQL 散布在各函数中
DBA_OBJECTS/DBA_TAB_COLUMNS 等查询直接内嵌。

### P3-09: `chunk_list` 类型标注限制为 `List[str]`
实现逻辑对任意类型均有效，建议使用泛型。

### P3-10: 日志消息混用中英文
建议统一日志语言风格。

### P3-11: DDL 清理规则缺乏集成测试 [NEW]
新增的 `DDL_CLEANUP_RULES` 管线包含 10+ 个清理函数串联执行，各函数间存在交互（如 P0-01 的管线冲突），但缺乏端到端测试验证管线整体行为。建议为每种对象类型编写包含边界 case 的管线集成测试。

---

## 问题分布热力图

| 代码区域 | 行范围 | 问题数 |
|----------|--------|--------|
| 常量/类型定义 | 1-900 | 3 |
| 规范化/校验/配置 | 900-3450 | 8 |
| 配置向导 | 3450-4350 | 2 |
| 对象映射/remap | 4350-7730 | 1 |
| obclient 执行 | 7730-7830 | 4 |
| OB/Oracle 元数据 | 7830-10400 | 2 |
| 依赖/授权 | 10400-12430 | 2 |
| 扩展对象校验 | 12430-14490 | 3 |
| DDL 抽取(dbcat) | 14490-16200 | 2 |
| DDL 清理管线 | 16200-19100 | **12** |
| Fixup 脚本生成 | 19100-23200 | 3 |
| 报告/DB 写入 | 23200-27970 | 3 |
| 主函数 | 29780-30828 | 1 |

> DDL 清理管线（16200-19100 行）是本次审查发现问题**最集中**的区域，包含 P0-01/P0-06/P0-07 三个严重缺陷。

---

## 优先修复建议

### 立即修复（P0）
1. **P0-01**: 修复 `clean_extra_dots` 正则，排除 `..` 范围运算符（或调整管线顺序）
2. **P0-03**: 修正 `is_index_expression_token` 的正则双转义 bug
3. **P0-04**: 修正 `compare_version` 处理带后缀版本号
4. **P0-07**: 修复 `add_custom_cleanup_rule` 使自定义规则可生效
5. **P0-02**: 补齐序列 LAST_NUMBER 采集
6. **P0-05**: 报告清理需级联删除子表
7. **P0-06**: `clean_extra_semicolons` 需排除字符串常量

### 近期修复（P1）
- **P1-02/P1-03**: 触发器表引用提取和 remap 逻辑需要重写（正则方案不够健壮）
- **P1-01**: OB 端 SQL 拼接需统一转义
- **P1-06**: DDL_CLEANUP_RULES 需线程安全保护或改为不可变

### 中期改进（P2/P3）
- DDL 清理管线需补充集成测试（P3-11），这是防止 P0-01 类管线冲突再次发生的关键
- 统一日志格式（P2-02）和规范化函数去重（P2-03/P2-04）可批量处理

---
---

# run_fixup.py 全面代码审查报告

**审查范围**: 程序逻辑、业务逻辑、代码逻辑、代码质量、与主程序一致性  
**排除范围**: 密码问题、程序过大问题  
**审查日期**: 2025-02  
**文件版本**: 4141 行  

---

## 审查摘要

共发现 **30 个问题**，按严重程度分布如下：

| 级别 | 数量 | 说明 |
|------|------|------|
| P0 - 严重缺陷 | 4 | 导致功能失效或结果错误 |
| P1 - 重要问题 | 10 | 影响正确性或健壮性 |
| P2 - 一般问题 | 10 | 影响可维护性或存在潜在风险 |
| P3 - 改进建议 | 6 | 代码质量和风格优化 |

---

## RF-P0 - 严重缺陷（4个）

### RF-P0-01: `exists_cache` 在 view chain autofix 中导致执行后状态检查失败

**位置**: 第 3574 行（缓存初始化）、第 3603-3611 行（前置检查）、第 3697-3704 行（后置检查）  
```python
# 前置检查: 对象不存在 → cache[(key)] = False
root_exists = check_object_exists(..., exists_cache, ..., use_planned=False)
# ... 执行 DDL 创建对象 ...
# 后置检查: 缓存命中, 返回 False (陈旧数据!)
post_exists = check_object_exists(..., exists_cache, ..., use_planned=False)
```
**问题**: `check_object_exists`（第 1569 行）在查询数据库后将结果缓存到 `exists_cache`。前置检查时对象不存在，缓存存储 `False`。DDL 执行后，后置检查命中相同缓存 key，**直接返回旧的 `False`**，不会重新查询数据库。

这意味着即使视图已成功创建，`classify_view_chain_status` 也会收到 `view_exists=False`，将结果判定为 **FAILED** 而非 SUCCESS。  
**影响**: view chain autofix 模式下所有成功创建的视图都会被错误报告为失败。  
**建议**: 在后置检查前删除对应缓存条目：`exists_cache.pop(key, None)`

### RF-P0-02: 迭代模式 `cumulative_failed` 计数永不减少，exit code 错误

**位置**: 第 4036-4039 行、第 4135 行  
```python
for item in round_results:
    if item.status in ("FAILED", "ERROR"):
        cumulative_failed_paths.add(item.path)  # 只增不减
cumulative_failed = len(cumulative_failed_paths)
...
exit_code = 0 if cumulative_failed == 0 else 1
```
**问题**: `cumulative_failed_paths` 集合在每轮中只添加失败路径，从不移除。若某脚本在第 1 轮失败但在第 2 轮成功（被移到 `done/` 目录），其路径仍留在集合中。最终 `cumulative_failed` 表示的是"曾经失败过的脚本数"而非"最终仍失败的脚本数"。  
**影响**: 即使所有脚本最终都成功执行，程序仍可能返回 `exit_code=1`。CI/CD 流水线会错误判定修补失败。  
**建议**: 每轮开始时清除上一轮成功的路径，或在最终统计时使用最后一轮的失败数

### RF-P0-03: `query_invalid_objects` 查询全库 INVALID 对象，可能重编译非目标对象

**位置**: 第 2890-2894 行  
```python
sql = """
SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
FROM DBA_OBJECTS
WHERE STATUS = 'INVALID'
ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME;
"""
```
**问题**: 查询没有按 OWNER 过滤，会返回所有 schema（包括 SYS、SYSTEM 等系统 schema）的 INVALID 对象。`build_compile_statement` 也不检查 owner 是否属于目标 schema。  
**影响**: 
1. 尝试重编译系统对象可能因权限不足而批量报错
2. 大型数据库可能有成千上万的系统 INVALID 对象，导致重编译阶段耗时极长
3. 可能意外修改非目标 schema 的对象状态  
**建议**: 添加 `WHERE OWNER IN (...)` 过滤，仅重编译 fixup 涉及的 schema

### RF-P0-04: `DEPENDENCY_LAYERS` 与 原始 `priority` 排序对 CONSTRAINT/INDEX 顺序严重不一致

**位置**: 第 432-447 行 vs 第 947-953 行  
```python
# DEPENDENCY_LAYERS (smart_order=True):
# Layer 4: view, synonym
# Layer 11: constraint, index  ← 在 trigger(12) 之前
# Layer 12: trigger

# priority (smart_order=False):
# Index 4: constraint  ← 在 view(6) 之前!
# Index 5: index       ← 在 view(6) 之前!
# Index 6: view
```
**问题**: 两种排序模式下 CONSTRAINT/INDEX 与 VIEW 的先后顺序完全相反：
- `--smart-order` 模式: VIEW(4) → CONSTRAINT(11)
- 默认模式: CONSTRAINT(4) → VIEW(6)

主程序 `generate_fixup_scripts` 的生成顺序是: TABLE → VIEW → INDEX → CONSTRAINT → TRIGGER。默认模式将 constraint/index 放在 view 之前，与主程序逻辑矛盾。  
**影响**: 默认模式下，引用视图的约束可能因视图尚未创建而执行失败。用户切换 `--smart-order` 后行为完全不同，造成困惑。  
**建议**: 统一两种模式的约束/索引位置，与主程序的生成顺序保持一致

---

## RF-P1 - 重要问题（10个）

### RF-P1-01: `classify_sql_error` 仅分类 ORA- 错误码，遗漏 OB- 错误码

**位置**: 第 187-253 行  
**问题**: 所有错误分类条件只检查 `ORA-xxxxx` 模式，而 OceanBase 自有错误码（如 `OB-00600` 内部错误、`ERROR 4012` 超时等）不会被分类，全部归入 `UNKNOWN`。  
**影响**: 迭代模式中基于错误类型的重试逻辑对 OB 原生错误码不生效。例如 OB 的 `ERROR 4012`（超时）不会被分类为 `TIMEOUT`。

### RF-P1-02: `apply_grant_entries` 仅检查 returncode，不检查错误输出

**位置**: 第 2641-2654 行  
```python
result = run_sql(obclient_cmd, entry.statement, timeout)
if result.returncode == 0:
    applied_grants.add(key)
    applied += 1
    continue
```
**问题**: obclient 在某些 SQL 错误下可能返回 returncode=0（例如存储过程中的编译警告），但 stderr/stdout 中包含错误信息。主程序的 `obclient_run_sql` 通过 `extract_error_from_output` 检查输出文本，但 `apply_grant_entries` 仅检查 returncode。  
**影响**: 实际失败的 GRANT 可能被误判为成功，导致后续对象因权限不足而执行失败。

### RF-P1-03: `build_compile_statement` 不引用标识符

**位置**: 第 2914-2926 行  
```python
return f"ALTER {obj_type_u} {owner_u}.{name_u} COMPILE;"
```
**问题**: `owner_u` 和 `name_u` 直接拼接进 SQL，未做双引号包裹。如果对象名包含特殊字符或是保留字（如 `"ORDER"`、`"GROUP"`），编译语句会产生语法错误。  
**建议**: 使用 `f'ALTER {obj_type_u} "{owner_u}"."{name_u}" COMPILE;'`

### RF-P1-04: `extract_object_from_error` 过度泛化匹配

**位置**: 第 1061-1077 行  
**问题**: `RE_PLAIN_DOT`（`r"([A-Za-z0-9_#$]+)\.([A-Za-z0-9_#$]+)"`）匹配错误信息中**任何** `word.word` 模式。错误消息如 `"at line 5.3"` 或 `"version 4.2"` 会被错误解析为 schema.object。  
**影响**: 迭代模式中 MISSING_OBJECT 错误的依赖解析可能指向错误的对象。

### RF-P1-05: `run_iterative_fixup` 和 `run_view_chain_autofix` 缺少连接性检查

**位置**: 第 3813 行、第 3571 行  
**问题**: `run_single_fixup` 在执行前调用 `check_obclient_connectivity`（第 3256 行）验证连接，但 `run_iterative_fixup` 和 `run_view_chain_autofix` 均未做此检查。  
**影响**: 连接失败时，大量脚本会逐个执行并逐个失败，产生海量错误日志，且退出码不明确。

### RF-P1-06: `topo_sort_nodes` DFS 递归内存 O(N²)

**位置**: 第 1270-1287 行  
```python
def dfs(node, stack):
    ...
    for ref in sorted(edges.get(node, set())):
        dfs(ref, stack + [node])  # 每次递归复制整个 stack
```
**问题**: `stack + [node]` 每次递归创建新列表，对 N 个节点的链式依赖，总内存分配为 O(N²)。  
**建议**: 改为 `stack.append(node)` / `stack.pop()` 就地修改

### RF-P1-07: `ensure_view_owner_grant_option` 递归无深度限制

**位置**: 第 1859-1951 行  
**问题**: 函数通过 `visited_views` 防止重复访问，但对深度无限制。如果依赖图非常深（虽然 `visited_views` 防止了环），极端情况下仍可能触发 Python 默认递归限制（1000）。  
**影响**: 深层视图依赖链可能导致 `RecursionError`。

### RF-P1-08: `GRANT_PRIVILEGE_BY_TYPE` 中 TRIGGER 映射为 EXECUTE

**位置**: 第 464 行  
```python
"TRIGGER": "EXECUTE",
```
**问题**: 与主程序相同的问题——Oracle TRIGGER 不通过 `EXECUTE` 授权。auto-grant 会生成无效的 `GRANT EXECUTE ON <trigger>` 语句。

### RF-P1-09: `is_comment_only_statement` 不处理字符串内的 `--`

**位置**: 第 1086-1100 行  
```python
if "--" in line_strip:
    line_strip = line_strip.split("--", 1)[0].strip()
```
**问题**: 如果代码行包含 `v_sql := 'SELECT -- test'`，`--` 后的内容会被误认为是注释而被截断，导致该行被判定为空行。如果整个语句都是此类行，会被错误判定为"仅注释"而跳过执行。  
**影响**: 包含字符串常量中 `--` 的有效 SQL 语句可能被跳过。

### RF-P1-10: `execute_grant_file_with_prune` 重写文件后丢失原始格式和注释

**位置**: 第 2818 行  
```python
rewritten = "\n\n".join(stmt.strip() for stmt in kept_statements if stmt.strip()).rstrip()
```
**问题**: 重写时将所有语句用 `\n\n` 连接，原始文件中的注释头（如 `-- Generated by ob_comparator`）、`ALTER SESSION SET CURRENT_SCHEMA` 等非语句行已在 `split_sql_statements` 中被吞掉，不会出现在 `kept_statements` 中。重写后的文件缺少 schema 上下文和注释。  
**影响**: 重写后的 grant 文件在下次执行时可能因缺少 `ALTER SESSION SET CURRENT_SCHEMA` 而在错误 schema 下执行。

---

## RF-P2 - 一般问题（10个）

### RF-P2-01: `LimitedCache` 混合 FIFO/LRU 语义

**位置**: 第 708-735 行  
**问题**: `__setitem__` 使用 `popitem(last=False)` 淘汰最旧插入的（FIFO），但 `get` 使用 `move_to_end(key)` 更新访问顺序（LRU）。这导致缓存行为既不是纯 FIFO 也不是纯 LRU，难以推理缓存命中率。

### RF-P2-02: 三种执行模式之间大量代码重复

**问题**: `run_single_fixup`（第 3220-3496 行）、`run_iterative_fixup`（第 3768-4136 行）和 `run_view_chain_autofix`（第 3499-3765 行）包含大量重复的初始化、执行循环和汇总逻辑。  
**建议**: 提取公共的初始化、执行和汇总逻辑为独立函数

### RF-P2-03: 无事务管理

**问题**: 每个 SQL 语句通过独立的 `run_sql` 调用执行（每次启动新的 obclient 进程），没有显式的 `COMMIT`/`ROLLBACK` 控制。DDL 在 Oracle 模式下自动提交，但 DML 类 fixup 语句可能需要事务边界控制。  
**影响**: 部分成功的 DML fixup 无法回滚。

### RF-P2-04: `query_count` / `query_single_column` 假设 tab 分隔输出

**位置**: 第 2310 行、第 2328 行  
**问题**: 使用 `line.split("\t", 1)` 解析 obclient 输出。如果 obclient 使用非 tab 分隔格式（取决于 `--silent` 模式和版本），解析会静默失败返回空结果。

### RF-P2-05: `sanitize_view_chain_view_ddl` 未处理所有 Oracle 关键字

**位置**: 第 622-642 行  
**问题**: 仅清理 `FORCE`/`NO FORCE`/`EDITIONABLE`/`NONEDITIONABLE`，未处理 `EDITIONING`（Oracle 12c+）。

### RF-P2-06: `find_latest_report_file` 搜索范围逐级扩大，可能找到非预期文件

**位置**: 第 1311-1363 行  
**问题**: 如果在 `report_dir` 中未找到文件，会向上搜索 `parent` 目录，再使用 `rglob` 递归搜索。在复杂目录结构中，可能匹配到旧版本或其他项目的报告文件。

### RF-P2-07: `record_error_entry` 截断消息但不标注

**位置**: 第 2700-2701 行  
```python
if len(message) > 200:
    message = message[:200] + "..."
```
**问题**: 错误消息被截断为 200 字符，但错误报告文件中没有标注该条目是否被截断。用户可能基于不完整信息做出错误判断。

### RF-P2-08: 日志输出中使用 emoji 字符

**位置**: 第 295、304、313、324 行等  
```python
log.info("❌ 依赖对象不存在: %d 个", len(items))
```
**问题**: 在非 Unicode 终端或日志文件中，emoji 字符可能显示为乱码或问号。  
**建议**: 使用纯 ASCII 字符（如 `[FAIL]`、`[OK]`）或根据终端能力动态选择

### RF-P2-09: `execute_sql_statements` 对每条语句独立启动 obclient 进程

**位置**: 第 2232-2267 行  
**问题**: 每个 SQL 语句通过 `run_sql` 启动一个新的 obclient 子进程，包含 TCP 连接建立和认证。对于包含数百条语句的文件，这意味着数百次连接/断开开销。  
**影响**: 执行效率低，尤其在网络延迟较高的环境中。  
**建议**: 考虑将多条语句合并到单次 obclient 会话中执行

### RF-P2-10: `view_chain_autofix` 执行 `sql_text` 时包含注释头

**位置**: 第 3672、3696 行  
```python
sql_text = "\n".join(sql_header + [""] + sql_lines).rstrip() + "\n"
...
summary = execute_sql_statements(obclient_cmd, sql_text, ob_timeout)
```
**问题**: `sql_text` 包含 `-- VIEW chain autofix SQL` 等注释头，这些注释虽然不会导致执行错误，但会被 `split_sql_statements` 作为语句的一部分保留，增加每次 obclient 调用的输入大小。更重要的是，`summary.statements` 计数可能包含这些仅含注释的"语句"。

---

## RF-P3 - 改进建议（6个）

### RF-P3-01: `plan_object_grant_for_dependency` 参数过多（18个）

**位置**: 第 1763-1784 行  
**建议**: 将缓存和计划状态封装到 context 对象中传递。类似的问题也出现在 `build_view_chain_plan`（23 个参数）、`ensure_view_owner_grant_option`（19 个参数）等函数中。

### RF-P3-02: 缺少单元测试

**问题**: `split_sql_statements`、`classify_sql_error`、`parse_grant_statement`、`topo_sort_nodes` 等核心纯函数没有测试覆盖。

### RF-P3-03: 部分函数缺少 docstring

**问题**: `execute_sql_statements`、`split_sql_statements`、`collect_sql_files_by_layer`、`build_fixup_object_index` 等重要函数缺少文档字符串。

### RF-P3-04: 混用中英文日志

**问题**: 函数内部英文注释/变量名与中文日志交替出现。`FailureType` 属性为英文但日志为中文。

### RF-P3-05: 硬编码 `default_excludes`

**位置**: 第 3154 行  
```python
default_excludes = {"tables_unsupported", "unsupported"}
```
**问题**: 默认排除的目录名硬编码在 `main` 函数中，不可配置。如果主程序将不支持对象输出到其他目录名，需要同步修改此处。  
**建议**: 从配置文件读取或定义为模块级常量

### RF-P3-06: `ScriptResult.path` 存储相对路径但类型为 `Path`

**位置**: 第 667-672 行  
**问题**: `path` 字段有时存储绝对路径（如 `sql_path`），有时存储相对路径（如 `sql_path.relative_to(repo_root)`）。类型标注仅为 `Path`，不表达这种语义差异。  
**建议**: 明确文档约定或使用不同字段

---

## 与主程序交互一致性问题

| 问题 | 主程序 | run_fixup.py | 影响 |
|------|--------|-------------|------|
| TRIGGER 权限映射 | `GRANT_PRIVILEGE_BY_TYPE["TRIGGER"] = "EXECUTE"` | 同样映射为 EXECUTE | 两端一致但都是错误的 |
| 错误码模式 | 同时检查 `ORA-` 和 `OB-` | 分类器仅识别 `ORA-` | OB 错误码不会被正确分类 |
| 密码传递 | `-p{password}` 命令行 | `-p{password}` 命令行 | 两端一致的安全风险 |
| CONSTRAINT/INDEX 执行顺序 | INDEX(5) → CONSTRAINT(6) → TRIGGER(7) | 默认: CONSTRAINT(4) → INDEX(5) → VIEW(6) | 默认模式与主程序生成顺序不一致 |
| SQL 语句分割 | `split_ddl_statements` 不支持 `q'[]'` | `split_sql_statements` **支持** `q'[]'` | run_fixup 更完善 |
| 标识符引用 | 部分函数使用双引号 | `build_compile_statement` 未引用 | 不一致 |

---

## 优先修复建议

### 立即修复（RF-P0）
1. **RF-P0-01**: 修复 `exists_cache` 陈旧数据（在 post-check 前清除缓存）
2. **RF-P0-02**: 修复 `cumulative_failed` 计数逻辑
3. **RF-P0-03**: `query_invalid_objects` 添加 OWNER 过滤
4. **RF-P0-04**: 统一两种排序模式的 CONSTRAINT/INDEX 位置

### 近期修复（RF-P1）
- **RF-P1-01**: 补充 OB 错误码分类
- **RF-P1-02**: grant 执行需同时检查 returncode 和错误输出
- **RF-P1-05**: 为所有执行模式添加连接性检查
- **RF-P1-10**: 重写文件时保留 `ALTER SESSION SET CURRENT_SCHEMA`
