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

---

# 混合深度审查报告：schema_diff_reconciler.py × run_fixup.py 端到端管线

**审查范围**: 两个程序之间的数据契约、生成→消费管线一致性、共享逻辑、跨程序缺陷  
**排除范围**: 密码存储问题、程序过大问题  
**审查日期**: 2025-02  

---

## 审查摘要

本报告聚焦于主程序（`schema_diff_reconciler.py`）与执行器（`run_fixup.py`）之间的**端到端管线一致性**。两个程序通过文件系统（fixup 目录结构、SQL 文件、依赖链报告）和配置文件（config.ini）进行隐式交互。本次审查发现 **25 个跨程序问题**，其中多数问题无法通过单独审查任何一个程序发现。

| 级别 | 数量 | 说明 |
|------|------|------|
| XR-P0 - 跨程序严重缺陷 | 6 | 端到端管线断裂或产生错误结果 |
| XR-P1 - 跨程序重要问题 | 8 | 一致性偏差影响正确性或健壮性 |
| XR-P2 - 跨程序一般问题 | 7 | 架构和维护性问题 |
| XR-P3 - 跨程序改进建议 | 4 | 工程化和可测试性 |

---

## 管线架构概览

```
schema_diff_reconciler.py                    run_fixup.py
========================                     ============

generate_fixup_scripts()                     collect_sql_files_by_layer()
  +- write_fixup_file()  --[文件系统]--->      +- parse_object_from_filename()
  |   +- {subdir}/{schema}.{name}.sql        |   +- stem.split(".", 1)
  |   +- ALTER SESSION SET CURRENT_SCHEMA    +- execute_sql_statements()
  |   +- DDL body                            |   +- split_sql_statements()
  |   +- trailing ; or /                     |   +- CURRENT_SCHEMA detection
  |   +- embedded grants                     |   +- run_sql() per statement
  |                                          |
  +- grants_all/{owner}.grants.sql           +- build_grant_index()
  +- grants_miss/{owner}.grants.sql ------>  |   +- parse_grant_statement()
  +- view_prereq_grants/                     |
  +- view_post_grants/                       +- execute_grant_file_with_prune()
  |                                          |
  +- compile/{schema}.{obj}.compile.sql      +- DEPENDENCY_LAYERS ordering
  |                                          |
  +- export_view_fixup_chains()              +- view chain autofix
      +- VIEWs_chain_{ts}.txt --[文件]--->       +- parse_view_chain_file()
                                                  +- build_view_chain_plan()
```

---

## XR-P0 - 跨程序严重缺陷（6个）

### XR-P0-01: `compile` 目录在执行器中成为孤儿目录

**生成端** (`schema_diff_reconciler.py:23222`):
```python
write_fixup_file(base_dir, 'compile', filename, content, header)
```
主程序将依赖重编译脚本生成到 `compile/` 目录。

**消费端** (`run_fixup.py:424-468`):
```python
TYPE_DIR_MAP = { ... }          # 无 "compile" 条目
DEPENDENCY_LAYERS = [ ... ]     # 无 "compile" 目录
```

**问题**: `compile` 目录既不在 `TYPE_DIR_MAP` 也不在 `DEPENDENCY_LAYERS` 中。在 smart-order 模式下，compile 脚本被归入 **layer 999**（未知层），在所有已知对象之后执行。在默认模式下同样落入 layer 999。

真正的问题是：`DIR_OBJECT_TYPE_MAP` 没有 compile 映射，导致 `obj_type = None`，auto-grant 逻辑被跳过，且 `parse_object_identity_from_path` 对 `{schema}.{obj}.compile.sql` 文件名（3段式）解析为 `schema=SCHEMA, name=OBJ`，丢弃 `.compile` 后缀。  
**影响**: 重编译脚本的对象身份解析碰巧正确，但缺少对象类型映射。如果 compile 脚本因权限不足失败，auto-grant 因 `obj_type=None` 而无法介入。

### XR-P0-02: `view_prereq_grants` 和 `view_post_grants` 不被识别为 grant 目录

**生成端** (`schema_diff_reconciler.py:23229-23283`):
```python
grant_dir_view_prereq = 'view_prereq_grants'
grant_dir_view_post = 'view_post_grants'
```

**消费端** (`run_fixup.py:470`):
```python
GRANT_DIRS = {"grants", "grants_miss", "grants_all"}
```

**问题**: `is_grant_dir()` 检查 `GRANT_DIRS`，不包含 `view_prereq_grants` 和 `view_post_grants`。这两个目录虽然出现在 `DEPENDENCY_LAYERS` 中（layer 3 和 layer 5），但在执行时走 `execute_script_with_summary` 路径（普通对象执行），而非 `execute_grant_file_with_prune` 路径（grant 专用，支持成功语句自动剔除）。

**影响**:
1. 这两个目录中的 grant 文件**不会被自动剔除成功语句**：下次运行时已成功的 GRANT 会被重复执行
2. 如果文件中有部分 GRANT 失败，整个文件被标记为 FAILED 而非像 grant 目录那样只保留失败语句
3. 成功的 view_prereq_grants 文件会被移到 done/，但失败的文件保持原样不会被重写精简

### XR-P0-03: `_compile_statements` (主程序) 与 `build_compile_statement` (执行器) 语义分歧

**主程序** (`schema_diff_reconciler.py:23173-23191`):
```python
# PACKAGE -> 生成 COMPILE + COMPILE BODY 两条语句
if obj_type_u in ("PACKAGE", "PACKAGE BODY"):
    return [
        f"ALTER PACKAGE {obj_name_u} COMPILE;",       # 不含 schema 前缀
        f"ALTER PACKAGE {obj_name_u} COMPILE BODY;"    # 不含 schema 前缀
    ]
```

**执行器** (`run_fixup.py:3013-3025`):
```python
# PACKAGE -> 仅生成 COMPILE，无 COMPILE BODY
if obj_type_u == "PACKAGE BODY":
    return f"ALTER PACKAGE {owner_u}.{name_u} COMPILE BODY;"
if obj_type_u in {"PACKAGE", ...}:
    return f"ALTER {obj_type_u} {owner_u}.{name_u} COMPILE;"
```

**差异总结**:

| 维度 | 主程序 | 执行器 |
|------|--------|--------|
| PACKAGE 编译 | COMPILE + COMPILE BODY | 仅 COMPILE |
| schema 前缀 | 无（依赖 CURRENT_SCHEMA） | `owner.name` 全限定 |
| 标识符引用 | 未引用 | 未引用 |

**影响**: 执行器的 `recompile_invalid_objects` 使用 `build_compile_statement`，对 PACKAGE 只做 COMPILE 不做 COMPILE BODY，**PACKAGE BODY 可能仍为 INVALID**。

### XR-P0-04: 嵌入式 grant 与 auto-grant 形成双重执行路径

**生成端** (`schema_diff_reconciler.py:19686-19689`):
```python
if grants_to_add:
    f.write('\n-- 自动追加相关授权语句\n')
    for grant_stmt in sorted(grants_to_add):
        f.write(f"{grant_stmt}\n")
```
主程序将 GRANT 语句**嵌入到 DDL 文件末尾**（如 trigger 脚本内嵌 `GRANT SELECT ON X.Y TO Z;`）。

**消费端** (`run_fixup.py:3419-3424`):
```python
if auto_grant_ctx and obj_full and obj_type:
    execute_auto_grant_for_object(auto_grant_ctx, obj_full, obj_type, label)
result, summary = execute_script_with_summary(...)
```

**问题**: 对于同一个对象，auto-grant 在执行文件前应用所需 grant，文件自身又包含嵌入的 GRANT 语句。两条路径独立工作：
- 同一 GRANT 被执行两次（冗余）
- **嵌入的 GRANT 失败时计入文件的 failure count**，导致本应 SUCCESS 的 DDL 文件被标记为 FAILED

### XR-P0-05: auto-grant 对 constraint/index 文件指向错误对象

**生成端** (`schema_diff_reconciler.py:22804, 23003`):
```python
# INDEX: filename = f"{ts}.{idx_name_u}.sql"      如 "HR.IDX_EMP_DEPT.sql"
# CONSTRAINT: filename = f"{ts}.{cons_name_u}.sql" 如 "HR.PK_EMPLOYEES.sql"
```

**消费端** (`run_fixup.py:3417-3424`):
```python
obj_type = DIR_OBJECT_TYPE_MAP.get(sql_path.parent.name.lower())
# "index" -> "INDEX", "constraint" -> "CONSTRAINT"
obj_schema, obj_name = parse_object_identity_from_path(sql_path)
# "HR.IDX_EMP_DEPT.sql" -> schema="HR", name="IDX_EMP_DEPT"
obj_full = f"{obj_schema}.{obj_name}"
# obj_full = "HR.IDX_EMP_DEPT", obj_type = "INDEX"
```

**问题**: auto-grant 以 `obj_full="HR.IDX_EMP_DEPT"` 和 `obj_type="INDEX"` 去查找所需权限。`GRANT_PRIVILEGE_BY_TYPE["INDEX"] = "SELECT"` 导致尝试找 `GRANT SELECT ON HR.IDX_EMP_DEPT TO ...`。但没有人对 INDEX 名做授权——授权的对象是**表**，不是索引。  
**影响**: constraint/index 文件的 auto-grant 永远找不到匹配的 grant 条目，相当于失效。

### XR-P0-06: `compile` 目录执行时机与主程序意图不符

**主程序生成顺序** (`schema_diff_reconciler.py:20599-20611`):
```
1.SEQUENCE  2.TABLE  3.TABLE_ALTER  4.VIEW/MVIEW
5.INDEX  6.CONSTRAINT  7.TRIGGER  8.COMPILE  9.GRANTS
```
主程序意图：compile 在 trigger 之前（第 8 步），因为 trigger 可能依赖已重编译的对象。

**执行器**: `compile` 落入 layer 999（两种模式都如此），在 trigger（layer 12）和 job（layer 13）之后执行。  
**影响**: trigger 可能因依赖未重编译的对象而执行失败。

---

## XR-P1 - 跨程序重要问题（8个）

### XR-P1-01: `split_ddl_statements` (主程序) 与 `split_sql_statements` (执行器) PL/SQL 块处理机制不同

**主程序** (`schema_diff_reconciler.py:19266-19405`):
- 使用 `BEGIN/END` 深度计数识别 PL/SQL 块
- 不处理 `/` 终结符
- 支持 `q'...'` 引用

**执行器** (`run_fixup.py:2207-2310`):
- 使用 `RE_BLOCK_START` 正则检测 `CREATE PROCEDURE/FUNCTION/PACKAGE/TYPE/TRIGGER` 和 `DECLARE/BEGIN`
- 使用独立行 `/` 作为 PL/SQL 块终结符
- 支持 `q'...'` 引用

**问题**: 主程序的 `write_fixup_file` 在写入 PL/SQL 对象时：
```python
tail = body.rstrip()
if tail and not tail.endswith((';', '/')):
    f.write(';\n')
```
如果 PL/SQL 块以 `END pkg_name;` 结尾（已有 `;`），不会追加 `/`。但执行器的 `split_sql_statements` 需要 `/` 来识别 PL/SQL 块的结束。

**场景**: PACKAGE BODY 文件：
```sql
ALTER SESSION SET CURRENT_SCHEMA = HR;
CREATE OR REPLACE PACKAGE BODY pkg AS ... END pkg;
```
执行器检测到 `CREATE ... PACKAGE BODY` 进入 `slash_block = True` 模式，等待独立行 `/` 来结束块。但文件中没有 `/`，整个文件成为一个超长语句。  
**影响**: PL/SQL 对象（PACKAGE, PACKAGE BODY, TYPE, TYPE BODY, PROCEDURE, FUNCTION, TRIGGER）的 fixup 文件可能在执行器中被错误分割。

### XR-P1-02: 主程序生成的 grant 文件命名与执行器的对象索引交互

**主程序**: 生成 `{owner}.grants.sql` 和 `{grantee}.privs.sql`。  
**执行器**: `parse_object_from_filename` 将 `HR.grants.sql` 解析为 `schema=HR, name=GRANTS`。

虽然 grant 目录走 `execute_grant_file_with_prune` 路径（不需要对象身份），但 `view_prereq_grants` 和 `view_post_grants` 不在 `GRANT_DIRS` 中（见 XR-P0-02），其文件会被 `build_fixup_object_index` 尝试索引为创建对象，产生无意义的索引条目。

### XR-P1-03: View chain 文件中的 EXISTS/GRANT_STATUS 元数据被丢弃

**主程序** (`schema_diff_reconciler.py:10983`):
```python
parts.append(f"{node[0]}[{obj_type}|{exists}|{grant_status}]")
# 输出: "HR.V_DEPT[VIEW|MISSING|GRANT_OK]"
```

**执行器** (`run_fixup.py:1241`):
```python
obj_type = raw_meta.split("|", 1)[0].strip().upper()
# 只取第一段: "VIEW"，丢弃 "MISSING" 和 "GRANT_OK"
```

**问题**: 主程序花费数据库查询确定每个对象的存在状态和授权状态，但执行器完全忽略这些信息，在 `build_view_chain_plan` 中重新查询。  
**影响**: 浪费已有信息，且重新查询时数据库状态可能已变化。

### XR-P1-04: 超时配置优先级不同

**主程序**: 使用 `obclient_timeout` 配置项。  
**执行器** (`run_fixup.py:814-825`):
```python
fixup_raw = parser.get("SETTINGS", "fixup_cli_timeout", fallback="")
if fixup_raw:
    fixup_timeout = int(fixup_raw)
else:
    fixup_timeout = parser.getint("SETTINGS", "obclient_timeout", fallback=DEFAULT_FIXUP_TIMEOUT)
```

**问题**: 执行器优先使用 `fixup_cli_timeout`，主程序不识别此配置项。如果同时设置 `obclient_timeout=300` 和 `fixup_cli_timeout=60`，主程序用 300 秒，执行器用 60 秒。对大表 DDL 可能不够。

### XR-P1-05: `GRANT_PRIVILEGE_BY_TYPE` 在两个程序中独立定义

两份独立副本，任何一方修改后不会自动同步。两份都包含相同的错误（TRIGGER 映射为 EXECUTE），但如果只修正一方会导致行为分歧。  
**类似重复**: `SYS_PRIV_IMPLICATIONS`、`obj_type_to_dir`/`TYPE_DIR_MAP` 也存在双副本问题。

### XR-P1-06: `write_fixup_file` 的 `;` 追加逻辑与执行器的 PL/SQL 分割交互

主程序在 DDL 末尾没有 `;` 或 `/` 时追加 `;`。但对于已以 `;` 结尾的 PL/SQL 块不追加 `/`。执行器的 `split_sql_statements` 中 `slash_block` 模式需要 `/` 来结束块。

此外，嵌入 grants 写在 PL/SQL 块之后，执行器在 `slash_block=True` 时不在 `;` 处分割，导致整个文件（包括嵌入的 GRANT）成为一个语句。  
**关联**: 与 XR-P1-01 同根同源，建议主程序对 PL/SQL 对象始终追加 `/` 终结符。

### XR-P1-07: `unsupported/` 子目录的嵌套结构与用户预期

**主程序** (`schema_diff_reconciler.py:22722`):
```python
subdir = f"unsupported/{obj_type_to_dir.get(ot, ot.lower())}"
# 生成: unsupported/view/SCHEMA.NAME.sql (两层嵌套)
```

**执行器**: 默认排除 `unsupported`。如果用户通过 `--only-dirs unsupported` 尝试执行，`iter_sql_files_recursive` 递归搜索可找到文件，但 `sql_path.parent.name` 是 `"view"` 而非 `"unsupported"`，导致这些文件被当作普通 view 对象处理。  
**影响**: 用户无法按类型选择性执行 unsupported 对象。

### XR-P1-08: 主程序 `build_compile_order` 与执行器 `topo_sort_nodes` 有相同的 O(N^2) 内存问题

**主程序** (`schema_diff_reconciler.py:21065`):
```python
dfs(dep, stack + [f"{node[0]}.{node[1]} ({node[2]})"])
```

**执行器** (`run_fixup.py:1354`): 相同的 `stack + [node]` 模式。

两个程序的拓扑排序 DFS 都使用 `stack + [node]` 创建新列表，O(N^2) 内存。这是共享的代码模式缺陷。  
**建议**: 统一为就地修改的 `stack.append(node)` / `stack.pop()` 实现。

---

## XR-P2 - 跨程序一般问题（7个）

### XR-P2-01: 无共享常量模块

`GRANT_PRIVILEGE_BY_TYPE`、`SYS_PRIV_IMPLICATIONS`、`TYPE_DIR_MAP`/`obj_type_to_dir`、`GRANT_OPTION_TYPES` 等常量在两个文件中独立定义。  
**建议**: 提取到共享的 `constants.py` 模块中。

### XR-P2-02: 无正式的文件格式契约

`write_fixup_file` 的输出格式（注释头、DDL body、可选 `;`、可选嵌入 grants）是隐式的。执行器通过逆向工程式的解析来消费这些文件，任何格式变更都可能悄无声息地破坏管线。  
**建议**: 定义正式的文件格式文档或 schema。

### XR-P2-03: config.ini 配置项缺乏统一文档

- `fixup_cli_timeout` 仅被 run_fixup 使用
- `fixup_auto_grant` 仅被 run_fixup 使用
- `fixup_dir` 被两个程序使用但解析路径逻辑不同
- `report_dir` 被两个程序使用

配置项无统一的 schema 文档，用户不知道哪些配置影响哪个程序。

### XR-P2-04: 主程序的 `_compile_statements` 有死代码

**位置** (`schema_diff_reconciler.py:23176, 23189-23190`):
```python
if obj_type_u in ("VIEW", "MATERIALIZED VIEW", "TYPE BODY"):
    return []     # TYPE BODY 在这里返回空
...
if obj_type_u == "TYPE BODY":
    return [f"ALTER TYPE {obj_name_u} COMPILE BODY;"]  # 死代码，永远不会执行
```

### XR-P2-05: 错误码分类覆盖范围差异

主程序 `RE_SQL_ERROR` 检测 `ORA-`、`OB-`、`ERROR ` 模式用于提取错误信息。执行器 `classify_sql_error` 对部分 OB 错误码有分类（如 `OB-00942`、`OB-01031`），但大量 OB 特有错误码（如 `OB-00600`、`OB-04012`）未分类。  
**影响**: 迭代模式的智能重试对 OB 特有错误效果有限。

### XR-P2-06: 主程序并行生成 fixup 文件但无原子性保证

主程序 `generate_fixup_scripts` 使用 `ThreadPoolExecutor` 并行生成 DDL 文件。如果在生成过程中崩溃，`fixup_dir` 中可能存在部分生成的文件集合。执行器假设文件集合完整一致，没有检查文件是否截断。

### XR-P2-07: grant merge 策略影响执行器的 grant 解析

主程序 `grant_merge_privileges=true` 时生成合并语句如 `GRANT SELECT, INSERT ON HR.T TO APP, BATCH;`。执行器 `RE_GRANT_OBJECT` 使用 `(?P<privs>.+?)` 非贪婪匹配。对象名包含 `ON` 子串时（如 `HR.PKG_ON_DELETE`）grant 解析可能失败。

---

## XR-P3 - 跨程序改进建议（4个）

### XR-P3-01: 缺少端到端集成测试

**建议**: 创建集成测试覆盖：PL/SQL 对象的生成到分割到执行、嵌入 grant 的生成到执行、view chain 的生成到解析到计划、constraint/index 的 auto-grant 路径。

### XR-P3-02: 考虑生成 manifest 文件

**建议**: 主程序在生成完成后写入 `fixup_manifest.json`，记录所有生成的文件、对象类型、schema 等元数据。执行器直接读取 manifest 而非从文件名逆向推导对象身份。

### XR-P3-03: 统一 SQL 分割器

两个程序各有 SQL 语句分割器（`split_ddl_statements` 和 `split_sql_statements`），处理逻辑不同。应统一到一个健壮的共享实现。

### XR-P3-04: 统一 DFS 拓扑排序实现

两个程序的拓扑排序 DFS 都有 O(N^2) 内存问题。应提取为共享实现并使用就地修改的 stack。

---

## 优先修复建议

### 立即修复（XR-P0）
1. **XR-P0-01/06**: 在 `TYPE_DIR_MAP` 和 `DEPENDENCY_LAYERS` 中添加 `compile` 映射，确保位于 trigger 之前
2. **XR-P0-02**: 将 `view_prereq_grants`/`view_post_grants` 加入 `GRANT_DIRS`，或为它们实现 grant 专用执行逻辑
3. **XR-P0-03**: `build_compile_statement` 对 PACKAGE 类型也生成 COMPILE BODY
4. **XR-P0-04**: 在执行文件前检查并剔除嵌入的 GRANT（如果 auto-grant 已处理），或统一为单一路径
5. **XR-P0-05**: constraint/index 的 auto-grant 需要从文件内容或元数据中获取**表名**而非索引/约束名

### 近期修复（XR-P1）
- **XR-P1-01/06**: 主程序 `write_fixup_file` 对 PL/SQL 对象始终追加 `/` 终结符
- **XR-P1-05**: 提取共享常量模块，消除重复定义
- **XR-P1-04**: 统一文档说明 `fixup_cli_timeout` 与 `obclient_timeout` 的优先级关系

---

# Oracle 迁移遗漏对象审查报告

**审查范围**: 主程序 `schema_diff_reconciler.py` 当前校验覆盖范围 vs Oracle 数据库中所有可迁移对象类型和属性  
**审查目标**: 识别人工迁移中可能被遗漏、且主程序尚未校验的对象类型和属性  
**审查日期**: 2025-02  

---

## 1. 当前校验覆盖范围总结

### 1.1 已覆盖的对象类型

| 对象类型 | 校验深度 | 数据源 |
|---------|---------|--------|
| TABLE | 列名集合 + VARCHAR长度窗口 + NUMBER精度 + 虚拟列 + Identity + Default On Null + Invisible | DBA_TAB_COLUMNS |
| VIEW | 存在性 + DDL兼容性分析 | DBA_OBJECTS + DBA_VIEWS |
| MATERIALIZED VIEW | 仅打印（不校验不生成fixup） | DBA_MVIEWS |
| PROCEDURE / FUNCTION | 存在性 + VALID/INVALID状态 | DBA_OBJECTS |
| PACKAGE / PACKAGE BODY | 存在性 + VALID/INVALID + 错误信息 | DBA_OBJECTS + DBA_ERRORS |
| TYPE / TYPE BODY | 存在性 + VALID/INVALID | DBA_OBJECTS + DBA_TYPES + DBA_SOURCE |
| SYNONYM | 存在性 | DBA_SYNONYMS |
| JOB / SCHEDULE | 存在性 | DBA_OBJECTS |
| INDEX | 列组合 + 唯一性 | DBA_INDEXES + DBA_IND_COLUMNS |
| CONSTRAINT (PK/UK/FK/CK) | 列组合 + 引用表 + 删除/更新规则 + Deferrable | DBA_CONSTRAINTS + DBA_CONS_COLUMNS |
| SEQUENCE | 仅存在性（不比较属性） | DBA_SEQUENCES |
| TRIGGER | 存在性 + 事件 + 启用状态 + 有效性 | DBA_TRIGGERS |
| TABLE/COLUMN COMMENTS | 注释文本比较 | DBA_TAB_COMMENTS + DBA_COL_COMMENTS |
| GRANTS | 对象权限 + 系统权限 + 角色权限 | DBA_TAB_PRIVS + DBA_SYS_PRIVS + DBA_ROLE_PRIVS |
| DEPENDENCIES | 依赖关系 | DBA_DEPENDENCIES |
| INTERVAL PARTITIONS | 分区边界补齐（仅interval类型） | DBA_PART_TABLES + DBA_TAB_PARTITIONS |

### 1.2 已加载但未比较的属性

以下属性已从 Oracle/OB 加载到内存，但**未执行源→目标对比**：

| 属性 | 加载位置 | 当前用途 | 是否比较 |
|------|---------|---------|---------|
| `DATA_DEFAULT`（列默认值） | OracleMetadata.table_columns / ObMetadata.tab_columns | 仅用于 ALTER ADD 生成 | **未比较** |
| `NULLABLE` | 同上 | 仅用于 ALTER ADD 生成 | **未比较** |
| `sequence_attrs`（INCREMENT_BY, MIN/MAX_VALUE, CACHE_SIZE, CYCLE_FLAG, ORDER_FLAG） | OracleMetadata.sequence_attrs / ObMetadata.sequence_attrs | 已加载，未使用 | **未比较** |
| `partition_key_columns` | OracleMetadata/ObMetadata | 仅用于PK降级判定和interval补齐 | **未比较分区方案** |

---

## 2. 遗漏对象类型和属性分析

### GAP-P0 - 高优先级遗漏（生产环境常见，数据正确性风险）

#### GAP-P0-01: 列默认值（DATA_DEFAULT）未比较

**现状**: 主程序从 `DBA_TAB_COLUMNS` 加载 `DATA_DEFAULT` 到 `OracleMetadata.table_columns` 和 `ObMetadata.tab_columns`，但表对比逻辑（`schema_diff_reconciler.py:12862-13084`）**完全不比较**此属性。

**Oracle 查询**:
```sql
REPLACE(REPLACE(REPLACE(DATA_DEFAULT, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ') AS DATA_DEFAULT
```

**影响**: 如果 OMS 迁移时丢失或修改了列默认值：
- `INSERT` 不带该列时产生不同的值（NULL vs 预期默认值）
- `SYSDATE`、`SYS_GUID()`、`USER` 等函数型默认值在迁移后可能语法不兼容
- `DEFAULT ON NULL` 属性虽已校验，但基础 DEFAULT 值本身未校验

**常见场景**: OMS 迁移大表后手动重建，忘记带 DEFAULT 子句；或 OceanBase 不支持某些默认值表达式导致静默丢失。

**修复建议**: 在列级比较的 `common_cols` 循环中，增加 `data_default` 标准化后的比较。需要处理：
- 去空格/换行标准化
- `NULL` vs 空字符串
- `SYSDATE` vs `CURRENT_TIMESTAMP` 等 OB 替代表达式
- 表达式中的 schema 前缀 remap

#### GAP-P0-02: 列可空性（NULLABLE）未比较

**现状**: `NULLABLE` 属性已加载（`Y`/`N`），但列级对比中没有 nullable 比较逻辑。

**影响**: 如果源端 `NOT NULL` 列在目标端变成了 `NULL`：
- 数据完整性约束丢失
- 应用程序假设非空的列可能插入 NULL
- NOT NULL 约束在 `DBA_CONSTRAINTS` 中以 `C` 类型 `SYS_C*` 名称存在，虽然 CHECK 约束比较会过滤掉 `is_system_notnull_check`，但这是**有意为之**的——因为它假设 NULLABLE 属性会在列级比较中处理。**然而实际上列级比较并没有处理它**。

**修复建议**: 在列级比较中增加 `nullable` 属性对比。源端 `N`（NOT NULL）而目标端 `Y`（NULLABLE）应报告为 mismatch。

#### GAP-P0-03: 序列属性未比较

**现状**: `DBA_SEQUENCES` 的 `INCREMENT_BY`、`MIN_VALUE`、`MAX_VALUE`、`CYCLE_FLAG`、`ORDER_FLAG`、`CACHE_SIZE` 已加载到 `OracleMetadata.sequence_attrs` 和 `ObMetadata.sequence_attrs`，但 `compare_sequences_for_schema`（line 14424）仅比较序列**存在性**。

**影响**:
- `INCREMENT_BY` 不同：自增步长错误，主键可能冲突（如果切换前后交替写入）
- `CACHE_SIZE` 不同：性能差异，且 OB 默认 CACHE 可能与 Oracle 不同
- `CYCLE_FLAG` 不同：序列到达 MAX_VALUE 后行为不同
- `MIN_VALUE`/`MAX_VALUE` 不同：序列范围不一致

**修复建议**: 扩展 `SequenceMismatch` 增加 `attr_mismatches` 字段，在存在性检查通过后，对公共序列逐一比较属性。

#### GAP-P0-04: 分区定义未比较（仅覆盖 interval 补齐）

**现状**: 主程序仅对 `INTERVAL` 分区表检查边界并生成补齐脚本。`RANGE`、`LIST`、`HASH` 分区定义**完全不比较**。分区键列虽已加载（`partition_key_columns`）但仅用于 PK 降级判定。

**影响**:
- 表可能在 OB 端完全没有分区（OMS 不迁移分区定义的情况）
- 分区键列不同：查询性能剧烈下降，分区裁剪失效
- RANGE 分区边界不同：数据路由错误
- LIST 分区值列表不同：插入可能失败（DEFAULT 分区缺失）
- 子分区模板丢失

**修复建议**: 增加 `DBA_PART_TABLES` 分区方案比较（分区类型 + 分区数 + 分区键列 + 关键边界值），至少做"分区类型和分区键列"级别的校验。

#### GAP-P0-05: PL/SQL 源代码未比较（仅校验存在性和状态）

**现状**: 对于 PROCEDURE、FUNCTION、PACKAGE、PACKAGE BODY、TYPE、TYPE BODY，主程序仅校验：
1. 对象是否存在（`DBA_OBJECTS`）
2. 状态是否为 VALID/INVALID
3. 错误信息（仅 PACKAGE 类型通过 `DBA_ERRORS`）

**未覆盖**: 源代码文本（`DBA_SOURCE`）的一致性**完全不比较**。

**影响**:
- 人工迁移时可能使用了旧版本的存储过程
- OMS 迁移 PL/SQL 后可能因兼容性改写导致逻辑变化
- 手动修改的 PL/SQL 代码不会被发现
- 两端都 VALID 但**逻辑完全不同**的情况不会被检测到

**常见场景**: 开发团队在 Oracle 端更新了存储过程，但忘记同步到 OB 端。因为对象 VALID，主程序报告一切正常。

**修复建议**: 增加可选的源代码哈希比较（`DBA_SOURCE` 按 OWNER+NAME+TYPE+LINE 拼接后取 MD5/SHA256），标记为 `check_plsql_source=true` 可选开关。不需要逐行比较，哈希不一致即报告 mismatch。

#### GAP-P0-06: DATABASE LINK 未校验

**现状**: 主程序对视图中的 DBLINK 引用有检测（`view_dblink_policy`），但**不校验 DATABASE LINK 对象本身**是否存在于目标端。

**Oracle 数据源**: `DBA_DB_LINKS`（OWNER, DB_LINK, HOST, USERNAME）

**影响**:
- 存储过程中引用 `table@dblink_name` 的代码在运行时才会失败
- 同义词指向的 DB LINK 目标不可达
- JOB 中调用跨库过程失败

**修复建议**: 从 `DBA_DB_LINKS` 加载 DB LINK 清单，对比源端和目标端的 DB_LINK 名称存在性。如果 DB LINK 不存在，将引用它的对象标记为 UNSUPPORTED。

---

### GAP-P1 - 中优先级遗漏（影响功能完整性或运维）

#### GAP-P1-01: MATERIALIZED VIEW LOG 未校验

**现状**: `MATERIALIZED VIEW` 在主程序中被归为 `PRINT_ONLY_PRIMARY_TYPES`（仅打印不校验）。与 MVIEW 配套的 **MVIEW LOG**（物化视图日志表）完全未被追踪。

**Oracle 数据源**: `DBA_MVIEW_LOGS`（LOG_OWNER, MASTER, LOG_TABLE）

**影响**:
- 如果目标端使用快速刷新（FAST REFRESH）的物化视图，缺少 MVIEW LOG 会导致刷新失败
- MVIEW LOG 表可能被 OMS 当作普通表迁移了数据，但结构可能不完整

**修复建议**: 在 `EXTRA_OBJECT_CHECK_TYPES` 中增加 `MATERIALIZED VIEW LOG` 或作为 `MATERIALIZED VIEW` 校验的附属检查。

#### GAP-P1-02: 索引属性未深度比较

**现状**: INDEX 比较仅校验**列组合**和**唯一性**。以下属性未比较：

| 索引属性 | 影响 |
|---------|------|
| COMPRESSION | 存储空间和查询性能 |
| VISIBILITY (INVISIBLE) | 优化器是否使用该索引 |
| LOCAL/GLOBAL (分区索引) | 分区表性能关键 |
| REVERSE | 热点消除 |
| FUNCTION-BASED 表达式细节 | 函数索引的正确性 |
| TABLESPACE | 存储位置 |
| PARALLEL | 并行度 |

**影响**: 索引"存在"但属性不同，可能导致：
- LOCAL 分区索引变成 GLOBAL 索引，分区维护操作 (DROP/TRUNCATE PARTITION) 会失败或极慢
- INVISIBLE 索引在目标端变成 VISIBLE，影响执行计划
- FUNCTION-BASED 表达式不同导致查询无法利用索引

**修复建议**: 优先增加 LOCAL/GLOBAL 和 INVISIBLE 属性比较，这两个属性影响最大且容易实现。

#### GAP-P1-03: 约束启用/禁用状态校验不完整

**现状**: 约束的 `STATUS`（ENABLED/DISABLED）和 `VALIDATED`/`NOT VALIDATED` 状态仅通过 `check_status_drift_types` 开关做有限检查（`STATUS_DRIFT_CHECK_TYPES` 仅含 TRIGGER 和 CONSTRAINT）。但状态漂移检查是**可选的**，默认可能未启用 CONSTRAINT。

**影响**:
- 源端 DISABLED 的 FK 约束在目标端变成 ENABLED：插入/更新可能因约束违反而失败
- 源端 ENABLED 的约束在目标端变成 DISABLED：数据完整性保证丢失

#### GAP-P1-04: 数据类型深度比较不足

**现状**: 列类型比较仅覆盖 VARCHAR/VARCHAR2 长度和 NUMBER 精度。以下类型变化未检测：

| 类型场景 | 风险 |
|---------|------|
| CHAR(N) ↔ VARCHAR2(N) | 语义不同（CHAR 补空格） |
| NCHAR/NVARCHAR2 ↔ CHAR/VARCHAR2 | 字符集语义变化 |
| DATE ↔ TIMESTAMP | 精度丢失（DATE 无毫秒） |
| TIMESTAMP 精度差异 | 微秒/纳秒精度丢失 |
| RAW(N) 长度 | 二进制数据截断 |
| CLOB ↔ VARCHAR2 | 长度限制和接口差异 |
| FLOAT ↔ NUMBER | 精度语义不同 |
| LONG/LONG RAW → CLOB/BLOB | 已检测，但转换完整性未验证 |

**修复建议**: 增加通用的 `data_type` 标准化比较，至少检测基础类型名称是否一致（忽略 Oracle→OB 的已知映射规则）。

#### GAP-P1-05: 表存储属性和表级特性未比较

**未校验的表级属性**:
- **LOGGING/NOLOGGING**: 影响恢复和性能
- **ROW MOVEMENT**: 分区表 UPDATE 分区键时需要
- **TABLE COMPRESSION**: 存储和性能
- **PARALLEL DEGREE**: 查询并行度
- **CACHE**: 小表全缓存特性
- **RESULT_CACHE**: 结果缓存策略

#### GAP-P1-06: 用户/Schema 创建状态未校验

**现状**: 主程序假设目标端 schema 已存在。如果 schema 不存在，所有该 schema 下的对象校验都会失败，但错误信息分散在各对象的 MISSING 报告中，难以定位根因。

**修复建议**: 在元数据加载阶段，先验证目标端所有 remap 后的 schema 是否存在（`DBA_USERS` 查询），不存在则立即报告并提示建 schema。

#### GAP-P1-07: VPD/RLS 行级安全策略未校验

**Oracle 数据源**: `DBA_POLICIES`（OBJECT_OWNER, OBJECT_NAME, POLICY_NAME, FUNCTION, ENABLE）

**影响**: 如果源端表有行级安全策略，迁移后：
- 查询返回全部数据（安全策略丢失）
- 应用程序依赖 VPD 的多租户隔离失效

**注**: OceanBase Oracle 模式已支持 DBMS_RLS，但需要手动迁移策略。

---

### GAP-P2 - 低优先级遗漏（较少见但可能造成问题）

#### GAP-P2-01: DIRECTORY 对象未校验

**Oracle 数据源**: `DBA_DIRECTORIES`（DIRECTORY_NAME, DIRECTORY_PATH）

**影响**: `UTL_FILE`、`DATAPUMP`、外部表依赖 DIRECTORY 对象。缺失会导致运行时错误。

#### GAP-P2-02: 表空间映射未校验

**影响**: 对象可能创建在错误的表空间中，影响存储管理和性能隔离。虽然 OceanBase 的表空间语义与 Oracle 不完全相同，但在 Oracle 兼容模式下仍有意义。

#### GAP-P2-03: PROFILE（资源配置文件）未校验

**Oracle 数据源**: `DBA_PROFILES`（PROFILE, RESOURCE_NAME, LIMIT）

**影响**: 密码策略（密码过期时间、登录失败锁定次数）和资源限制（CPU_PER_SESSION、SESSIONS_PER_USER）丢失。

#### GAP-P2-04: CONTEXT（应用上下文）未校验

**Oracle 数据源**: `DBA_CONTEXT`（NAMESPACE, SCHEMA, PACKAGE）

**影响**: 使用 `SYS_CONTEXT('namespace', 'attribute')` 的 PL/SQL 代码会在运行时失败。主程序已有 `CHECK_SYS_CONTEXT_USERENV_RE` 检测 CHECK 约束中的 `SYS_CONTEXT`，但不检查 CONTEXT 对象本身。

#### GAP-P2-05: 行数/数据量粗粒度校验

**现状**: 主程序不校验源端和目标端的数据行数或数据量。

**影响**: 数据迁移可能不完整（部分表数据丢失），但因为只校验结构不校验数据，无法发现。

**修复建议**: 增加可选的 `check_row_counts=true` 开关，对已匹配的表执行 `SELECT COUNT(*) FROM table` 粗粒度行数比较。

#### GAP-P2-06: AUDIT 配置未校验

**Oracle 数据源**: `DBA_AUDIT_TRAIL` / `DBA_STMT_AUDIT_OPTS` / `DBA_OBJ_AUDIT_OPTS`

**影响**: 审计策略丢失意味着合规性要求无法满足。

#### GAP-P2-07: JAVA 对象未校验

**Oracle 数据源**: `DBA_JAVA_CLASSES` / `DBA_JAVA_METHODS`

**影响**: Java 存储过程在 OceanBase 中不支持。如果有 PL/SQL 代码调用 Java 过程，应标记为 UNSUPPORTED。

---

## 3. 优先级矩阵

| 编号 | 遗漏项 | 数据已加载 | 实现难度 | 生产影响 | 建议优先级 |
|------|--------|-----------|---------|---------|-----------|
| GAP-P0-01 | 列默认值 | ✅ 已加载 | 中（需标准化） | **高** | **立即** |
| GAP-P0-02 | 列可空性 | ✅ 已加载 | **低** | **高** | **立即** |
| GAP-P0-03 | 序列属性 | ✅ 已加载 | **低** | **高** | **立即** |
| GAP-P0-04 | 分区定义 | 部分加载 | 中 | **高** | 尽快 |
| GAP-P0-05 | PL/SQL 源码 | 未加载 | 中 | **高** | 尽快 |
| GAP-P0-06 | DATABASE LINK | 未加载 | 低 | 高 | 尽快 |
| GAP-P1-01 | MVIEW LOG | 未加载 | 低 | 中 | 近期 |
| GAP-P1-02 | 索引属性深度 | 部分加载 | 中 | 中 | 近期 |
| GAP-P1-03 | 约束启用状态 | 部分实现 | 低 | 中 | 近期 |
| GAP-P1-04 | 数据类型深度 | ✅ 已加载 | 中 | 中 | 近期 |
| GAP-P1-05 | 表存储属性 | 未加载 | 中 | 低-中 | 后续 |
| GAP-P1-06 | Schema 存在性 | 未加载 | **低** | 中 | 近期 |
| GAP-P1-07 | VPD/RLS | 未加载 | 中 | 中 | 后续 |
| GAP-P2-01~07 | DIRECTORY/TABLESPACE/PROFILE/CONTEXT/AUDIT/JAVA/行数 | 未加载 | 各异 | 低 | 后续 |

---

## 4. 快速收益建议（投入产出比最高的前 3 项）

### 第一优先：GAP-P0-02 列可空性比较
- **投入**: ~20 行代码
- **实现**: 在 `common_cols` 循环中增加 `src_nullable != tgt_nullable` 判断
- **收益**: 立即发现所有 NOT NULL → NULL 的退化，这是人工迁移中最常见的遗漏之一

### 第二优先：GAP-P0-03 序列属性比较
- **投入**: ~50 行代码
- **实现**: 数据已在 `sequence_attrs` 中，只需在 `check_extra_objects` 的序列校验部分增加属性比对循环
- **收益**: 发现 INCREMENT_BY / CACHE_SIZE 不一致，防止自增主键冲突

### 第三优先：GAP-P0-01 列默认值比较
- **投入**: ~80 行代码（含标准化逻辑）
- **实现**: 标准化 `data_default` 后比较，需要处理 NULL/空字符串、函数名映射
- **收益**: 发现默认值丢失，防止 INSERT 行为异常

---

## 5. "多余对象"检测能力分析（目标端有、源端无）

### 5.1 当前已具备的多余对象检测

主程序**已具备**多余对象（目标端存在但源端不存在）的检测能力，覆盖范围如下：

| 检测层级 | 实现位置 | 输出方式 | 粒度 |
|---------|---------|---------|------|
| **主对象（TABLE/VIEW/PROCEDURE 等）** | `check_primary_objects` → `extra_targets` (line 13138-13144) | `extra_targets_detail_{timestamp}.txt` + 控制台 "目标端多出的对象" | **具体对象名** |
| **INDEX** | `IndexMismatch.extra_indexes` | 报告 mismatch 明细 | **具体索引名（按表）** |
| **CONSTRAINT** | `ConstraintMismatch.extra_constraints` | 报告 mismatch 明细 | **具体约束名（按表）** |
| **SEQUENCE** | `SequenceMismatch.extra_sequences` | 报告 mismatch 明细 | **具体序列名（按 schema）** |
| **TRIGGER** | `TriggerMismatch.extra_triggers` | 报告 mismatch 明细 | **具体触发器名（按表）** |
| **TABLE 列** | `extra_in_tgt`（列名集合差集） | mismatched 报告 | **具体列名** |
| **各类型数量汇总** | `compute_object_counts` → `summary["extra"]` | 报告汇总 + Report DB | **仅数量（INDEX/CONSTRAINT 为近似值）** |

### 5.2 多余对象检测的盲区

#### EXTRA-GAP-01: `print_only` 类型的多余对象不检测

**现状**: `MATERIALIZED VIEW` 被归入 `PRINT_ONLY_PRIMARY_TYPES`，`check_primary_objects` 中对 `print_only` 类型只记录 `skipped`，**不参与 `extra_targets` 检测**（line 13139 排除了 `print_only_types_u`）。

```python
for obj_type in sorted((allowed_types - print_only_types_u) - set(PACKAGE_OBJECT_TYPES)):
```

**影响**: 如果 OB 端存在源端不存在的 MATERIALIZED VIEW，不会被报告为多余对象。

#### EXTRA-GAP-02: INDEX/CONSTRAINT 的多余数量为近似值

**现状**: `compute_object_counts` 对 INDEX 和 CONSTRAINT 使用总数差值 `max(0, tgt_count - src_count)` 计算"多余"数量，**不是精确的名称级比较**。系统生成的索引/约束名在源端和目标端可能不同，导致名称级别的多余/缺失统计偏高。

**注**: 实际的名称级多余检测由 `IndexMismatch.extra_indexes` / `ConstraintMismatch.extra_constraints` 完成，这部分是准确的（按列组合匹配后取差集）。但汇总数量可能不一致。

#### EXTRA-GAP-03: 多余对象不生成清理脚本

**现状**: 对于检测到的多余对象，主程序**仅报告不处理**。不生成 `DROP` 脚本来清理目标端的多余对象。

**影响**:
- 多余的触发器可能在 DML 时执行非预期逻辑
- 多余的索引影响写入性能和存储空间
- 多余的约束可能阻止合法的数据插入
- 多余的存储过程/函数可能被应用误调用

**建议**: 增加可选的 `generate_extra_cleanup=true` 开关，为多余对象生成 `DROP` 脚本（放入 `fixup_scripts/cleanup/` 目录），但**默认关闭**以避免误删。脚本应包含注释说明该对象在源端不存在。

#### EXTRA-GAP-04: 未追踪类型的多余对象完全不可见

与"遗漏对象"（第 2 章）对应，以下类型的多余对象同样无法检测：

| 类型 | 多余风险说明 |
|------|------------|
| DATABASE LINK | OB 端可能残留测试环境的 DB LINK，指向错误目标 |
| DIRECTORY | 残留的 DIRECTORY 可能指向不存在的路径 |
| VPD POLICY | 目标端可能有测试策略未清理，导致查询行为异常 |
| PROFILE | 多余的 PROFILE 不影响功能，风险低 |
| CONTEXT | 多余的 CONTEXT 不影响功能，风险低 |

### 5.3 多余对象检测的改进建议

| 优先级 | 改进项 | 投入 |
|--------|--------|------|
| **高** | EXTRA-GAP-03: 为多余 TRIGGER / INDEX / CONSTRAINT 生成可选的 DROP 脚本 | ~100 行 |
| 中 | EXTRA-GAP-01: 将 MATERIALIZED VIEW 纳入 `extra_targets` 检测 | ~5 行 |
| 中 | EXTRA-GAP-02: INDEX/CONSTRAINT 汇总数量改为精确统计 | ~30 行 |
| 低 | EXTRA-GAP-04: 扩展对 DB LINK / DIRECTORY 的多余对象检测 | ~60 行 |

---

## 6. 总结

本报告从两个方向审查了 `schema_diff_reconciler.py` 的校验覆盖范围：

**方向一：遗漏对象（源端有、目标端无/不一致）**
- 发现 **6 项 P0 高优先级**遗漏，其中 3 项（列可空性、序列属性、列默认值）数据已在内存，实现成本极低
- 发现 **7 项 P1** 和 **7 项 P2** 遗漏，覆盖索引深度属性、数据类型、DB LINK、MVIEW LOG 等

**方向二：多余对象（目标端有、源端无）**
- 主程序已具备名称级多余对象检测（覆盖主要类型 + INDEX/CONSTRAINT/SEQUENCE/TRIGGER）
- 发现 **4 项盲区**，最关键的是**多余对象不生成清理脚本**（EXTRA-GAP-03），可能导致多余触发器/约束在生产环境造成非预期行为
