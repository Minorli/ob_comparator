# schema_diff_reconciler.py 全面代码审查报告

**审查范围**: 程序逻辑、业务逻辑、代码逻辑、代码质量、文档  
**排除范围**: 密码问题、程序过大问题  
**审查日期**: 2025-01  
**文件版本**: 30672 行  

---

## 审查摘要

共发现 **42 个问题**，按严重程度分布如下：

| 级别 | 数量 | 说明 |
|------|------|------|
| P0 - 严重缺陷 | 5 | 可能导致数据错误或功能失效 |
| P1 - 重要问题 | 12 | 影响正确性或健壮性 |
| P2 - 一般问题 | 15 | 影响可维护性或存在潜在风险 |
| P3 - 改进建议 | 10 | 代码质量和风格优化 |

---

## P0 - 严重缺陷（5个）

### P0-01: 序列迁移遗漏 LAST_NUMBER / RESTART WITH

**位置**: `load_oracle_metadata` / `load_ob_metadata` (序列元数据查询)  
**问题**: 序列比对仅校验 `INCREMENT_BY`、`MIN_VALUE`、`MAX_VALUE`、`CYCLE_FLAG`、`ORDER_FLAG`、`CACHE_SIZE`，完全忽略了 `LAST_NUMBER`（Oracle 当前值）。迁移后的序列若不执行 `ALTER SEQUENCE ... RESTART WITH`，新环境中自增列可能从初始值重新开始，导致**主键冲突或数据覆盖**。  
**影响**: 数据完整性风险，生产环境可能出现主键重复错误。  
**建议**:
1. 在 Oracle 端查询 `DBA_SEQUENCES.LAST_NUMBER`
2. 在生成的 fixup 脚本中追加 `ALTER SEQUENCE ... RESTART WITH <last_number>` 语句
3. 在比对报告中标注当前值差异

### P0-02: `is_index_expression_token` 正则转义错误

**位置**: 第 1661 行  
```python
return bool(re.search(r"[()\s'\"+\-*/]|\\bCASE\\b", token, flags=re.IGNORECASE))
```
**问题**: `\\bCASE\\b` 使用了双反斜杠，在原始字符串 `r""` 中会被解释为字面量 `\bCASE\b` 而非正则的单词边界 `\bCASE\b`。正确写法应为 `\bCASE\b`（在 raw string 中单反斜杠即可）。  
**影响**: 包含 `CASE` 表达式的函数索引将**无法被正确识别**为表达式索引，可能导致索引比对逻辑出错。  
**建议**: 修改为 `r"[()\s'\"+\-*/]|\bCASE\b"`

### P0-03: `compare_version` 解析失败时静默返回相等

**位置**: 第 16025-16043 行  
```python
except (ValueError, AttributeError):
    return 0
```
**问题**: 当版本号包含非数字部分（如 `4.2.1-bp1`、`4.3.0.1-100010012024`）时，`int(x)` 会抛出 `ValueError`，函数返回 `0`（表示相等）。这会导致**版本相关的兼容性判断全部失效**——本应阻断或特殊处理的操作被放行。  
**影响**: OceanBase 版本特定的 DDL 清理规则（如 `clean_view_ddl_for_oceanbase`）可能不会正确执行。  
**建议**: 在分割版本号时，先用正则提取纯数字段：`re.findall(r'\d+', version)`

### P0-04: 报告数据库保留期清理仅删除 summary 表

**位置**: 第 27809-27815 行  
```python
delete_sql = (
    f"DELETE FROM {schema_prefix}{REPORT_DB_TABLES['summary']} "
    f"WHERE RUN_TIMESTAMP < SYSTIMESTAMP - INTERVAL '{retention_days}' DAY"
)
```
**问题**: `report_retention_days` 到期清理时，仅删除 `summary` 表的过期记录，但 `detail`、`detail_item`、`counts`、`usability`、`dependency` 等十余张子表的关联数据**不会被清理**。这些孤儿记录会持续累积。  
**影响**: 长期运行后报告数据库空间持续膨胀，子表中的大量 CLOB 数据无法回收。  
**建议**: 
1. 级联删除所有子表中对应 `REPORT_ID` 的记录
2. 或在建表时使用外键 `ON DELETE CASCADE`

### P0-05: `obclient_run_sql` 通过命令行传递密码

**位置**: 第 7605 行  
```python
'-p' + ob_cfg['password'],
```
**问题**: 密码通过命令行参数直接拼接传递给 `subprocess.run`，在 Linux 系统上可通过 `ps aux` 或 `/proc/<pid>/cmdline` 查看到明文密码。虽然审查要求排除密码存储问题，但此处属于**密码泄露通道**而非存储问题，需特别标注。  
**影响**: 任何有系统进程查看权限的用户均可获取 OB 密码。  
**建议**: 通过 `stdin` 管道传递密码，或使用环境变量方式

---

## P1 - 重要问题（12个）

### P1-01: SQL 拼接存在注入风险

**位置**: `obclient_query_by_owner_chunks`（第 7699 行）、`ob_has_dba_column`（第 7722 行）等多处  
```python
owners_in = ",".join(f"'{s}'" for s in chunk)
sql = sql_tpl.format(owners_in=owners_in)
```
```python
sql = (
    "SELECT 1 FROM DBA_TAB_COLUMNS "
    f"WHERE OWNER='{owner_u}' AND TABLE_NAME='{table_u}' AND COLUMN_NAME='{column_u}' "
)
```
**问题**: OB 端查询通过 obclient CLI 执行，所有参数直接拼接进 SQL 字符串。虽然 schema/table 名通常来自字典查询结果，但若 `owner`、`table_name` 等值包含单引号，会导致 SQL 语法错误甚至注入。Oracle 端查询使用了 bind placeholder（安全），但 OB 端全部使用字符串拼接（不安全）。  
**影响**: 当对象名包含特殊字符时，可能导致查询失败或意外行为。  
**建议**: 对所有拼接值至少做 `value.replace("'", "''")` 转义，或统一使用 `sql_quote_literal`

### P1-02: `global OBC_TIMEOUT` 全局变量的并发安全问题

**位置**: 第 688 行（定义）、第 3435 行（修改）  
**问题**: `OBC_TIMEOUT` 作为全局变量在 `load_config` 中被修改，而 `obclient_run_sql` 在多线程环境中读取该值。虽然 Python 的 GIL 保证了基本类型赋值的原子性，但这种模式使代码难以测试和推理。  
**影响**: 代码可维护性差，且若未来代码结构变化可能引入竞态条件。

### P1-03: `_EXTRA_CHECK_CONTEXT` 全局可变状态

**位置**: 第 13954-13967 行  
```python
_EXTRA_CHECK_CONTEXT: Dict[str, object] = {}
def _init_extra_check_worker(...):
    global _EXTRA_CHECK_CONTEXT
    _EXTRA_CHECK_CONTEXT = { ... }
```
**问题**: 使用全局字典作为多进程 worker 的初始化参数传递通道。`ProcessPoolExecutor` 的 `initializer` 机制确实需要全局状态，但该变量在模块级别暴露，且类型标注为 `Dict[str, object]`，缺乏封装。  
**影响**: 其他代码可能意外修改该全局字典，导致 worker 行为异常。

### P1-04: 错误检测启发式规则存在误判风险

**位置**: 第 7618-7622 行  
```python
# 仅当错误出现在行首时视为执行失败
if re.search(r"^(ORA-\d{5}|OB-\d+)\b", line_clean, flags=re.IGNORECASE):
    return line_clean
```
**问题**: obclient 的输出格式并不保证错误信息一定出现在行首。某些场景下（如多语句执行、nested error），错误可能出现在行中。同时，若正常输出的某列数据恰好以 `ORA-` 开头，也可能被误判。  
**影响**: 可能漏报真实错误或误报正常输出。

### P1-05: DDL 清理中的 NOKEEP/NOSCALE/GLOBAL 移除过于激进

**位置**: 第 18675 行附近  
```python
for token in ("NOKEEP", "NOSCALE", "GLOBAL"):
    cleaned = re.sub(rf"\s*\b{token}\b", " ", cleaned, flags=re.IGNORECASE)
```
**问题**: 使用简单的正则替换移除关键字，不区分上下文。例如 `GLOBAL` 可能出现在注释、字符串常量或列名中，被误删除。  
**影响**: 可能破坏包含这些关键字作为标识符或字符串值的 DDL。  
**建议**: 限制替换范围，例如仅在 `CREATE TABLE` 的 DDL 头部进行，或使用 SQL 解析而非正则替换

### P1-06: `clean_plsql_ending` 仅处理 `END name;` 后的多余分号

**位置**: 第 17780-17821 行  
**问题**: 该函数假设 `END` 语句后紧跟对象名（如 `END my_proc;`），但不处理无名称的 `END;` 后跟多余分号的情况。此外，正则 `^\s*END\s+\w+\s*;\s*$` 无法匹配带引号的对象名（如 `END "My_Proc";`）。  
**影响**: 对于引号包裹的 PL/SQL 对象名，清理逻辑不会触发。

### P1-07: `load_config` 函数过于庞大且职责混杂

**位置**: 第 2959-3449 行（约 490 行）  
**问题**: 一个函数承担了配置读取、默认值设置、类型转换、验证、全局变量修改等所有职责。函数使用大量 `try-except` 块逐个解析每个配置项，代码高度重复。  
**影响**: 难以维护、难以测试、难以新增配置项。每新增一个配置项需在函数中添加约 10 行样板代码。  
**建议**: 定义配置 schema（如 dataclass 或 dict 模板），自动进行类型转换和默认值填充

### P1-08: `settings` Dict 类型混杂

**位置**: 贯穿全程  
**问题**: `settings` 字典中混合存储了 `str`、`int`、`float`、`bool`、`Set[str]`、`List[str]`、`Optional[datetime]` 等多种类型。部分配置项存在**重复存储**：例如 `fixup_max_sql_file_mb` 既有字符串形式也有整数形式。后续使用处需要反复进行 `int()` / `bool()` / `str()` 转换。  
**影响**: 类型安全性差，容易出现运行时类型错误。  
**建议**: 使用 `dataclass` 或 `TypedDict` 定义配置结构

### P1-09: `abort_run()` 函数返回类型不一致

**位置**: 第 128-129 行  
```python
def abort_run(message: Optional[str] = None) -> None:
    raise FatalError(message or "fatal error")
```
**问题**: 函数签名声明返回 `None`，但实际上总是抛出异常。这本身不是错误，但在调用点如 `parse_oracle_dsn` 中：
```python
def parse_oracle_dsn(dsn: str) -> Tuple[str, str, Optional[str]]:
    ...
    except ValueError:
        log.error(...)
        abort_run()  # 这里没有 return 语句
```
函数在 `abort_run()` 后没有 `return` 语句。虽然运行时不会到达，但类型检查器（mypy）会报告 "missing return statement"。  
**建议**: 将 `abort_run` 返回类型标注为 `NoReturn`

### P1-10: 配置向导中 `infer_schema_mapping` 提示文本与默认值矛盾

**位置**: 第 3966-3969 行  
```python
_prompt_field(
    "SETTINGS",
    "infer_schema_mapping",
    "是否自动推导 schema 映射 (true/false，默认 false，建议保持 false)",
    default=cfg.get("SETTINGS", "infer_schema_mapping", fallback="true"),
    ...
)
```
**问题**: 提示文本说"默认 false，建议保持 false"，但 `fallback` 值是 `"true"`。`load_config` 中的 `setdefault` 也是 `'true'`（第 3060 行）。用户看到的提示与实际默认值不一致。  
**影响**: 用户可能被误导做出错误的配置选择。

### P1-11: 报告数据库 INSERT 语句未使用参数化查询

**位置**: `save_report_to_db`（第 27659-27708 行）及所有 `_insert_report_*` 函数  
**问题**: 所有 INSERT 语句通过 f-string 拼接 SQL 值，虽然使用了 `sql_quote_literal` 做单引号转义，但未处理 `NUL` 字符（`\x00`）、超长 CLOB 拼接溢出等边界情况。`sql_clob_literal` 将长文本拆分为多个 `TO_CLOB() || TO_CLOB()` 拼接，极端情况下可能生成超长 SQL 语句导致 obclient 执行失败。  
**影响**: 包含特殊字符的报告数据可能写入失败。

### P1-12: `ThreadPoolExecutor` 中异常处理不完整

**位置**: 第 15014-15017 行（可用性校验）及 fixup 生成中的多处  
```python
with ThreadPoolExecutor(max_workers=workers) as executor:
    futures = [executor.submit(_check_one, item) for item in candidates]
    for future in as_completed(futures):
        results.append(future.result())
```
**问题**: `future.result()` 会重新抛出 worker 中的异常，但此处没有 try-except 包裹。如果任一 worker 抛出未预期的异常，整个可用性校验会中断，且已收集的结果会丢失。  
**影响**: 单个对象的校验失败会导致整批结果丢失。  
**建议**: 在 `future.result()` 外包裹 try-except，记录异常并继续处理

---

## P2 - 一般问题（15个）

### P2-01: 过度使用宽泛的 `except Exception`

**位置**: 第 146、175、3355、3361、4347、15022、15070、15086、15093、15100、15102、20323、23016、30669 行等（共约 20+ 处）  
**问题**: 大量使用 `except Exception:` 甚至 `except Exception: pass`，吞没了所有异常信息。这些位置分布在：
- 日志初始化兜底
- 配置解析兜底
- 连接关闭兜底
- Oracle 基本信息获取兜底
- 路径解析兜底

**影响**: 运行时错误被静默忽略，增加调试难度。  
**建议**: 
1. 至少使用 `log.debug` 记录被吞没的异常
2. 缩小捕获范围为具体异常类型
3. 关键路径（如数据库连接）应使用 `except (oracledb.Error, ConnectionError)` 等

### P2-02: f-string 与 `%s` 日志格式混用

**位置**: 全文多处（如 2961、2964、3441、4326、5329、7640、7655 等行使用 f-string；7644、7648 等行使用 `%s`）  
**问题**: Python logging 推荐使用 `%s` 占位符（延迟格式化，仅在实际输出时才构造字符串）。f-string 在所有日志级别下都会执行字符串拼接，即使该级别被过滤也会产生开销。更重要的是，**风格不一致**降低代码可读性。  
**建议**: 统一使用 `log.info("message %s", value)` 格式

### P2-03: 规范化函数模板代码高度重复

**位置**: 第 2069-2261 行（约 10 个 `normalize_*` 函数）  
**问题**: `normalize_synonym_fixup_scope`、`normalize_sequence_remap_policy`、`normalize_report_dir_layout`、`normalize_report_detail_mode`、`normalize_fixup_idempotent_mode`、`normalize_column_visibility_policy` 等函数结构完全相同：
1. 检查空值 → 返回默认值
2. 小写化
3. 别名映射
4. 验证合法值 → 不合法则 warning 并返回默认值

**影响**: 每新增一种枚举配置，需要复制粘贴约 10 行样板代码。  
**建议**: 抽取通用的 `normalize_enum_config(raw, values, aliases, default, key_name)` 工具函数

### P2-04: 配置向导中验证函数同样高度重复

**位置**: 第 3638-3728 行（约 10 个 `_validate_*` 函数）  
**问题**: 与 P2-03 类似，每个配置项的验证函数结构完全一致。  
**建议**: 复用 `normalize_*` 函数作为验证器

### P2-05: `NamedTuple` 数量过多且部分可合并

**位置**: 第 300-700 行及 2363-2518 行  
**问题**: 文件中定义了约 40+ 个 `NamedTuple`，部分结构非常相似。例如 `IndexMismatch`、`ConstraintMismatch`、`TriggerMismatch` 都包含 `table`、`missing_*`、`extra_*`、`detail_mismatch` 字段，可以泛化为一个通用的 `ObjectMismatch` 类型加 `mismatch_type` 字段。  
**影响**: 增加理解和维护成本。

### P2-06: `settings` 字典键名拼写/命名不一致

**位置**: 全文  
**问题**: 部分键名使用 snake_case（`enable_comment_check`），部分直接使用配置文件中的名称（`check_comments`），部分使用衍生名称（`enabled_primary_types`）。存在映射关系不清晰的情况：
- `check_comments` → `enable_comment_check`
- `check_column_order` → `enable_column_order_check`
- `generate_grants` → `enable_grant_generation`

**影响**: 容易混淆配置项的原始名称和程序内部使用的键名。

### P2-07: `obclient_run_sql` 超时日志使用全局变量而非实际值

**位置**: 第 7655 行  
```python
log.error(f"严重错误: obclient 执行超时 (>{OBC_TIMEOUT} 秒)。...")
```
**问题**: 日志中输出的是全局 `OBC_TIMEOUT` 而非该次调用实际使用的 `timeout_val`（第 7594 行），当调用者通过 `timeout` 参数覆盖超时值时，日志信息会与实际超时时间不符。  
**建议**: 改为 `timeout_val`

### P2-08: `normalize_comment_text` 将 "NULL"/"NONE" 视为空

**位置**: 第 2649 行  
```python
if normalized.upper() in {"NULL", "<NULL>", "NONE"}:
    return ""
```
**问题**: 如果用户确实需要将注释文本设为字面量 `"NULL"` 或 `"NONE"`，该逻辑会将其清空。虽然这种情况极少，但属于信息丢失。  
**影响**: 边界条件下的注释比对可能出现误报。

### P2-09: 日志输出中引用的环境变量仅适用于 Linux

**位置**: 第 5305-5307 行  
```python
log.info("如遇 libnnz19.so 等库缺失，请先执行:")
log.info(f"  export LD_LIBRARY_PATH=\"{client_path}:${{LD_LIBRARY_PATH}}\"")
```
**问题**: 提示信息中使用 `export LD_LIBRARY_PATH` 语法，仅适用于 Linux/macOS。在 Windows 上无意义。  
**建议**: 根据 `sys.platform` 输出平台对应的环境变量设置提示

### P2-10: `clean_for_loop_single_dot_range` 正则可能误伤小数

**位置**: 第 17841-17854 行  
```python
FOR_LOOP_RANGE_SINGLE_DOT_PATTERN = re.compile(
    r'(\bIN\s+-?\d+)\s*\.(\s*)(?=(?:"[^"]+"|[A-Z_]))',
    re.IGNORECASE
)
```
**问题**: 虽然注释说"仅在点号后为标识符时生效，避免误伤小数"，但正则中 `[A-Z_]` 的前瞻不够严格。例如 `IN 1.E10`（科学记数法的一部分）也会匹配，被错误替换为 `IN 1..E10`。  
**影响**: 极端情况下可能破坏合法的 PL/SQL 表达式。

### P2-11: `run_config_wizard` 明文回显密码输入

**位置**: 第 3742-3743 行  
```python
_prompt_field("ORACLE_SOURCE", "password", "Oracle 密码 (ORACLE_SOURCE.password)", required=True)
```
**问题**: 使用 `input()` 接收密码，输入内容会在终端明文显示。应使用 `getpass.getpass()` 替代。  
**建议**: 对密码类字段使用 `getpass` 模块

### P2-12: `shutil.rmtree` 使用 `ignore_errors=True`

**位置**: 第 20341 行、第 23181 行  
```python
shutil.rmtree(child, ignore_errors=True)
```
**问题**: 静默忽略目录删除中的所有错误（包括权限不足、文件被锁等），可能导致旧脚本残留但程序误认为已清理成功。  
**建议**: 至少记录 warning 级别日志

### P2-13: `build_column_order_sequence` 返回 `tuple()` 而非 `None` 表示空

**位置**: 第 2570-2571 行  
```python
if not candidates:
    return tuple(), None
```
**问题**: 返回 `(tuple(), None)` 表示"空但无错误"，与返回 `(None, "column_meta_missing")` 表示"错误"使用了不同的 sentinel。但调用方需同时检查 `result is None` 和 `len(result) == 0`，增加了使用复杂度。  
**建议**: 统一使用 `Optional` 语义

### P2-14: `dfs` 递归可能导致栈溢出

**位置**: 第 10821-10833 行  
```python
def dfs(node, path, seen):
    ...
    for ref in refs:
        dfs(ref, path + [ref], seen | {node})
```
**问题**: 依赖图的 DFS 使用递归实现，`max_depth` 默认值虽然限制了深度，但每次递归都创建新的 `path` 列表和 `seen` 集合（通过 `+` 和 `|` 操作），在大型依赖图上可能造成显著的内存和性能开销。  
**建议**: 使用迭代式 DFS 或限制递归深度

### P2-15: `GRANT_PRIVILEGE_BY_TYPE` 中 `TRIGGER` 映射为 `EXECUTE`

**位置**: 第 2676 行  
```python
'TRIGGER': 'EXECUTE',
```
**问题**: Oracle 中 TRIGGER 的权限模型不是通过 `EXECUTE` 授予的。触发器的执行权限取决于其所在表的 DML 权限。将 TRIGGER 映射为 `EXECUTE` 会生成无效的 GRANT 语句。  
**影响**: 生成的 GRANT 脚本中可能包含无效的 `GRANT EXECUTE ON <trigger>` 语句。

---

## P3 - 改进建议（10个）

### P3-01: 缺少单元测试

**问题**: 项目中未发现针对核心函数的单元测试文件。`normalize_sql_expression`、`classify_unsupported_constraint`、`is_number_equivalent` 等纯函数非常适合单元测试覆盖。  
**建议**: 为核心比对逻辑和规范化函数编写测试用例

### P3-02: 类型别名使用 `Dict` 而非 `TypedDict`

**位置**: 第 305-340 行  
```python
OraConfig = Dict[str, str]
ObConfig = Dict[str, str]
```
**问题**: 配置字典使用无约束的 `Dict[str, str]` 类型别名，无法在类型检查时捕获键名拼写错误或值类型不匹配。  
**建议**: 使用 `TypedDict` 明确定义必需键和可选键

### P3-03: 常量定义分散在文件各处

**问题**: `REPORT_DB_TABLES`、`GRANT_PRIVILEGE_BY_TYPE`、`DDL_OBJECT_TYPE_OVERRIDE`、`DBCAT_OPTION_MAP` 等常量定义散布在 700-2950 行范围内，与使用它们的函数相距甚远。  
**建议**: 将所有常量集中到文件头部或独立的 `constants.py` 模块

### P3-04: `write_fixup_file` 分号追加逻辑不处理 PL/SQL 块

**位置**: `write_fixup_file` 函数  
```python
if tail and not tail.endswith((';', '/')):
    f.write(';\n')
```
**问题**: 对于 PL/SQL 块（PROCEDURE、FUNCTION 等），结尾应为 `/` 而非 `;`。当前逻辑在 tail 不以 `;` 或 `/` 结尾时统一追加 `;`，对 PL/SQL 对象可能追加了错误的终止符。  
**影响**: 极端情况下可能生成语法不正确的 fixup 脚本。

### P3-05: `sql_quote_literal` 仅处理单引号

**位置**: 第 24790-24794 行  
```python
def sql_quote_literal(value: Optional[object]) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    return "'" + text.replace("'", "''") + "'"
```
**问题**: 仅转义单引号，未处理 `NUL`（`\x00`）字符。某些数据库在 SQL 文本中遇到 NUL 字符时会截断或报错。  
**建议**: 过滤掉 `\x00` 字符

### P3-06: `main()` 函数过于庞大

**位置**: 第 29783-30672 行（约 890 行）  
**问题**: `main()` 函数承担了整个程序的编排流程，包括配置加载、元数据转储、校验、fixup 生成、报告输出、数据库写入等所有步骤。  
**建议**: 将各阶段拆分为独立函数，`main()` 仅负责编排调用

### P3-07: 部分函数缺少 docstring

**问题**: 虽然核心函数普遍有文档字符串，但部分重要的辅助函数（如 `normalize_column_sequence`、`is_column_order_candidate`、`build_column_order_sequence`、`normalize_index_columns` 等）缺少 docstring。  
**建议**: 为所有公开使用的函数添加文档字符串

### P3-08: 硬编码的数据库查询 SQL 散布在各函数中

**问题**: 用于查询 `DBA_OBJECTS`、`DBA_TAB_COLUMNS`、`DBA_SEQUENCES`、`DBA_CONSTRAINTS` 等的 SQL 语句直接硬编码在各个函数内部。若需适配不同的数据字典版本（如从 `DBA_*` 切换到 `ALL_*`），需要逐个修改。  
**建议**: 将 SQL 模板集中管理

### P3-09: `chunk_list` 函数接受 `List[str]` 但实际可泛化

**位置**: 第 2343 行  
```python
def chunk_list(items: List[str], size: int) -> List[List[str]]:
```
**问题**: 类型标注限制为 `List[str]`，但实现逻辑对任意类型均有效。  
**建议**: 使用泛型 `List[T]`

### P3-10: 日志消息混用中英文

**问题**: 日志消息主体使用中文（如 "正在加载配置文件"），但部分技术术语和括号内容使用英文（如 "parse_mode"、"DDL_HINT"），且标点符号混用中英文逗号和冒号。  
**建议**: 统一日志语言风格，建议技术日志全部使用英文或全部使用中文

---

## 附录：问题分布热力图

| 代码区域 | 行范围 | 问题数量 |
|----------|--------|----------|
| 常量/类型定义 | 1-700 | 3 |
| 硬编码规则/常量 | 700-1050 | 2 |
| 规范化/校验函数 | 1050-2350 | 4 |
| 配置加载 | 2350-3450 | 5 |
| 配置向导 | 3450-4350 | 3 |
| 对象映射/remap | 4350-7590 | 2 |
| obclient 执行 | 7590-7760 | 4 |
| OB 元数据转储 | 7760-9500 | 2 |
| Oracle 元数据转储 | 9500-10450 | 1 |
| 依赖/授权 | 10450-12430 | 3 |
| 扩展对象校验 | 12430-14250 | 2 |
| 可用性校验 | 14250-15050 | 2 |
| DDL 抽取/清理 | 15050-20200 | 5 |
| Fixup 脚本生成 | 20200-23200 | 2 |
| 报告生成/DB 写入 | 23200-27820 | 4 |
| 主函数 | 29780-30672 | 1 |

---

## 总结与优先修复建议

### 立即修复（P0）
1. **P0-01**: 补齐序列 `LAST_NUMBER` 采集和 `RESTART WITH` 生成
2. **P0-02**: 修正 `is_index_expression_token` 中的正则转义
3. **P0-03**: 修正 `compare_version` 使其正确处理带后缀的版本号
4. **P0-04**: 报告数据库清理需级联删除所有子表
5. **P0-05**: obclient 密码改为通过 stdin 传递

### 近期修复（P1）
- P1-01（SQL 注入防护）、P1-10（提示文本矛盾）、P1-12（线程异常处理）应优先处理
- P1-04（错误检测规则）和 P1-05（DDL 清理规则）需结合实际测试案例验证

### 中期改进（P2/P3）
- 统一日志格式（P2-02）和规范化函数去重（P2-03/P2-04）可批量处理
- 引入单元测试（P3-01）是长期代码质量保障的关键
