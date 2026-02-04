# `run_fixup.py` 代码审查报告

**审查日期**: 2026-02-04  
**审查范围**: `run_fixup.py` 全量代码 (3888 行)  
**关联文件**: `schema_diff_reconciler.py` (fixup 脚本生成逻辑)  

---

## 一、文件概述

`run_fixup.py` 是 OceanBase 迁移校验工具的核心组件，负责执行由 `schema_diff_reconciler.py` 生成的 DDL 修补脚本。

### 主要功能模块

| 函数/模块 | 行号范围 | 功能说明 |
|-----------|----------|----------|
| `run_single_fixup()` | 2979-3254 | 单轮执行模式 |
| `run_iterative_fixup()` | 3524-3883 | 迭代执行模式（自动重试） |
| `run_view_chain_autofix()` | 3257-3521 | VIEW 链路自动修复 |
| `execute_sql_statements()` | 2002-2032 | SQL 语句执行 |
| `split_sql_statements()` | 1889-1987 | SQL 语句分割解析 |
| `execute_auto_grant_for_object()` | 1320-1348 | 自动补权限 |
| `recompile_invalid_objects()` | 2695-2756 | 重编译 INVALID 对象 |

---

## 二、问题清单

### 问题分类说明

- 🔴 **P0 - 严重**: 可能导致数据丢失、安全漏洞或程序崩溃
- 🟠 **P1 - 重要**: 影响功能正确性或健壮性
- 🟡 **P2 - 中等**: 潜在风险或代码质量问题
- 🟢 **P3 - 建议**: 改进建议，非必须修复

---

### 🔴 P0-00: 序列迁移缺少 LAST_NUMBER 同步（RESTART WITH）

**位置**: `schema_diff_reconciler.py:10133-10138`, `schema_diff_reconciler.py:8522-8527`

**问题描述**:  
序列元数据查询未包含 `LAST_NUMBER` 字段，导致迁移后的序列从 `START WITH` 值开始，而非源端当前值。

**现有代码** (Oracle 端查询):
```python
sql_seq_tpl = """
    SELECT SEQUENCE_OWNER, SEQUENCE_NAME,
           INCREMENT_BY, MIN_VALUE, MAX_VALUE, CYCLE_FLAG, ORDER_FLAG, CACHE_SIZE
    FROM DBA_SEQUENCES
    WHERE SEQUENCE_OWNER IN ({owners_clause})
"""
```

**缺失字段**: `LAST_NUMBER`（序列的当前值/下一个可用值）

**风险场景**:

| 阶段 | 源端序列 | 迁移后序列 | 结果 |
|------|----------|------------|------|
| 迁移前 | LAST_NUMBER = 50000 | - | - |
| 迁移后 | - | START WITH = 1 | 序列从 1 开始 |
| 数据写入 | - | NEXTVAL = 1, 2, 3... | **主键冲突！** |

**影响范围**:
1. **`schema_diff_reconciler.py`** - 序列元数据采集缺失 LAST_NUMBER
2. **fixup 脚本** - 只生成 CREATE SEQUENCE，无后续 RESTART WITH
3. **`run_fixup.py`** - 无法感知序列值同步需求

**修复建议**:

1. **修改元数据查询** (schema_diff_reconciler.py):
```python
sql_seq_tpl = """
    SELECT SEQUENCE_OWNER, SEQUENCE_NAME,
           INCREMENT_BY, MIN_VALUE, MAX_VALUE, CYCLE_FLAG, ORDER_FLAG, CACHE_SIZE,
           LAST_NUMBER
    FROM DBA_SEQUENCES
    WHERE SEQUENCE_OWNER IN ({owners_clause})
"""
```

2. **生成 RESTART WITH 脚本**:
```python
# 在 CREATE SEQUENCE 之后，生成同步脚本
# fixup_scripts/sequence_restart/SCHEMA.SEQ_NAME.sql
restart_ddl = f"ALTER SEQUENCE {tgt_schema}.{tgt_seq} RESTART WITH {last_number};"
```

3. **执行顺序调整** (run_fixup.py):
```
sequence (CREATE) → 数据迁移 → sequence_restart (ALTER RESTART WITH)
```

**注意事项**:
- `LAST_NUMBER` 需要在数据迁移完成后再同步，否则可能不准确
- 对于 CACHE 序列，`LAST_NUMBER` 可能有跳跃，需考虑安全边际
- 建议采用 `LAST_NUMBER + (CACHE_SIZE * INCREMENT_BY)` 作为安全值

---

### 🔴 P0-01: subprocess 异常捕获不完整

**位置**: `run_fixup.py:1990-1999`

**问题描述**:  
`run_sql()` 函数只捕获了 `TimeoutExpired` 异常，未处理其他可能的异常情况。

**现有代码**:
```python
def run_sql(obclient_cmd: List[str], sql_text: str, timeout: Optional[int]) -> subprocess.CompletedProcess:
    """Execute SQL text by piping it to obclient."""
    return subprocess.run(
        obclient_cmd,
        input=sql_text,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )
```

**风险**:
- `FileNotFoundError`: obclient 可执行文件不存在时程序崩溃
- `PermissionError`: 无执行权限时程序崩溃
- `OSError`: 其他系统错误未处理

**修复建议**:
```python
def run_sql(obclient_cmd: List[str], sql_text: str, timeout: Optional[int]) -> subprocess.CompletedProcess:
    """Execute SQL text by piping it to obclient."""
    try:
        return subprocess.run(
            obclient_cmd,
            input=sql_text,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except FileNotFoundError:
        raise ConfigError(f"obclient 可执行文件未找到: {obclient_cmd[0]}")
    except PermissionError:
        raise ConfigError(f"无权限执行 obclient: {obclient_cmd[0]}")
    except OSError as e:
        raise ConfigError(f"执行 obclient 失败: {e}")
```

---

### 🔴 P0-02: 密码明文暴露在命令行

**位置**: `run_fixup.py:647-657`

**问题描述**:  
数据库密码通过命令行参数传递，可被系统进程列表（如 `ps aux`）看到。

**现有代码**:
```python
def build_obclient_command(ob_cfg: Dict[str, str]) -> List[str]:
    return [
        ob_cfg["executable"],
        "-h", ob_cfg["host"],
        "-P", ob_cfg["port"],
        "-u", ob_cfg["user_string"],
        f"-p{ob_cfg['password']}",  # 密码明文暴露
        "--prompt", "fixup>",
        "--silent",
    ]
```

**风险**:
- 多用户系统中密码可被其他用户看到
- 日志记录可能包含敏感信息

**修复建议**:
```python
def build_obclient_command(ob_cfg: Dict[str, str]) -> Tuple[List[str], Dict[str, str]]:
    """返回命令行和环境变量，密码通过环境变量传递"""
    cmd = [
        ob_cfg["executable"],
        "-h", ob_cfg["host"],
        "-P", ob_cfg["port"],
        "-u", ob_cfg["user_string"],
        "--prompt", "fixup>",
        "--silent",
    ]
    env = os.environ.copy()
    env["MYSQL_PWD"] = ob_cfg["password"]  # obclient 支持此环境变量
    return cmd, env

# 调用处修改
def run_sql(obclient_cmd: List[str], sql_text: str, timeout: Optional[int], env: Optional[Dict] = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        obclient_cmd,
        input=sql_text,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
        env=env,
    )
```

---

### 🔴 P0-03: 文件移动操作存在数据覆盖风险

**位置**: `run_fixup.py:3098-3105`, `run_fixup.py:3134-3141`, `run_fixup.py:2625-2632`

**问题描述**:  
成功执行的脚本移动到 `done/` 目录时，如果目标文件已存在会被直接覆盖。

**现有代码**:
```python
try:
    target_dir = done_dir / sql_path.parent.name
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / sql_path.name
    shutil.move(str(sql_path), target_path)  # 直接覆盖
    move_note = f"(已移至 done/{sql_path.parent.name}/)"
except Exception as exc:
    move_note = f"(移动失败: {exc})"
```

**风险**:
- 重复执行时可能覆盖之前的执行记录
- 无法追溯历史执行情况

**修复建议**:
```python
def safe_move_to_done(sql_path: Path, done_dir: Path) -> str:
    """安全移动文件到 done 目录，避免覆盖"""
    try:
        target_dir = done_dir / sql_path.parent.name
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / sql_path.name
        
        if target_path.exists():
            # 已存在则添加时间戳后缀
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{sql_path.stem}.{timestamp}{sql_path.suffix}"
            backup_path = target_dir / backup_name
            shutil.move(str(target_path), backup_path)
            log.info("已存在的文件已备份: %s", backup_path.name)
        
        shutil.move(str(sql_path), target_path)
        return f"(已移至 done/{sql_path.parent.name}/)"
    except Exception as exc:
        log.error("移动文件失败: %s -> %s: %s", sql_path, target_path, exc)
        return f"(移动失败: {exc})"
```

---

### 🟠 P1-01: 迭代模式累计失败计数逻辑错误

**位置**: `run_fixup.py:3785-3793`

**问题描述**:  
在迭代执行模式中，同一脚本在多轮失败会被重复计入 `cumulative_failed`。

**现有代码**:
```python
round_success = sum(1 for r in round_results if r.status == "SUCCESS")
round_failed = sum(1 for r in round_results if r.status in ("FAILED", "ERROR"))
round_skipped = sum(1 for r in round_results if r.status == "SKIPPED")

cumulative_success += round_success
cumulative_failed += round_failed  # 问题：重复计数
```

**示例**:
- 脚本 A 第1轮失败 → cumulative_failed = 1
- 脚本 A 第2轮再次失败 → cumulative_failed = 2 (实际应为 1)

**修复建议**:
```python
# 在函数开始处初始化
all_failed_scripts: Set[Path] = set()

# 每轮结束时
for r in round_results:
    if r.status in ("FAILED", "ERROR"):
        all_failed_scripts.add(r.path)

# 最终汇总时
cumulative_failed = len(all_failed_scripts)
```

---

### 🟠 P1-02: 端口号验证不严格

**位置**: `run_fixup.py:602`

**问题描述**:  
只做了类型转换，未验证端口范围有效性。

**现有代码**:
```python
ob_cfg["port"] = str(int(ob_cfg["port"]))
```

**风险**:
- 端口号超出有效范围 (1-65535) 时会导致连接失败
- 负数或零值端口未被拦截

**修复建议**:
```python
try:
    port = int(ob_cfg["port"])
    if not (1 <= port <= 65535):
        raise ConfigError(f"端口号超出有效范围 (1-65535): {port}")
    ob_cfg["port"] = str(port)
except ValueError:
    raise ConfigError(f"端口号格式无效: {ob_cfg['port']}")
```

---

### 🟠 P1-03: 路径遍历风险

**位置**: `run_fixup.py:618-622`

**问题描述**:  
`fixup_dir` 配置项未做路径安全检查，可能导致路径遍历。

**现有代码**:
```python
fixup_dir = parser.get("SETTINGS", "fixup_dir", fallback=DEFAULT_FIXUP_DIR).strip()
fixup_path = (repo_root / fixup_dir).resolve()

if not fixup_path.exists():
    raise ConfigError(f"修补脚本目录不存在: {fixup_path}")
```

**风险**:
- 配置 `fixup_dir = ../../sensitive_dir` 可访问预期之外的目录

**修复建议**:
```python
fixup_dir = parser.get("SETTINGS", "fixup_dir", fallback=DEFAULT_FIXUP_DIR).strip()
fixup_path = (repo_root / fixup_dir).resolve()

# 安全检查：确保在 repo_root 下
try:
    fixup_path.relative_to(repo_root)
except ValueError:
    raise ConfigError(f"fixup_dir 路径越界，必须位于配置文件所在目录内: {fixup_path}")

if not fixup_path.exists():
    raise ConfigError(f"修补脚本目录不存在: {fixup_path}")
```

---

### 🟠 P1-04: grant 文件重写的原子性问题

**位置**: `run_fixup.py:2576-2591`

**问题描述**:  
重写 grant 文件时使用临时文件，但在 `replace()` 之前如果程序崩溃，原文件可能已被损坏。

**现有代码**:
```python
rewritten = "\n\n".join(stmt.strip() for stmt in kept_statements if stmt.strip()).rstrip()
try:
    tmp_path = sql_path.with_suffix(sql_path.suffix + ".tmp")
    tmp_path.write_text(rewritten + "\n", encoding="utf-8")
    tmp_path.replace(sql_path)
```

**修复建议**:
```python
rewritten = "\n\n".join(stmt.strip() for stmt in kept_statements if stmt.strip()).rstrip()
try:
    # 先备份原文件
    backup_path = sql_path.with_suffix(sql_path.suffix + ".bak")
    shutil.copy2(str(sql_path), backup_path)
    
    # 写入临时文件
    tmp_path = sql_path.with_suffix(sql_path.suffix + ".tmp")
    tmp_path.write_text(rewritten + "\n", encoding="utf-8")
    
    # 原子替换
    tmp_path.replace(sql_path)
    
    # 成功后删除备份
    backup_path.unlink(missing_ok=True)
except Exception as exc:
    # 恢复备份
    if backup_path.exists():
        shutil.copy2(str(backup_path), sql_path)
    log.error("重写文件失败，已恢复原文件: %s", exc)
```

---

### 🟠 P1-05: 自动补权限跳过时缺少明确提示

**位置**: `run_fixup.py:1218-1230`

**问题描述**:  
当 `dependency_chains` 或 `VIEWs_chain` 报告不存在时，自动补权限功能静默跳过。

**现有代码**:
```python
def init_auto_grant_context(...) -> Optional[AutoGrantContext]:
    if not fixup_settings.enabled:
        return None
    dep_file = find_latest_report_file(report_dir, "dependency_chains")
    dep_map = parse_dependency_chains_file(dep_file) if dep_file else {}
    view_chain_file = find_latest_view_chain_file(report_dir)
    # ...
    if not dep_map:
        log.warning("[AUTO-GRANT] 未找到 dependency_chains/VIEWs_chain，自动补权限跳过。")
        return None
```

**修复建议**:
```python
if not dep_map:
    log.warning(
        "[AUTO-GRANT] 未找到依赖报告，自动补权限已禁用。\n"
        "  - 检查路径: %s\n"
        "  - 预期文件: dependency_chains_*.txt 或 VIEWs_chain_*.txt\n"
        "  - 解决方法: 先运行 schema_diff_reconciler.py 生成依赖报告",
        report_dir
    )
    return None
```

---

### 🟡 P2-01: 缓存无上限可能导致内存问题

**位置**: `run_fixup.py:564-581`

**问题描述**:  
`AutoGrantContext` 中的多个缓存字典无大小限制。

**现有代码**:
```python
@dataclass
class AutoGrantContext:
    # ...
    roles_cache: Dict[str, Set[str]]
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]]
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]]
    sys_privs_cache: Dict[str, Set[str]]
```

**风险**:
- 大规模数据库场景下缓存可能占用大量内存

**修复建议**:
使用 `functools.lru_cache` 或实现简单的 LRU 缓存类，设置合理上限（如 10000 条）。

---

### 🟡 P2-02: SQL 语句分割器边界条件

**位置**: `run_fixup.py:1889-1987`

**问题描述**:  
`split_sql_statements()` 函数对某些边界情况处理不完整。

**已知问题**:
1. 嵌套注释 `/* /* */ */` 处理不正确
2. Q-quote 跨多行时结束符匹配可能失败
3. 某些 OceanBase 特殊语法未覆盖

**修复建议**:
1. 添加嵌套注释计数器
2. 补充单元测试覆盖边界情况
3. 考虑使用更成熟的 SQL 解析库

---

### 🟡 P2-03: 错误分类不完整

**位置**: `run_fixup.py:179-221`

**问题描述**:  
`classify_sql_error()` 函数缺少常见错误码分类。

**缺失的错误码**:
| 错误码 | 说明 | 建议分类 |
|--------|------|----------|
| ORA-00054 | resource busy | LOCK_TIMEOUT |
| ORA-01017 | invalid username/password | AUTH_FAILED |
| ORA-12170 | TNS connect timeout | CONNECTION_TIMEOUT |
| ORA-04031 | unable to allocate memory | RESOURCE_EXHAUSTED |
| ORA-01555 | snapshot too old | SNAPSHOT_ERROR |
| ORA-00060 | deadlock detected | DEADLOCK |

**修复建议**:
补充上述错误码的分类，并在 `FailureType` 类中添加相应常量。

---

### 🟡 P2-04: 文件大小未做限制

**位置**: `run_fixup.py:3055-3061`

**问题描述**:  
读取 SQL 文件时未检查文件大小，极大文件可能导致内存耗尽。

**现有代码**:
```python
try:
    sql_text = sql_path.read_text(encoding="utf-8")
except Exception as exc:
    msg = f"读取文件失败: {exc}"
```

**修复建议**:
```python
MAX_SQL_FILE_SIZE = 50 * 1024 * 1024  # 50MB

try:
    file_size = sql_path.stat().st_size
    if file_size > MAX_SQL_FILE_SIZE:
        msg = f"文件过大 ({file_size / 1024 / 1024:.1f}MB > {MAX_SQL_FILE_SIZE / 1024 / 1024}MB)"
        results.append(ScriptResult(relative_path, "SKIPPED", msg, layer))
        log.warning("%s %s -> SKIP (%s)", label, relative_path, msg)
        continue
    sql_text = sql_path.read_text(encoding="utf-8")
except Exception as exc:
    msg = f"读取文件失败: {exc}"
```

---

### 🟡 P2-05: VIEW 链路循环依赖处理不完整

**位置**: `run_fixup.py:1769-1774`

**问题描述**:  
检测到循环依赖后设置 `blocked=True`，但后续仍会处理 `order` 列表中的节点。

**现有代码**:
```python
blocked = bool(cycles)
if cycles:
    plan_lines.append("BLOCK: 检测到依赖环，跳过自动执行。")
    for cycle in cycles:
        cycle_str = " -> ".join(f"{n[0]}({n[1]})" for n in cycle)
        plan_lines.append(f"  CYCLE: {cycle_str}")

# 后续仍会遍历 order 列表
for node in order:
    # ...
```

**修复建议**:
```python
blocked = bool(cycles)
if cycles:
    plan_lines.append("BLOCK: 检测到依赖环，跳过自动执行。")
    for cycle in cycles:
        cycle_str = " -> ".join(f"{n[0]}({n[1]})" for n in cycle)
        plan_lines.append(f"  CYCLE: {cycle_str}")
    # 循环依赖时直接返回，不继续处理
    return plan_lines, sql_lines, blocked
```

---

### 🟢 P3-01: 建议添加预检查阶段

**位置**: `main()` 函数入口

**建议**:
在正式执行前添加预检查，验证环境配置是否正确。

```python
def preflight_check(ob_cfg: Dict[str, str], fixup_dir: Path) -> List[str]:
    """执行前预检查，返回问题列表"""
    issues = []
    
    # 1. 验证 obclient 存在且可执行
    executable = ob_cfg.get("executable", "obclient")
    if not shutil.which(executable):
        issues.append(f"obclient 未找到: {executable}")
    
    # 2. 测试数据库连接
    try:
        obclient_cmd = build_obclient_command(ob_cfg)
        result = run_sql(obclient_cmd, "SELECT 1 FROM DUAL;", 10)
        if result.returncode != 0:
            issues.append(f"数据库连接失败: {result.stderr.strip()[:200]}")
    except Exception as e:
        issues.append(f"数据库连接测试异常: {e}")
    
    # 3. 检查 fixup 目录可读
    if not os.access(fixup_dir, os.R_OK):
        issues.append(f"无法读取 fixup 目录: {fixup_dir}")
    
    return issues
```

---

### 🟢 P3-02: 建议添加干运行模式

**位置**: `parse_args()` 函数

**建议**:
添加 `--dry-run` 参数，仅显示将执行的操作而不实际执行。

```python
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="仅显示将执行的脚本列表，不实际执行",
)
```

---

### 🟢 P3-03: 建议生成结构化执行报告

**建议**:
执行结束后生成 JSON 格式的结构化报告，便于集成到 CI/CD 流程。

```python
@dataclass
class FixupExecutionReport:
    timestamp: str
    duration_seconds: float
    total_scripts: int
    success_count: int
    failed_count: int
    skipped_count: int
    failed_scripts: List[Dict[str, str]]
    auto_grant_stats: Dict[str, int]
    
    def to_json(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=2, ensure_ascii=False))
```

---

## 三、与 schema_diff_reconciler 集成注意事项

### 3.1 脚本文件命名约定

`run_fixup.py` 通过文件名解析对象身份（`run_fixup.py:811-816`）：
```python
def parse_object_from_filename(path: Path) -> Tuple[Optional[str], Optional[str]]:
    stem = path.stem
    if "." not in stem:
        return None, None
    schema, name = stem.split(".", 1)
```

**依赖假设**: 文件名格式必须为 `SCHEMA.OBJECT_NAME.sql`

**风险**: 如果 `schema_diff_reconciler.py` 生成的文件名格式变化，解析会失败。

**建议**: 在 `schema_diff_reconciler.py` 中添加文件命名规范的常量定义，并在两个文件中共享。

### 3.2 脚本格式约定

`write_fixup_file()` 函数生成的脚本格式（`schema_diff_reconciler.py:18887-18920`）：
- 头部注释以 `-- ` 开头
- DDL 语句
- 可选的自动追加 GRANT 语句

`run_fixup.py` 依赖 `split_sql_statements()` 解析这些脚本。如果格式发生变化，需同步更新解析逻辑。

---

## 四、问题汇总表

| 编号 | 优先级 | 类型 | 位置 | 简述 |
|------|--------|------|------|------|
| P0-00 | 🔴 P0 | **功能缺失** | schema_diff_reconciler | **序列迁移缺少 LAST_NUMBER 同步** |
| P0-01 | 🔴 P0 | 健壮性 | 1990-1999 | subprocess 异常捕获不完整 |
| P0-02 | 🔴 P0 | 安全性 | 647-657 | 密码明文暴露在命令行 |
| P0-03 | 🔴 P0 | 数据安全 | 3098-3105 | 文件移动存在覆盖风险 |
| P1-01 | 🟠 P1 | 正确性 | 3785-3793 | 迭代模式累计失败计数错误 |
| P1-02 | 🟠 P1 | 防御性 | 602 | 端口号验证不严格 |
| P1-03 | 🟠 P1 | 安全性 | 618-622 | 路径遍历风险 |
| P1-04 | 🟠 P1 | 数据安全 | 2576-2591 | grant 文件重写原子性问题 |
| P1-05 | 🟠 P1 | 可用性 | 1218-1230 | 自动补权限跳过提示不足 |
| P2-01 | 🟡 P2 | 性能 | 564-581 | 缓存无上限 |
| P2-02 | 🟡 P2 | 正确性 | 1889-1987 | SQL 分割器边界条件 |
| P2-03 | 🟡 P2 | 可维护性 | 179-221 | 错误分类不完整 |
| P2-04 | 🟡 P2 | 健壮性 | 3055-3061 | 文件大小未限制 |
| P2-05 | 🟡 P2 | 正确性 | 1769-1774 | 循环依赖处理不完整 |
| P3-01 | 🟢 P3 | 功能增强 | main() | 建议添加预检查 |
| P3-02 | 🟢 P3 | 功能增强 | parse_args() | 建议添加干运行模式 |
| P3-03 | 🟢 P3 | 功能增强 | - | 建议生成结构化报告 |

---

## 五、修复优先级建议

### 第一阶段（必须修复）
0. **P0-00: 序列 LAST_NUMBER 同步** ⚠️ 最高优先级，涉及数据一致性
1. P0-01: subprocess 异常捕获
2. P0-02: 密码安全传递
3. P0-03: 文件移动安全性

### 第二阶段（重要修复）
4. P1-01: 迭代计数逻辑
5. P1-02: 端口验证
6. P1-03: 路径安全检查
7. P1-04: 文件写入原子性

### 第三阶段（质量提升）
8. P2-01 ~ P2-05: 各项中等优先级问题

### 第四阶段（功能增强）
9. P3-01 ~ P3-03: 新功能建议

---

## 六、附录：测试建议

修复完成后，建议补充以下测试用例：

1. **异常场景测试**
   - obclient 不存在
   - 数据库连接失败
   - 密码错误
   - 端口无效

2. **边界条件测试**
   - 空 SQL 文件
   - 超大 SQL 文件
   - 特殊字符文件名
   - 循环依赖 VIEW

3. **并发/重入测试**
   - 多次执行同一 fixup 目录
   - 中途中断后恢复

---

**报告生成者**: Code Review Assistant  
**审查完成日期**: 2026-02-04
