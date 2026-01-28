# run_fixup.py 代码审查报告

**审查日期**: 2026-01-28
**审查范围**: 未使用对象、重复代码、复杂度、安全、缺陷
**文件**: run_fixup.py (~3,872 行)

---

## 1. 未使用对象

| 对象名 | 行号 | 类型 | 严重程度 | 说明 |
|--------|------|------|----------|------|
| `json` | 41 | import | 低 | 导入但从未使用 |
| `DEFAULT_OBCLIENT_TIMEOUT` | 57 | 常量 | 低 | 定义但从未引用，使用的是 `DEFAULT_FIXUP_TIMEOUT` |

**建议**: 移除未使用的导入和常量。

---

## 2. 重复代码

### 2.1 文件移动逻辑重复 (中)

**位置**: 行 2543-2548, 2610-2615, 3083-3088, 3120-3124

**重复代码**:
```python
try:
    target_dir = done_dir / sql_path.parent.name
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / sql_path.name
    shutil.move(str(sql_path), target_path)
    move_note = f"(已移至 done/{sql_path.parent.name}/)"
except Exception as exc:
    move_note = f"(移动失败: {exc})"
```

**建议**: 提取为辅助函数 `move_to_done_dir(sql_path, done_dir) -> str`

### 2.2 错误预览提取重复 (中)

**位置**: 行 300, 2380, 2388, 2630, 3141, 3220

**重复代码**:
```python
msg_preview = item.message.splitlines()[0][:80] if item.message else "无错误信息"
```

**问题**: 如果 `splitlines()` 返回空列表，访问 `[0]` 会抛出 `IndexError`

**建议**: 创建辅助函数 `get_first_line(text: str, max_len: int = 80) -> str`

### 2.3 异常处理模式重复 (中)

**位置**: 行 88, 118, 600, 2159, 2399, 2661

**问题**: 多处使用裸 `except Exception:` 且无日志记录

**建议**: 创建装饰器或辅助函数统一异常处理

---

## 3. 高复杂度函数

| 函数名 | 行号 | 代码行数 | 参数数 | 严重程度 | 问题 |
|--------|------|----------|--------|----------|------|
| `run_iterative_fixup` | 3510 | 358 行 | - | 高 | 主循环嵌套深度 5+ 层 |
| `build_view_chain_plan` | 1726 | 147 行 | 18 | 高 | 参数过多，逻辑复杂 |
| `ensure_view_owner_grant_option` | 1631 | 93 行 | 16 | 高 | 递归调用，参数过多 |
| `plan_object_grant_for_dependency` | 1535 | 94 行 | 15 | 高 | 参数过多 |
| `collect_sql_files_by_layer` | 647 | 133 行 | - | 中 | 两条路径有大量重复 |
| `split_sql_statements` | 1875 | 99 行 | - | 中 | 状态机复杂，6 个状态变量 |

### 3.1 参数过多示例

**`plan_object_grant_for_dependency`** (行 1535):
```python
def plan_object_grant_for_dependency(
    grantee: str,
    target_full: str,
    target_type: str,
    required_priv: str,
    require_grant_option: bool,
    allow_fallback: bool,
    obclient_cmd: List[str],
    timeout: Optional[int],
    grant_index_miss: GrantIndex,
    grant_index_all: GrantIndex,
    roles_cache: Dict[str, Set[str]],
    tab_privs_cache: Dict[Tuple[str, str, str], Set[str]],
    tab_privs_grantable_cache: Dict[Tuple[str, str, str], Set[str]],
    sys_privs_cache: Dict[str, Set[str]],
    planned_statements: Set[str],
    planned_object_privs: Set[Tuple[str, str, str]],
    planned_object_privs_with_option: Set[Tuple[str, str, str]],
    planned_sys_privs: Set[Tuple[str, str]],
    plan_lines: List[str],
    sql_lines: List[str]
) -> bool:
```

**建议**: 将相关参数分组到 dataclass 中

---

## 4. 安全问题

### 4.1 SQL 注入风险 (中)

**位置**: 行 1358-1363, 1384-1387, 1404-1408, 1426-1431, 1450-1452

**代码示例**:
```python
sql = (
    "SELECT COUNT(*) FROM DBA_OBJECTS "
    f"WHERE OWNER='{escape_sql_literal(schema)}' "
    f"AND OBJECT_NAME='{escape_sql_literal(name)}' "
    f"AND OBJECT_TYPE='{escape_sql_literal(obj_type.upper())}'"
)
```

**评估**: 使用了 `escape_sql_literal()` 进行转义，风险已缓解，但参数化查询更安全

### 4.2 命令行凭据暴露 (中)

**位置**: 行 634-644

**代码**:
```python
f"-p{ob_cfg['password']}",
```

**问题**: 密码作为命令行参数传递，可在进程列表中可见

**建议**: 这是 obclient CLI 的固有设计限制，考虑使用环境变量或配置文件

### 4.3 文件路径未验证 (低)

**位置**: 行 2547, 2614, 3087, 3123

**问题**: `shutil.move()` 调用前未验证源路径是否在预期目录内

**建议**: 添加路径验证防止目录遍历攻击

---

## 5. 潜在缺陷

### 5.1 逻辑错误 - cumulative_failed 跟踪 (高)

**位置**: 行 3772

**代码**:
```python
cumulative_failed = round_failed  # 只计算当前失败数
```

**问题**: 覆盖而非累加之前的失败数

**建议**: 应改为 `cumulative_failed += round_failed` 或单独跟踪

### 5.2 不安全的 splitlines()[0] (中)

**位置**: 行 300, 2380, 2388, 2630, 3141, 3220

**问题**: 如果 `splitlines()` 返回空列表，访问 `[0]` 会抛出 `IndexError`

**建议**: 使用 `(text.splitlines() or [""])[0]` 或辅助函数

### 5.3 裸异常捕获 (中)

**位置**: 行 88, 118, 600, 2159, 2399, 2661

**问题**: 捕获所有异常但不记录详情

**建议**: 添加日志记录或使用更具体的异常类型

---

## 6. 代码风格问题

### 6.1 魔法数字 (低)

**位置**: 行 200, 254, 263, 272, 289, 298, 3219

**示例**:
```python
if len(items) <= 5:
    for item in items[:5]:
```

**建议**: 定义常量如 `MAX_FAILURE_PREVIEW = 5`

### 6.2 长参数列表 (中)

**位置**: 行 1535, 1631, 1726, 3243, 3510

**问题**: 多个函数有 15+ 个参数

**建议**: 使用 dataclass 分组参数（部分已实现）

---

## 7. 问题汇总表

| 类别 | 数量 | 严重程度 |
|------|------|----------|
| 未使用对象 | 2 | 低 |
| 重复代码 | 5 处 | 中 |
| 高复杂度函数 | 6 | 高 |
| 安全问题 | 3 | 中 |
| 潜在缺陷 | 3 | 中-高 |
| 代码风格 | 2 类 | 低 |

---

## 8. 优先修复建议

### 严重 (立即修复)

1. **修复 `cumulative_failed` 逻辑** (行 3772) - 当前覆盖而非累加

### 高优先级

2. **重构参数过多的函数** - `plan_object_grant_for_dependency`, `ensure_view_owner_grant_option` (15+ 参数)
3. **提取文件移动逻辑** - 重复 4 次

### 中优先级

4. **创建安全的首行提取辅助函数** - 使用 6 次，有 IndexError 风险
5. **为裸 `except Exception:` 添加日志** - 便于调试
6. **拆分 `split_sql_statements()`** - 状态机过于复杂

### 低优先级

7. **移除未使用的导入和常量** (`json`, `DEFAULT_OBCLIENT_TIMEOUT`)
8. **定义魔法数字常量**

---

## 9. 正面发现

- 良好的 dataclass 结构用于类型安全
- 全面的类型提示
- 完善的错误分类系统
- 正确使用 `is None` 进行 None 检查
- 使用 `escape_sql_literal()` 进行 SQL 转义
- 良好的关注点分离
- 优秀的日志和进度跟踪
- 正确使用 Path 对象而非字符串

---

*审查工具: Claude Code (claude-opus-4-5-20251101)*
