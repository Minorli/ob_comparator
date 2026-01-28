# schema_diff_reconciler.py 安全与缺陷审查报告

**审查日期**: 2026-01-28
**审查范围**: 安全漏洞、潜在缺陷、类型安全、反模式
**文件**: schema_diff_reconciler.py (~25,774 行)

---

## 1. 安全问题

### 1.1 SQL 注入风险 (严重)

**位置**: 行 7083-7086

**问题代码**:
```python
sql = (
    "SELECT 1 FROM DBA_TAB_COLUMNS "
    f"WHERE OWNER='{owner_u}' AND TABLE_NAME='{table_u}' AND COLUMN_NAME='{column_u}' "
    "AND ROWNUM = 1"
)
```

**问题**: 使用字符串插值构建 SQL 查询，而非参数化查询。虽然值经过 `.upper()` 处理，但如果输入包含特殊字符或引号，仍可能被利用。

**严重程度**: 严重

**建议**: 使用绑定变量的参数化查询：
```python
sql = "SELECT 1 FROM DBA_TAB_COLUMNS WHERE OWNER=:1 AND TABLE_NAME=:2 AND COLUMN_NAME=:3 AND ROWNUM = 1"
cursor.execute(sql, [owner_u, table_u, column_u])
```

### 1.2 命令行凭据暴露 (高)

**位置**: 行 7006, 13748

**问题代码**:
```python
command_args = [
    ob_cfg['executable'],
    '-h', ob_cfg['host'],
    '-P', ob_cfg['port'],
    '-u', ob_cfg['user_string'],
    '-p' + ob_cfg['password'],  # 密码直接拼接
    '-ss',
    '-e', sql_query
]
```

**问题**: 数据库密码作为命令行参数传递，可在进程列表和系统日志中可见。

**严重程度**: 高

**建议**: 通过环境变量或配置文件传递敏感信息：
```python
env = os.environ.copy()
env['MYSQL_PWD'] = ob_cfg['password']
subprocess.run(command_args, env=env, ...)
```

### 1.3 命令注入风险 (高)

**位置**: 行 7001-7019, 13743-13759, 18062-18068

**问题**: 使用用户可控数据构建子进程命令。虽然使用列表形式比 `shell=True` 更安全，但 `sql_query` 参数未经验证直接传递。

**严重程度**: 高

**建议**: 验证和清理 sql_query 参数，限制允许的 SQL 语句类型。

---

## 2. 潜在缺陷

### 2.1 数组/元组越界访问 (高)

**位置**: 行 5826, 6504-6505, 13003-13004, 13021, 13125, 13135, 15704, 19046, 22120

**问题代码**:
```python
# 危险 - 未检查 split 结果长度
src_obj = src_name.split('.', 1)[1]  # 行 5826

# 安全 - 有条件检查
src_name = src_full.split(".", 1)[1] if "." in src_full else src_full  # 行 6504
```

**问题**: 行 5826 直接访问索引 [1]，如果 `src_name` 不包含点号，将抛出 `IndexError`。

**严重程度**: 高

**建议**: 始终在索引前检查长度，或使用条件表达式。

### 2.2 数据库游标行元组越界 (高)

**位置**: 行 4807-4809, 4838-4841, 4858-4859, 4965-4968, 5011-5014, 5033-5035, 5089-5093, 5113-5117, 7204-7207, 7278-7283, 7422-7441, 8289-8291, 8333-8340, 8409-8411

**问题代码**:
```python
# 危险 - 未检查行长度
owner = (row[0] or '').strip().upper()
obj_name = (row[1] or '').strip().upper()
obj_type = (row[2] or '').strip().upper()

# 安全 - 有长度检查
status = parts[3].strip().upper() if len(parts) > 3 else "UNKNOWN"  # 行 7207
```

**问题**: 大多数访问未检查行是否有足够元素。如果查询返回的列数少于预期，将发生 `IndexError`。

**严重程度**: 高

**建议**: 在访问每个索引前检查 `len(row) > index`，或使用带错误处理的元组解包。

### 2.3 字符串 split 操作越界 (高)

**位置**: 行 4527, 4555, 4583, 4635, 4655, 4675, 6258, 6634, 6732, 14624, 15527, 15532

**问题代码**:
```python
table_str = item.table.split()[0]  # 行 4527 - 未检查
first_token = part.split()[0]  # 行 15527 - 未检查
return line.split('-')[0]  # 行 14624 - 未检查
```

**问题**: 如果字符串为空或仅包含空白，`split()` 返回空列表，访问 [0] 将抛出 `IndexError`。

**严重程度**: 高

**建议**: 访问前检查 `if split_result:`，或使用 `split_result[0] if split_result else default_value`。

### 2.4 资源泄漏 - 数据库连接 (中)

**位置**: 行 21429-21433

**问题代码**:
```python
if oracle_conn:
    try:
        oracle_conn.close()
    except Exception:
        pass
```

**问题**: 如果在此之前发生异常，连接可能未关闭。`try-except-pass` 模式静默忽略错误。

**严重程度**: 中

**建议**: 使用上下文管理器 (`with` 语句) 确保资源清理：
```python
with oracledb.connect(...) as oracle_conn:
    # 使用连接
```

### 2.5 潜在 None 解引用 (中)

**位置**: 行 1681-1683, 2104, 3860, 3867, 5181

**问题代码**:
```python
head = parts[0].lower()  # 行 1681 - parts 可能为空
if head == "unsupported" and len(parts) > 1:
    head = parts[1].lower()  # 行 1683 - parts[1] 可能为 None
```

**问题**: 如果 `parts` 为空，访问 `parts[0]` 抛出 `IndexError`。如果 `parts[1]` 为 None，调用 `.lower()` 抛出 `AttributeError`。

**严重程度**: 中

**建议**: 在索引前添加长度检查，在方法调用前添加 None 检查。

### 2.6 不安全的类型转换 (中)

**位置**: 行 7288, 7294, 7320, 7486-7489, 7665, 8940, 8943, 9167

**问题代码**:
```python
entry["count"] = int(entry["count"]) + 1  # 行 7288 - 无 try-except
pos = int(parts[3]) if parts[3] else None  # 行 7665 - 非数字字符串会失败
```

**问题**: 如果字符串不是有效整数，`int()` 抛出 `ValueError`。部分转换有检查，部分没有。

**严重程度**: 中

**建议**: 将所有类型转换包装在 try-except 块中，或先验证格式。

### 2.7 子进程超时处理不一致 (中)

**位置**: 行 7034-7043, 13752-13765, 18062-18083

**问题**: 部分 subprocess 调用处理了 `TimeoutExpired`，但错误处理不一致。某些调用如果未设置超时可能无限挂起。

**严重程度**: 中

**建议**: 确保所有 `subprocess.run/Popen` 调用一致设置 timeout 参数。

---

## 3. 错误处理问题

### 3.1 宽泛异常捕获 (中)

**位置**: 行 3827, 4273, 4745, 7041, 13690, 13697, 13704, 21433

**问题代码**:
```python
except Exception:  # 行 3827
    log.warning(f"  [规则警告] 第 {i+1} 行解析失败，已跳过: {line}")

except Exception as exc:  # 行 4745
    log.error(f"错误详情: {exc}")
```

**问题**: 捕获所有异常包括 `SystemExit`、`KeyboardInterrupt` 和编程错误，使调试困难。

**严重程度**: 中

**建议**: 捕获具体异常类型 (ValueError, KeyError 等) 而非裸 Exception。

### 3.2 错误处理模式不一致 (低)

**位置**: 多处

**问题**: 混用不同的错误处理方式：
- 部分函数返回带错误标志的元组: `(ok, data, err)`
- 部分函数直接调用 `sys.exit(1)`
- 部分函数记录日志后继续
- 部分函数抛出异常

**建议**: 在代码库中统一错误处理模式。

---

## 4. 线程安全问题

### 4.1 线程中调用 sys.exit (低)

**位置**: 行 14547-14564, 20122-20126

**问题代码**:
```python
with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
    futures = {executor.submit(...): schema for ...}
    for future in as_completed(futures):
        if err:
            for f in futures:
                f.cancel()
            sys.exit(1)  # 在线程上下文中不安全
```

**问题**: 从线程上下文调用 `sys.exit(1)` 是不安全的，应使用适当的线程同步。

**严重程度**: 低

**建议**: 使用 `threading.Event` 或类似机制协调线程关闭。

---

## 5. 问题汇总表

| 问题 | 行号 | 严重程度 | 类型 | 影响 |
|------|------|----------|------|------|
| SQL 注入 | 7083-7086 | 严重 | 安全 | 数据库被入侵 |
| 命令行凭据暴露 | 7006, 13748 | 高 | 安全 | 凭据泄露 |
| 命令注入风险 | 7001-7019 | 高 | 安全 | 系统被入侵 |
| 数组越界访问 | 5826 等 | 高 | 缺陷 | IndexError 崩溃 |
| 行元组越界 | 4807-4809 等 | 高 | 缺陷 | IndexError 崩溃 |
| split 越界 | 4527 等 | 高 | 缺陷 | IndexError 崩溃 |
| 宽泛异常捕获 | 3827 等 | 中 | 缺陷 | 错误被掩盖 |
| 资源泄漏 | 21429-21433 | 中 | 缺陷 | 连接泄漏 |
| None 解引用 | 1681-1683 | 中 | 缺陷 | AttributeError 崩溃 |
| 类型转换 | 7288 等 | 中 | 缺陷 | ValueError 崩溃 |
| 超时处理 | 7034-7043 | 中 | 缺陷 | 潜在挂起 |
| 错误处理不一致 | 多处 | 低 | 设计 | 可维护性差 |
| 线程安全 | 14547-14564 | 低 | 缺陷 | 竞态条件 |

---

## 6. 修复建议优先级

### 立即处理 (严重/高)

| 项目 | 工作量 | 影响 |
|------|--------|------|
| 将 SQL 字符串拼接改为参数化查询 | 2-4 小时 | 严重 |
| 将凭据从命令行参数移至环境变量 | 1-2 小时 | 高 |
| 为所有数组/元组索引添加边界检查 | 4-8 小时 | 高 |

### 短期处理 (中)

| 项目 | 工作量 | 影响 |
|------|--------|------|
| 将裸 `except Exception:` 改为具体异常类型 | 2-4 小时 | 中 |
| 在入口点添加全面的输入验证 | 4-6 小时 | 中 |
| 使用上下文管理器实现资源清理 | 2-4 小时 | 中 |

### 长期处理 (低)

| 项目 | 工作量 | 影响 |
|------|--------|------|
| 统一错误处理模式 | 8-16 小时 | 低 |
| 添加类型提示以支持静态分析 | 16-24 小时 | 低 |
| 添加预提交钩子进行安全扫描 | 2-4 小时 | 低 |

---

## 7. 审查结论

| 类别 | 问题数 | 严重程度 |
|------|--------|----------|
| 安全漏洞 | 3 | 严重/高 |
| 越界访问风险 | 30+ 处 | 高 |
| 资源管理问题 | 5+ 处 | 中 |
| 类型安全问题 | 10+ 处 | 中 |
| 错误处理问题 | 32 处 | 中/低 |

**总体评估**: 代码存在若干安全风险和潜在运行时错误。建议优先修复 SQL 注入和凭据暴露问题，然后系统性地添加边界检查和输入验证。

---

*审查工具: Claude Code (claude-opus-4-5-20251101)*
