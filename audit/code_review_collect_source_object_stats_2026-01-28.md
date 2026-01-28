# collect_source_object_stats.py 代码审查报告

**审查日期**: 2026-01-28
**审查范围**: 未使用对象、重复代码、安全、缺陷、代码质量
**文件**: collect_source_object_stats.py

---

## 1. 未使用对象

| 对象名 | 行号 | 类型 | 严重程度 | 说明 |
|--------|------|------|----------|------|
| `Iterable` | 23 | import | 低 | 从 typing 导入但从未使用 |

**建议**: 移除未使用的导入

---

## 2. 重复代码

### 2.1 重复 SQL 模板定义 (中)

**位置**: 行 272-293 和 402-423

**问题**: INDEX, CONSTRAINT, TRIGGER 查询的 SQL 模板定义了两次（一次在 `print_brief_report()`，一次在 `main()`）

**建议**: 将 SQL 模板提取为模块级常量

### 2.2 重复统计汇总函数 (中)

**位置**: 行 165-188 和 191-210

**问题**: `summarize_table_stats()` 和 `summarize_table_stats_brief()` 包含几乎相同的逻辑，仅格式化不同

**建议**: 合并为一个函数，使用 `brief` 参数控制输出格式

---

## 3. 安全问题

### 3.1 SQL 注入风险 (低)

**位置**: 行 95

**代码**:
```python
obj_clause = ",".join(f"'{t}'" for t in object_types)
```

**问题**: 虽然 `object_types` 在此文件中是硬编码的（行 33-50），但如果此函数被用户输入调用，将存在漏洞

**建议**: 使用参数化查询或在字符串插值前验证白名单

### 3.2 错误消息中的凭据 (低)

**位置**: 行 331

**代码**:
```python
print(f"ERROR: Oracle connection failed: {exc}", file=sys.stderr)
```

**问题**: Oracle 错误消息可能包含连接详情

**建议**: 在记录前清理异常消息

---

## 4. 潜在缺陷

### 4.1 百分位计算边界错误 (中)

**位置**: 行 175-176, 201-202

**代码**:
```python
p95 = counts[int(len(counts) * 0.95) - 1] if counts else 0
p99 = counts[int(len(counts) * 0.99) - 1] if counts else 0
```

**问题**:
- 当 `len(counts) == 1` 时，`int(1 * 0.95) - 1 = -1`（访问最后一个元素，非预期）
- 当 `len(counts) == 2` 时，`int(2 * 0.99) - 1 = 0`（应为索引 1）

**建议**: 使用 `max(0, int(len(counts) * percentile) - 1)` 或使用 `numpy.percentile()`

### 4.2 空列表边界情况 (低)

**位置**: 行 175-176, 201-202

**问题**: `if counts else 0` 防止崩溃，但无数据时返回 0 语义不正确（应为 None 或 "N/A"）

**建议**: 返回 `None` 或使用哨兵值表示缺失数据

### 4.3 PUBLIC 同义词处理 (低)

**位置**: 行 346

**代码**:
```python
counts_by_owner[public_owner]["SYNONYM"] += public_synonym_count
```

**问题**: 如果 `counts_by_owner[public_owner]` 不存在，可能导致隐式行为

**建议**: 显式初始化: `counts_by_owner.setdefault(public_owner, {})["SYNONYM"] = ...`

---

## 5. 代码质量问题

### 5.1 魔法数字 (低)

**位置**: 行 352, 31

**示例**:
```python
top_n = max(1, min(args.top_n, 20))  # 硬编码限制 20 未文档化
ORACLE_IN_BATCH_SIZE = 900  # 无注释解释为何是 900
```

**建议**: 添加内联注释解释这些常量

### 5.2 错误处理不一致 (低)

**位置**: 行 330-332 和 348-349

**问题**: Oracle 连接错误被捕获并记录，但 PUBLIC 同义词获取错误仅存储为字符串

**建议**: 统一错误处理策略

---

## 6. 问题汇总表

| 类别 | 数量 | 严重程度 |
|------|------|----------|
| 未使用导入 | 1 | 低 |
| 重复代码 | 2 处 | 中 |
| 安全问题 | 2 | 低 |
| 潜在缺陷 | 3 | 低-中 |
| 代码质量 | 2 类 | 低 |

---

## 7. 优先修复建议

1. **合并重复的 SQL 模板和汇总函数** - 减少维护负担
2. **修复百分位计算边界错误** (行 175-176, 201-202)
3. **移除未使用的导入** (`Iterable`)
4. **添加魔法数字注释**

---

*审查工具: Claude Code (claude-opus-4-5-20251101)*
