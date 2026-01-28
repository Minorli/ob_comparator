# 视图重写 Bug 分析报告：表别名被错误替换为 SCHEMA.ALIAS

**报告日期**: 2026-01-28  
**严重程度**: 高  
**影响范围**: 所有需要 remap 的视图 DDL 生成

---

## 一、问题描述

### 1.1 错误现象

视图 DDL 重写后，**表别名**被错误地添加了 schema 前缀。

**错误输出**:
```sql
SELECT t.FCD, t.FCU
FROM UWSDATA.POL_INFO LIFEDATA.T, LCSDATA.CHILD_REGION_CODE_SYNCH r1, LCSDATA.REGION_CODE_TBL b1
WHERE t.lcd >= sysdate - 7
AND business_src IN ('D', 'W')
```

**正确应为**:
```sql
SELECT t.FCD, t.FCU
FROM UWSDATA.POL_INFO T, LCSDATA.CHILD_REGION_CODE_SYNCH r1, LCSDATA.REGION_CODE_TBL b1
WHERE t.lcd >= sysdate - 7
AND business_src IN ('D', 'W')
```

### 1.2 问题本质

`UWSDATA.POL_INFO T` 中的 `T` 是**表别名（table alias）**，但程序将其误判为对象引用，并替换为 `LIFEDATA.T`。

---

## 二、根本原因分析

### 2.1 问题定位

**问题函数**: `remap_view_dependencies()`  
**文件位置**: `schema_diff_reconciler.py` 第 15149-15244 行

### 2.2 代码流程

```
1. extract_view_dependencies() 提取 FROM/JOIN 后的对象名
   ↓
2. 构建 replacements 字典: {源对象名 -> 目标全名}
   ↓
3. 对于同 schema 内的对象，额外添加裸名替换规则:
   replacements[dep_obj] = tgt_full  (例如: "T" -> "LIFEDATA.T")
   ↓
4. 使用正则表达式全局替换
```

### 2.3 Bug 触发条件

当满足以下条件时触发 bug：

1. **存在短名对象**：源端存在一个名为 `T` 的表/视图/同义词（位于 `LIFEDATA` schema）
2. **该对象需要 remap**：`LIFEDATA.T` 需要映射到目标端
3. **视图 SQL 中使用相同字母作为别名**：恰好使用 `T` 作为表别名

### 2.4 代码缺陷位置

```python
# schema_diff_reconciler.py:15218-15220
if dep_schema == view_schema_u:
    # 无前缀引用也替换为全名(或目标名)，避免跨 schema 迁移后失效
    replacements.setdefault(dep_obj, tgt_u)  # ← BUG: dep_obj="T" 被加入替换列表
```

**问题**：代码假设所有无前缀的标识符都是对象引用，但没有考虑**表别名**的情况。

### 2.5 正则表达式缺陷

```python
# schema_diff_reconciler.py:15236-15241
# 对于无前缀标识符 (如 "T")：
pattern = re.compile(
    rf'(?<![A-Z0-9_\$#"\.]){re.escape(src_ref)}(?![A-Z0-9_\$#"])',
    re.IGNORECASE
)
```

**问题**：正则只检查前后字符边界，无法区分：
- `FROM table_name T` → `T` 是别名
- `FROM T` → `T` 是对象名

---

## 三、影响分析

### 3.1 受影响场景

| 场景 | 是否受影响 |
|-----|-----------|
| 视图使用单字母/短名表别名 (如 T, A, B) | ✅ 高风险 |
| 源端存在同名短名对象 | ✅ 高风险 |
| 视图引用 PUBLIC 同义词解析后的短名对象 | ✅ 高风险 |
| 使用长别名 (如 pol_info_alias) | ⚠️ 低风险（但仍可能误中） |

### 3.2 潜在后果

1. **生成的 fixup 脚本语法错误**：`FROM TABLE_A SCHEMA.T` 不是合法 SQL
2. **执行失败**：CREATE VIEW 语句会报错
3. **逻辑错误**：如果恰好存在 `SCHEMA.T` 对象，可能创建出错误的视图

---

## 四、修复建议

### 4.1 方案 A：语法感知替换（推荐）

在替换前解析 SQL 结构，识别 FROM 子句中的**表引用**和**别名**位置。

```python
def _parse_from_clause(segment: str) -> List[Tuple[str, Optional[str]]]:
    """
    解析 FROM 子句，返回 [(table_ref, alias), ...]
    例如: "UWSDATA.POL_INFO T, LCSDATA.TBL r1"
       -> [("UWSDATA.POL_INFO", "T"), ("LCSDATA.TBL", "r1")]
    """
    results = []
    parts = segment.split(',')
    for part in parts:
        tokens = part.strip().split()
        if not tokens:
            continue
        table_ref = tokens[0]
        alias = tokens[1] if len(tokens) > 1 and not _is_keyword(tokens[1]) else None
        results.append((table_ref, alias))
    return results
```

**替换时跳过别名位置**：

```python
# 在 remap_view_dependencies 中
# 1. 先提取 FROM 子句中的别名集合
aliases = set()
for table_ref, alias in _parse_from_clause(from_segment):
    if alias:
        aliases.add(alias.upper())

# 2. 替换时排除别名
for src_ref in replacements:
    if src_ref in aliases:
        continue  # 跳过别名，不替换
    # ... 执行替换
```

### 4.2 方案 B：位置感知替换

在替换时检查标识符的位置上下文：

```python
def _is_alias_position(sql: str, match_start: int, match_end: int) -> bool:
    """
    判断匹配位置是否是表别名位置。
    表别名的特征：紧跟在"表引用"后面，且不是关键字。
    """
    # 向前查找，如果前面是 SCHEMA.TABLE 或 TABLE 形式，且中间只有空白，则是别名
    prefix = sql[:match_start].rstrip()
    # 检查是否紧跟在一个标识符后（可能是表名）
    if re.search(r'[A-Z0-9_\$#"]\s*$', prefix, re.IGNORECASE):
        # 再检查更前面是否有 FROM/JOIN 关键字
        if re.search(r'\b(FROM|JOIN)\s+[A-Z0-9_\$#".]+\s*$', prefix, re.IGNORECASE):
            return True
    return False
```

### 4.3 方案 C：保守替换（最小改动）

只替换**已带 schema 前缀**的引用，不替换裸名引用。

```python
# 修改 schema_diff_reconciler.py:15218-15220
# 注释掉或删除裸名替换逻辑：
# if dep_schema == view_schema_u:
#     replacements.setdefault(dep_obj, tgt_u)  # ← 删除此行
```

**风险**：可能导致跨 schema 迁移时裸名引用失效，需要配合其他机制（如 `adjust_ddl_for_object` 的 `replace_unqualified_identifier`）处理。

---

## 五、测试用例

### 5.1 回归测试

```sql
-- 测试视图 1: 单字母别名
CREATE VIEW TEST_V1 AS
SELECT t.col1, t.col2
FROM SCHEMA_A.TABLE1 T, SCHEMA_B.TABLE2 r
WHERE t.id = r.id;

-- 期望：T 和 r 作为别名保持不变

-- 测试视图 2: 存在同名短名对象
-- 假设 SCHEMA_A 中存在表 T
CREATE VIEW TEST_V2 AS
SELECT T.col1, s.col2
FROM SCHEMA_A.T T, SCHEMA_A.TABLE2 s
WHERE T.id = s.id;

-- 期望：FROM 后的 T (表名) 替换为目标 schema，但别名 T 保持不变

-- 测试视图 3: 子查询中的别名
CREATE VIEW TEST_V3 AS
SELECT *
FROM (SELECT a.col1 FROM SCHEMA_A.TABLE1 a) sub, SCHEMA_B.TABLE2 b
WHERE sub.col1 = b.col1;

-- 期望：子查询中的别名 a 和外层的别名 sub, b 都保持不变
```

### 5.2 验证方法

1. 生成 fixup 脚本后，手动检查 FROM/JOIN 子句
2. 确认表别名未被添加 schema 前缀
3. 在 OB 目标库执行 CREATE VIEW，确认语法正确

---

## 六、临时绕过方案

在修复前，可采用以下方式绕过：

1. **手动修正**：生成脚本后手动检查并修正别名
2. **使用长别名**：在源端视图中避免使用单字母别名（如用 `pol_info` 代替 `t`）
3. **禁用裸名替换**：临时注释 15218-15220 行代码

---

## 七、相关代码位置

| 文件 | 函数 | 行号 | 说明 |
|-----|------|-----|------|
| schema_diff_reconciler.py | `extract_view_dependencies` | 15048-15146 | 提取视图依赖 |
| schema_diff_reconciler.py | `remap_view_dependencies` | 15149-15244 | **问题函数** |
| schema_diff_reconciler.py | `adjust_ddl_for_object` | 15537-15802 | DDL 调整（辅助函数） |
| schema_diff_reconciler.py | `replace_unqualified_identifier` | 15581-15700 | 裸名替换（有上下文检测但不完整） |

---

## 八、修复优先级

**建议**: P0（高优先级）

**理由**:
1. 影响所有视图的 fixup 脚本生成
2. 导致执行失败，无法自动修复
3. 问题在常见 SQL 写法中容易触发（单字母别名很常见）

---

## 九、实际案例确认

### 9.1 确认的触发条件

经确认，源端 `LIFEDATA` schema 下**确实存在一张名为 `T` 的表**。这正是触发本 bug 的直接原因：

- 程序识别到 `LIFEDATA.T` 是依赖对象
- 添加替换规则 `T -> LIFEDATA.T`
- 视图 SQL 中的表别名 `T` 被错误替换

### 9.2 排查建议

建议排查源端是否存在其他短名对象，这些都是高风险的误替换候选：

```sql
-- 查找短名对象（可能触发 bug）
SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
FROM DBA_OBJECTS
WHERE LENGTH(OBJECT_NAME) <= 2
  AND OWNER NOT IN ('SYS', 'SYSTEM', 'PUBLIC')
  AND OBJECT_TYPE IN ('TABLE', 'VIEW', 'SYNONYM')
ORDER BY OWNER, OBJECT_NAME;
```

### 9.3 常见高风险对象名

| 对象名 | 风险说明 |
|-------|---------|
| `T` | 最常用的表别名 |
| `A`, `B`, `C` | 常用于多表 JOIN 的别名 |
| `X`, `Y` | 常用于子查询别名 |
| `R`, `S` | 常用于关联查询 |

---

## 十、数据治理建议

1. **长期**：避免创建单字母或双字母的表/视图/同义词名称
2. **短期**：在修复代码前，生成 fixup 脚本后需人工复核 FROM/JOIN 子句
3. **自动化**：可考虑在脚本生成后增加语法校验步骤，检测明显的语法错误

---

**报告人**: Cascade AI  
**审核状态**: 待审核  
**更新记录**:
- 2026-01-28: 确认 `LIFEDATA.T` 表存在，补充排查 SQL 和数据治理建议
