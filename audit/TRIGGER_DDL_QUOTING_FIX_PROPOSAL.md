# DDL 引号格式与约束修复方案

**日期**: 2026-01-27  
**问题级别**: 高  
**影响范围**: 触发器、VIEW、SYNONYM DDL 生成；CHECK 约束重复问题

---

## 1. 问题描述

用户发现触发器生成的 DDL 中，schema 和对象名的引号格式错误：

| 位置 | 错误格式 | 正确格式 |
|------|---------|---------|
| CREATE TRIGGER 名称 | `"SCHEMA.TRIGGER_NAME"` | `"SCHEMA"."TRIGGER_NAME"` |
| ON 子句表名 | `"SCHEMA.TABLE_NAME"` | `"SCHEMA"."TABLE_NAME"` |

**错误示例**:
```sql
CREATE OR REPLACE TRIGGER "SCOTT.MY_TRIGGER"
BEFORE UPDATE ON "SCOTT.MY_TABLE"
FOR EACH ROW
BEGIN
  ...
END;
```

**正确示例**:
```sql
CREATE OR REPLACE TRIGGER "SCOTT"."MY_TRIGGER"
BEFORE UPDATE ON "SCOTT"."MY_TABLE"
FOR EACH ROW
BEGIN
  ...
END;
```

---

## 2. 问题根因分析

### 2.1 触发器主函数 `remap_trigger_ddl` (行 15965-16033)

**问题位置 1**: 行 16017-16018
```python
working_sql = name_pattern.sub(
    lambda m: f"{m.group(1)}{tgt_schema_u}.{tgt_trigger_u}",  # ❌ 无引号
    working_sql,
    count=1
)
```

**问题位置 2**: 行 16029-16030
```python
working_sql = on_pattern.sub(
    lambda m: f"{m.group(1)}{on_schema_u}.{on_table_u}",  # ❌ 无引号
    working_sql,
    count=1
)
```

### 2.2 备用重写函数 `_rewrite_trigger_name_and_on` (行 20559-20571)

**问题位置 3**: 行 20564
```python
text = name_pattern.sub(rf'\1{ts}.{to}', text, count=1)  # ❌ 无引号
```

**问题位置 4**: 行 20570
```python
text = on_pattern.sub(rf'\1{tts}.{tt}', text, count=1)  # ❌ 无引号
```

---

## 3. 修复方案

### 3.1 建议引入辅助函数

在文件头部（约第 700 行附近）添加统一的引号处理函数：

```python
def quote_identifier(name: str) -> str:
    """为 Oracle/OceanBase 标识符添加双引号"""
    return f'"{name.upper()}"'

def quote_qualified_name(schema: str, obj: str) -> str:
    """生成带引号的全限定名: "SCHEMA"."OBJECT" """
    return f'"{schema.upper()}"."{obj.upper()}"'
```

### 3.2 修复 `remap_trigger_ddl` 函数

**修复位置 1** (行 16017-16021):
```python
# 修复前
working_sql = name_pattern.sub(
    lambda m: f"{m.group(1)}{tgt_schema_u}.{tgt_trigger_u}",
    working_sql,
    count=1
)

# 修复后
working_sql = name_pattern.sub(
    lambda m: f'{m.group(1)}"{tgt_schema_u}"."{tgt_trigger_u}"',
    working_sql,
    count=1
)
```

**修复位置 2** (行 16029-16032):
```python
# 修复前
working_sql = on_pattern.sub(
    lambda m: f"{m.group(1)}{on_schema_u}.{on_table_u}",
    working_sql,
    count=1
)

# 修复后
working_sql = on_pattern.sub(
    lambda m: f'{m.group(1)}"{on_schema_u}"."{on_table_u}"',
    working_sql,
    count=1
)
```

### 3.3 修复 `_rewrite_trigger_name_and_on` 函数

**修复位置 3** (行 20564):
```python
# 修复前
text = name_pattern.sub(rf'\1{ts}.{to}', text, count=1)

# 修复后
text = name_pattern.sub(rf'\1"{ts}"."{to}"', text, count=1)
```

**修复位置 4** (行 20570):
```python
# 修复前
text = on_pattern.sub(rf'\1{tts}.{tt}', text, count=1)

# 修复后
text = on_pattern.sub(rf'\1"{tts}"."{tt}"', text, count=1)
```

---

## 4. 其他可能受影响的位置

以下位置使用了类似的 `{schema}.{object}` 格式，建议一并审查是否需要添加引号：

### 4.1 DROP 语句 (行 17036, 17043)

```python
# 行 17036
return f"ALTER TABLE {schema_u}.{parent_table.upper()} DROP CONSTRAINT {name_u}"

# 行 17043  
return f"DROP {obj_type_u} {schema_u}.{name_u}"
```

**建议**: 视上下文决定是否需要引号。如果对象名可能包含特殊字符或保留字，应添加引号。

### 4.2 ALTER TABLE 语句 (行 17803, 17845, 等)

```python
f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} ADD (...)"
f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} MODIFY (...)"
```

**建议**: 同上，视对象名是否包含特殊字符决定。

### 4.3 约束 FK REFERENCES 子句 (行 20406)

```python
f"REFERENCES {ref_tgt_schema}.{ref_tgt_table} ({', '.join(ref_cols)})"
```

**建议**: 应改为 `REFERENCES "{ref_tgt_schema}"."{ref_tgt_table}"`

---

## 5. 测试建议

### 5.1 单元测试用例

```python
def test_trigger_ddl_quoting(self):
    """测试触发器 DDL 中 schema.object 的引号格式"""
    ddl = 'CREATE OR REPLACE TRIGGER SCOTT.MY_TRIGGER BEFORE UPDATE ON SCOTT.MY_TABLE FOR EACH ROW BEGIN NULL; END;'
    
    result = remap_trigger_ddl(
        ddl,
        full_object_mapping={...},
        source_schema='SCOTT',
        tgt_schema='OB_USER',
        tgt_trigger='MY_TRIGGER',
        on_target=('OB_USER', 'MY_TABLE'),
        qualify_schema=True
    )
    
    # 验证格式
    self.assertIn('"OB_USER"."MY_TRIGGER"', result)
    self.assertIn('ON "OB_USER"."MY_TABLE"', result)
    self.assertNotIn('"OB_USER.MY_TRIGGER"', result)  # 不应出现错误格式
    self.assertNotIn('"OB_USER.MY_TABLE"', result)
```

### 5.2 集成测试

使用真实的 TRIGGER DDL 样本，验证生成的 SQL 可以在 OceanBase 中正确执行。

---

## 6. 风险评估

| 风险项 | 级别 | 说明 |
|-------|-----|------|
| 兼容性 | 低 | 添加引号是 Oracle/OceanBase 的标准格式，不会导致兼容性问题 |
| 回归 | 中 | 需要全面测试所有触发器类型 (BEFORE/AFTER, INSERT/UPDATE/DELETE) |
| 遗漏 | 中 | 其他 DDL 生成位置可能存在类似问题，需逐一审查 |

---

## 7. 修复优先级

1. **P0 (立即修复)**: `remap_trigger_ddl` 函数中的两处问题 (行 16018, 16030)
2. **P0 (立即修复)**: `_rewrite_trigger_name_and_on` 函数中的两处问题 (行 20564, 20570)
3. **P1 (建议修复)**: FK REFERENCES 子句 (行 20406)
4. **P2 (可选)**: DROP/ALTER TABLE 语句，视实际对象名是否包含特殊字符决定

---

## 8. 其他对象类型的引号问题

### 8.1 VIEW DDL (行 15290)

**问题代码**:
```python
ddl = f"CREATE OR REPLACE VIEW {owner.upper()}.{name.upper()} AS {text.strip()}"
```

**修复方案**:
```python
ddl = f'CREATE OR REPLACE VIEW "{owner.upper()}"."{name.upper()}" AS {text.strip()}'
```

### 8.2 SYNONYM DDL (行 18456)

**问题代码**:
```python
ddl = f"CREATE OR REPLACE SYNONYM {syn_meta.owner}.{syn_name} FOR {target};"
```

**修复方案**:
```python
ddl = f'CREATE OR REPLACE SYNONYM "{syn_meta.owner}"."{syn_name}" FOR {target};'
```

### 8.3 DROP 语句 (行 17036, 17043)

**问题代码**:
```python
return f"ALTER TABLE {schema_u}.{parent_table.upper()} DROP CONSTRAINT {name_u}"
return f"DROP {obj_type_u} {schema_u}.{name_u}"
```

**修复方案**:
```python
return f'ALTER TABLE "{schema_u}"."{parent_table.upper()}" DROP CONSTRAINT "{name_u}"'
return f'DROP {obj_type_u} "{schema_u}"."{name_u}"'
```

### 8.4 ALTER TABLE 语句 (多处)

行 17803, 17845, 17857, 17878, 17893, 17899, 17919, 17949, 20362, 20404, 20425 等：

**问题代码**:
```python
f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} ADD (...)"
f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} MODIFY (...)"
```

**修复方案**:
```python
f'ALTER TABLE "{tgt_schema_u}"."{tgt_table_u}" ADD (...)'
f'ALTER TABLE "{tgt_schema_u}"."{tgt_table_u}" MODIFY (...)'
```

---

## 9. CHECK 约束重复名称问题分析

### 9.1 用户报告的问题

执行 CHECK 约束 fixup 脚本时报错：`name already used by an existing constraint`

示例 DDL：
```sql
ALTER TABLE "GBSMAN"."CHS_CLAIM_AUTO_UPLOAD_INFO"
ADD CONSTRAINT "CK_CHS_CLM_UPL_INF_IS_VALID" CHECK (IS_VALID IN('Y', 'N')) VALIDATE;
```

### 9.2 根因分析

**可能原因 1**: 约束表达式规范化差异

CHECK 约束比较使用 `expr_key` 匹配（行 12509）：
```python
expr_matches = tgt_by_expr.get(expr_key)
```

如果源端和目标端的表达式规范化结果不同（如空格、括号、大小写差异），即使约束名相同，也会被判定为"缺失"。

**可能原因 2**: 约束已存在但表达式不匹配

行 12486-12508 的逻辑：
- 如果约束名存在但表达式不一致，只记录 `detail_mismatch`
- 但约束名不会加入 `used` 集合
- 后续行 12509-12517 会尝试按表达式匹配
- 如果表达式也不匹配，约束会被加入 `missing` 集合

**可能原因 3**: 幂等模式未启用

fixup 脚本生成时调用 `apply_fixup_idempotency`（行 20469），但该函数依赖配置：
```python
mode = normalize_fixup_idempotent_mode(settings.get("fixup_idempotent_mode", "off"))
```

默认模式是 `off`，不会添加存在性检查。

### 9.3 修复建议

**方案 A**: 改进约束比较逻辑

当约束名已存在时，无论表达式是否匹配，都不应再加入 `missing` 集合：

```python
# 行 12485-12521 修改建议
for expr_key, name, raw_expr, deferrable, deferred in src_list:
    # 如果约束名已存在于目标端，标记为已匹配（不再判定为缺失）
    if name in tgt_by_name:
        used.add(name)
        tgt_expr_key, tgt_expr_raw, tgt_deferrable, tgt_deferred = tgt_by_name[name]
        if tgt_expr_key != expr_key:
            # 仅记录表达式差异，不标记为缺失
            detail_mismatch.append(...)
        continue  # 关键：不再进入后续的 missing 判定
    
    # 后续按表达式匹配逻辑...
```

**方案 B**: 启用幂等模式

在 `config.ini` 中配置：
```ini
fixup_idempotent_mode = guard
fixup_idempotent_types = TABLE,VIEW,SEQUENCE,INDEX,CONSTRAINT,TRIGGER,PROCEDURE,FUNCTION,PACKAGE,SYNONYM
```

这会在生成的 DDL 中添加存在性检查，避免重复创建。

**方案 C**: 约束 fixup 前检查目标端

在生成 CONSTRAINT fixup 脚本前，先检查目标端是否已存在同名约束。

---

## 10. 引号问题汇总表

| 位置 | 行号 | 对象类型 | 问题 | 优先级 |
|------|-----|---------|------|--------|
| `remap_trigger_ddl` | 16018 | TRIGGER 名称 | 无引号 | P0 |
| `remap_trigger_ddl` | 16030 | ON 表名 | 无引号 | P0 |
| `_rewrite_trigger_name_and_on` | 20564 | TRIGGER 名称 | 无引号 | P0 |
| `_rewrite_trigger_name_and_on` | 20570 | ON 表名 | 无引号 | P0 |
| `build_view_ddl_from_text` | 15290 | VIEW | 无引号 | P1 |
| SYNONYM DDL | 18456 | SYNONYM | 无引号 | P1 |
| `_build_drop_statement` | 17036, 17043 | DROP | 无引号 | P2 |
| ALTER TABLE 语句 | 多处 | TABLE | 无引号 | P2 |
| FK REFERENCES | 20406 | CONSTRAINT | 无引号 | P2 |

---

## 11. 总结

| 项目 | 说明 |
|-----|------|
| **引号问题数量** | 4 处 P0 + 2 处 P1 + 多处 P2 |
| **CHECK 约束问题** | 比较逻辑缺陷 + 幂等模式默认关闭 |
| **影响对象** | TRIGGER, VIEW, SYNONYM, ALTER TABLE, DROP DDL |
| **修复复杂度** | 引号问题=低；CHECK 约束=中 |
| **测试要求** | 全面回归测试所有 DDL 生成路径 |
