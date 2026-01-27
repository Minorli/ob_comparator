# Fixup 专项审查报告

**日期**: 2026-01-27  
**审查范围**: `schema_diff_reconciler.py` fixup 生成逻辑 + `run_fixup.py` 执行脚本  
**问题级别**: 高

---

## 目录

1. [DDL 引号格式问题汇总](#1-ddl-引号格式问题汇总)
2. [约束处理逻辑缺陷](#2-约束处理逻辑缺陷)
3. [fixup 生成流程问题](#3-fixup-生成流程问题)
4. [run_fixup.py 执行逻辑问题](#4-run_fixuppy-执行逻辑问题)
5. [幂等性处理问题](#5-幂等性处理问题)
6. [修复建议优先级](#6-修复建议优先级)

---

## 1. DDL 引号格式问题汇总

### 1.1 问题概述

生成的 DDL 中 `schema.object` 格式缺少正确的双引号包裹，导致执行时可能出错。

**标准格式**: `"SCHEMA"."OBJECT_NAME"`  
**错误格式**: `SCHEMA.OBJECT_NAME` 或 `"SCHEMA.OBJECT_NAME"`

### 1.2 受影响位置

| 文件 | 行号 | 对象类型 | 问题代码 | 优先级 |
|------|-----|---------|---------|--------|
| schema_diff_reconciler.py | 16018 | TRIGGER 名称 | `f"{tgt_schema_u}.{tgt_trigger_u}"` | **P0** |
| schema_diff_reconciler.py | 16030 | TRIGGER ON 表名 | `f"{on_schema_u}.{on_table_u}"` | **P0** |
| schema_diff_reconciler.py | 20564 | TRIGGER 名称 | `rf'\1{ts}.{to}'` | **P0** |
| schema_diff_reconciler.py | 20570 | TRIGGER ON 表名 | `rf'\1{tts}.{tt}'` | **P0** |
| schema_diff_reconciler.py | 15290 | VIEW | `f"CREATE OR REPLACE VIEW {owner.upper()}.{name.upper()}"` | **P1** |
| schema_diff_reconciler.py | 18456 | SYNONYM | `f"CREATE OR REPLACE SYNONYM {syn_meta.owner}.{syn_name}"` | **P1** |
| schema_diff_reconciler.py | 17036 | DROP CONSTRAINT | `f"ALTER TABLE {schema_u}.{parent_table.upper()} DROP CONSTRAINT {name_u}"` | **P2** |
| schema_diff_reconciler.py | 17043 | DROP 通用 | `f"DROP {obj_type_u} {schema_u}.{name_u}"` | **P2** |
| schema_diff_reconciler.py | 19415 | INDEX | `f"CREATE {prefix}INDEX {tgt_schema.upper()}.{idx_name.upper()} ON ..."` | **P2** |
| schema_diff_reconciler.py | 20362 | CONSTRAINT PK/UK | `f"ALTER TABLE {ts}.{tt} ADD CONSTRAINT ..."` | **P2** |
| schema_diff_reconciler.py | 20404-20406 | CONSTRAINT FK | `f"ALTER TABLE {ts}.{tt} ... REFERENCES {ref_tgt_schema}.{ref_tgt_table}"` | **P2** |
| schema_diff_reconciler.py | 20425 | CONSTRAINT CHECK | `f"ALTER TABLE {ts}.{tt} ADD CONSTRAINT {cons_name_u} CHECK ..."` | **P2** |
| schema_diff_reconciler.py | 12167, 12212 | ALTER TABLE ADD PARTITION | `f"ALTER TABLE {table_full} ADD PARTITION ..."` | **P2** |
| schema_diff_reconciler.py | 17803-17949 | ALTER TABLE MODIFY | 多处 `f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} ..."` | **P2** |

### 1.3 修复方案

**方案 A**: 引入统一辅助函数

```python
def quote_identifier(name: str) -> str:
    """为标识符添加双引号"""
    return f'"{name.upper()}"'

def quote_qualified_name(schema: str, obj: str) -> str:
    """生成带引号的全限定名"""
    return f'"{schema.upper()}"."{obj.upper()}"'
```

**方案 B**: 逐个修复

将 `{schema}.{name}` 改为 `"{schema}"."{name}"`

---

## 2. 约束处理逻辑缺陷

### 2.1 CHECK 约束重复创建问题

**问题描述**: 用户报告执行 CHECK 约束 fixup 脚本时报错 `name already used by an existing constraint`

**根因分析** (`schema_diff_reconciler.py` 行 12474-12521):

```python
def match_check_constraints(src_list, tgt_list):
    for expr_key, name, raw_expr, deferrable, deferred in src_list:
        if name in tgt_by_name:
            # 检查表达式是否一致
            if tgt_expr_key != expr_key:
                detail_mismatch.append(...)  # 仅记录差异
            # ❌ 问题: 未将 name 加入 used 集合
            # ❌ 问题: 未 continue，继续执行后续逻辑
        
        expr_matches = tgt_by_expr.get(expr_key)
        if not expr_matches:
            missing.add(name)  # ❌ 约束名已存在却被标记为缺失
```

**问题本质**: 
- 当约束名存在但表达式不完全匹配时，约束被错误标记为"缺失"
- 生成的 fixup 脚本尝试 ADD CONSTRAINT，但约束名已存在

### 2.2 修复方案

```python
def match_check_constraints(src_list, tgt_list):
    for expr_key, name, raw_expr, deferrable, deferred in src_list:
        if name in tgt_by_name:
            used.add(name)  # ✅ 标记为已匹配
            tgt_expr_key, tgt_expr_raw, tgt_deferrable, tgt_deferred = tgt_by_name[name]
            if tgt_expr_key != expr_key:
                detail_mismatch.append(...)  # 记录表达式差异
            continue  # ✅ 不再判定为缺失
        
        # 后续按表达式匹配逻辑...
```

---

## 3. fixup 生成流程问题

### 3.1 VIEW DDL 来源不一致

**位置**: `schema_diff_reconciler.py` 行 15283-15298 `build_view_ddl_from_text`

**问题**: 此函数仅从 `TEXT` 列构建 VIEW DDL，不包含列定义。当 VIEW 有显式列名时可能生成不完整的 DDL。

**示例**:
```sql
-- 源端 VIEW
CREATE VIEW SCOTT.MY_VIEW (COL1, COL2) AS SELECT A, B FROM T;

-- 生成的 DDL (缺少列定义)
CREATE OR REPLACE VIEW SCOTT.MY_VIEW AS SELECT A, B FROM T;
```

### 3.2 SYNONYM 目标解析问题

**位置**: `schema_diff_reconciler.py` 行 18451-18456

**问题**: SYNONYM 的 `target` 未正确添加引号

```python
# 当前代码
ddl = f"CREATE OR REPLACE SYNONYM {syn_meta.owner}.{syn_name} FOR {target};"

# target 可能是 SCHEMA.TABLE，也需要引号
```

### 3.3 INDEX DDL 缺少引号

**位置**: `schema_diff_reconciler.py` 行 19415

```python
# 当前代码
return f"CREATE {prefix}INDEX {tgt_schema.upper()}.{idx_name.upper()} ON {tgt_schema.upper()}.{tgt_table.upper()} ({col_list});"

# 多处 schema.name 均缺少引号
```

### 3.4 分区 DDL 缺少引号

**位置**: `schema_diff_reconciler.py` 行 12167, 12212

```python
# 当前代码
f"ALTER TABLE {table_full} ADD PARTITION {part_name} VALUES LESS THAN ({boundary_expr});"

# table_full 和 part_name 缺少引号
```

---

## 4. run_fixup.py 执行逻辑问题

### 4.1 执行顺序设计

**当前实现** (行 335-349):

```python
DEPENDENCY_LAYERS = [
    ["sequence"],                    # Layer 0
    ["table"],                       # Layer 1
    ["table_alter"],                 # Layer 2
    ["grants"],                      # Layer 3: ✓ 正确放在依赖对象之前
    ["view", "synonym"],             # Layer 4
    ["materialized_view"],           # Layer 5
    ["type"],                        # Layer 6
    ["package"],                     # Layer 7
    ["procedure", "function"],       # Layer 8
    ["type_body", "package_body"],   # Layer 9
    ["constraint", "index"],         # Layer 10
    ["trigger"],                     # Layer 11
    ["job", "schedule"],             # Layer 12
]
```

**评价**: 执行顺序设计合理，GRANT 在 VIEW 之前执行。

### 4.2 错误分类与重试逻辑

**位置**: 行 154-210 `classify_sql_error`

**问题**: 错误分类逻辑完善，但部分错误码缺失

**建议补充**:
```python
# 约束已存在
if 'ORA-02264' in stderr_upper or 'CONSTRAINT ALREADY EXISTS' in stderr_upper:
    return FailureType.DUPLICATE_OBJECT

# 对象不存在（用于 DROP）
if 'ORA-02289' in stderr_upper:  # SEQUENCE does not exist
    return FailureType.MISSING_OBJECT
```

### 4.3 SQL 语句分割问题

**位置**: 行 1846-1944 `split_sql_statements`

**潜在问题**: 
- Q-Quote 处理可能不完整
- 嵌套 PL/SQL 块的 `/` 终结符处理复杂

**建议**: 增加边界测试用例覆盖

### 4.4 自动授权回退逻辑

**位置**: 行 2159-2173 `build_auto_grant_statement`

**问题**: 生成的 GRANT 语句缺少引号

```python
# 当前代码
return f"GRANT {priv_u} ON {object_u} TO {grantee_u}{suffix};"

# object_u 格式为 SCHEMA.NAME，缺少引号
# 应为: GRANT SELECT ON "SCHEMA"."NAME" TO GRANTEE;
```

---

## 5. 幂等性处理问题

### 5.1 默认模式为 off

**位置**: `schema_diff_reconciler.py` 行 17112-17115

```python
mode = normalize_fixup_idempotent_mode(settings.get("fixup_idempotent_mode", "off"))
if mode == "off" or obj_type_u not in types_set:
    return ddl
```

**问题**: 默认不启用幂等性，导致重复执行 fixup 脚本时报错

### 5.2 CONSTRAINT 幂等性不完整

**位置**: 行 17127-17134

当 `mode == "guard"` 时，CONSTRAINT 类型会生成存在性检查块，但 `_build_exist_check_sql` 检查的是 `ALL_CONSTRAINTS.CONSTRAINT_NAME`，不验证约束所属表。

**问题场景**: 如果同名约束存在于不同表，guard 逻辑会错误跳过。

### 5.3 修复建议

1. 默认启用 `fixup_idempotent_mode = guard`
2. CONSTRAINT 存在性检查增加 TABLE_NAME 条件:
```python
if obj_type_u == "CONSTRAINT":
    return (
        f"SELECT COUNT(*) INTO v_count FROM ALL_CONSTRAINTS "
        f"WHERE OWNER='{schema_u}' AND CONSTRAINT_NAME='{name_u}' "
        f"AND TABLE_NAME='{table_u}'"
    )
```

---

## 6. 修复建议优先级

### P0 - 立即修复

| 问题 | 位置 | 影响 |
|-----|------|-----|
| TRIGGER DDL 引号 | 16018, 16030, 20564, 20570 | 触发器无法创建 |
| CHECK 约束重复判定 | 12486-12521 | 约束 fixup 执行失败 |

### P1 - 高优先级

| 问题 | 位置 | 影响 |
|-----|------|-----|
| VIEW DDL 引号 | 15290 | 视图创建失败 |
| SYNONYM DDL 引号 | 18456 | 同义词创建失败 |
| 幂等模式默认关闭 | 17112 | 重复执行报错 |
| 自动授权缺少引号 | run_fixup.py:2173 | GRANT 执行失败 |

### P2 - 建议修复

| 问题 | 位置 | 影响 |
|-----|------|-----|
| INDEX DDL 引号 | 19415 | 索引创建可能失败 |
| CONSTRAINT DDL 引号 | 20362, 20404, 20425 | 约束创建可能失败 |
| ALTER TABLE 引号 | 17803-17949, 12167 | DDL 执行可能失败 |
| DROP 语句引号 | 17036, 17043 | DROP 执行可能失败 |
| 错误码分类补充 | run_fixup.py:168-210 | 错误分析不完整 |

### P3 - 可选优化

| 问题 | 位置 | 影响 |
|-----|------|-----|
| VIEW 列定义丢失 | 15283-15298 | 边缘情况 DDL 不完整 |
| CONSTRAINT 幂等检查不含表名 | 17011-17012 | 同名约束误判 |
| SQL 分割边界情况 | run_fixup.py:1846-1944 | 复杂 PL/SQL 可能分割错误 |

---

## 7. 测试建议

### 7.1 单元测试用例

```python
def test_ddl_quoting():
    """测试所有 DDL 生成函数的引号格式"""
    # TRIGGER
    assert '"SCHEMA"."TRIGGER_NAME"' in generated_trigger_ddl
    assert 'ON "SCHEMA"."TABLE_NAME"' in generated_trigger_ddl
    
    # VIEW
    assert 'CREATE OR REPLACE VIEW "SCHEMA"."VIEW_NAME"' in generated_view_ddl
    
    # INDEX
    assert 'CREATE INDEX "SCHEMA"."IDX_NAME" ON "SCHEMA"."TABLE"' in generated_index_ddl
    
    # CONSTRAINT
    assert 'ALTER TABLE "SCHEMA"."TABLE" ADD CONSTRAINT' in generated_constraint_ddl

def test_check_constraint_matching():
    """测试 CHECK 约束匹配逻辑"""
    # 约束名已存在时不应标记为缺失
    src_constraints = [("expr1", "CK_TEST", "col > 0", "", "")]
    tgt_constraints = [("expr1_diff", "CK_TEST", "col >= 0", "", "")]  # 表达式略有不同
    
    result = match_check_constraints(src_constraints, tgt_constraints)
    assert "CK_TEST" not in result.missing  # 不应在 missing 集合中
```

### 7.2 集成测试

1. 生成 fixup 脚本后验证 DDL 语法正确性
2. 使用 OceanBase 的 `EXPLAIN` 或语法解析验证
3. 测试幂等模式下重复执行 fixup 脚本

---

## 8. 总结

| 类别 | 问题数量 | 备注 |
|-----|---------|------|
| **DDL 引号问题** | 14+ 处 | 影响所有对象类型 |
| **约束逻辑缺陷** | 1 处核心 | CHECK 约束重复判定 |
| **幂等性问题** | 2 处 | 默认关闭 + CONSTRAINT 检查不完整 |
| **run_fixup.py** | 2 处 | 自动授权引号 + 错误码补充 |

**建议修复顺序**: P0 → P1 → P2 → P3

**预计工作量**: 
- P0: 0.5 人日
- P1: 1 人日
- P2: 1.5 人日
- P3: 1 人日
