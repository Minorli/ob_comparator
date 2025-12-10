# 对象推导逻辑审核报告

## 审核日期
2025-12-10

## 审核目标
验证程序是否实现了"所有对象推导时跟着父表走"的逻辑，包括同义词、视图、触发器、包、包体、存储过程等所有对象类型。

---

## 核心推导函数：`resolve_remap_target()`

### 推导优先级（按顺序）

```python
def resolve_remap_target(
    src_name: str,
    obj_type: str,
    remap_rules: RemapRules,
    schema_mapping: Optional[Dict[str, str]] = None,
    object_parent_map: Optional[ObjectParentMap] = None,
    source_dependencies: Optional[Set[Tuple[str, str, str, str]]] = None
) -> Optional[str]:
```

#### 1. 显式 Remap 规则（最高优先级）
```python
# 第1步：查找 remap_rules 中的显式规则
if key in remap_rules:
    return remap_rules[key]
```
- ✅ 如果用户在 `remap_rules.txt` 中显式指定了对象的映射，直接使用
- ✅ 支持 PACKAGE BODY 的特殊语法（`PACKAGE_NAME BODY = ...`）

#### 2. 依附对象跟随父表（仅限特定类型）
```python
# 第2步：对于依附对象（TRIGGER/INDEX/CONSTRAINT/SEQUENCE），使用父表的 remap 目标 schema
if '.' in src_name and object_parent_map:
    parent_table = object_parent_map.get(src_name.upper())
    if parent_table:
        parent_target = remap_rules.get(parent_table.upper())
        if parent_target:
            tgt_schema = parent_target.split('.', 1)[0].upper()
            src_obj = src_name.split('.', 1)[1]
            return f"{tgt_schema}.{src_obj}"
```
- ⚠️ **问题：仅处理 `object_parent_map` 中的对象**
- ⚠️ **`object_parent_map` 只包含 TRIGGER**（见下文）

#### 3. 基于依赖分析推导（独立对象）
```python
# 第3步：对于独立对象（VIEW/PROCEDURE/FUNCTION/PACKAGE），尝试基于依赖分析推导
if '.' in src_name and obj_type_u != 'TABLE':
    if source_dependencies:
        inferred = infer_target_schema_from_dependencies(
            src_name, obj_type, remap_rules, source_dependencies
        )
        if inferred:
            return inferred
```
- ✅ 分析对象依赖的所有表
- ✅ 统计这些表被 remap 到哪些目标 schema
- ✅ 选择出现次数最多的目标 schema
- ✅ **这是"跟着父表走"的核心实现**

#### 4. Schema 映射推导（回退方案）
```python
# 第4步：回退到schema映射推导（适用于多对一、一对一场景）
src_schema, src_obj = src_name.split('.', 1)
if schema_mapping:
    tgt_schema = schema_mapping.get(src_schema_u)
    if tgt_schema:
        return f"{tgt_schema}.{src_obj}"
```
- ✅ 基于 TABLE 的 schema 映射关系推导
- ✅ 适用于多对一、一对一场景

---

## 关键问题分析

### 问题1：`object_parent_map` 覆盖不全

**当前实现：**
```python
def get_object_parent_tables(ora_cfg: OraConfig, schemas_list: List[str]) -> ObjectParentMap:
    """
    获取依附对象（TRIGGER 等）所属的父表。
    """
    parent_map: ObjectParentMap = {}
    
    # 只查询 TRIGGER
    cursor.execute(f"""
        SELECT OWNER, TRIGGER_NAME, TABLE_OWNER, TABLE_NAME
        FROM DBA_TRIGGERS
        WHERE OWNER IN ({placeholders})
          AND TABLE_NAME IS NOT NULL
          AND BASE_OBJECT_TYPE IN ('TABLE', 'VIEW')
    """, schemas_list)
    
    # 没有查询其他对象类型！
    return parent_map
```

**问题：**
- ❌ 只包含 TRIGGER 的父表映射
- ❌ 不包含 VIEW、SYNONYM、PROCEDURE、FUNCTION、PACKAGE、PACKAGE BODY 等
- ❌ 这些对象无法通过第2步（依附对象跟随父表）推导

**影响：**
- VIEW、SYNONYM、PROCEDURE、FUNCTION、PACKAGE、PACKAGE BODY 等对象
- 必须依赖第3步（依赖分析）或第4步（schema映射）
- 如果依赖分析失败（如对象不依赖任何表），则无法推导

---

### 问题2：依赖分析的局限性

**`infer_target_schema_from_dependencies()` 的逻辑：**
```python
# 查找该对象依赖的所有表
for dep_owner, dep_name, dep_type, ref_owner, ref_name, ref_type in source_dependencies:
    if dep_full == src_name_u and ref_type_u == 'TABLE':
        referenced_tables.append(ref_full)

if not referenced_tables:
    return None  # 如果对象不依赖任何表，无法推导！
```

**问题场景：**
1. **SYNONYM 指向另一个 SYNONYM**
   - 不依赖 TABLE，依赖分析失败
   - 回退到 schema 映射

2. **PROCEDURE 只调用其他 PROCEDURE**
   - 不依赖 TABLE，依赖分析失败
   - 回退到 schema 映射

3. **PACKAGE 只包含类型定义**
   - 不依赖 TABLE，依赖分析失败
   - 回退到 schema 映射

4. **VIEW 引用多个 schema 的表且次数相同**
   ```python
   # 多个schema的引用次数相同，无法推导
   if len(candidate_schemas) > 1:
       return None
   ```

---

## 各对象类型的推导路径分析

### ✅ TABLE
- 路径：显式 remap 规则 → 保持原名
- 结论：**正常**（TABLE 是基础，必须显式指定）

### ⚠️ TRIGGER
- 路径：显式 remap → **依附对象跟随父表** → 依赖分析 → schema 映射
- 结论：**部分正常**（通过 `object_parent_map` 跟随父表）

### ⚠️ INDEX / CONSTRAINT / SEQUENCE
- 路径：显式 remap → ~~依附对象跟随父表~~（不在 `object_parent_map` 中）→ 依赖分析 → schema 映射
- 结论：**有问题**（不在 `object_parent_map` 中，无法通过第2步推导）
- 实际：依赖第3步（依赖分析）或第4步（schema 映射）

### ⚠️ VIEW
- 路径：显式 remap → ~~依附对象跟随父表~~（不在 `object_parent_map` 中）→ **依赖分析** → schema 映射
- 结论：**基本正常**（通过依赖分析跟随引用的表）
- 风险：如果 VIEW 不引用任何表，或引用多个 schema 的表且次数相同，推导失败

### ⚠️ SYNONYM
- 路径：显式 remap → ~~依附对象跟随父表~~（不在 `object_parent_map` 中）→ 依赖分析 → schema 映射
- 结论：**有问题**
- 问题：SYNONYM 的依赖关系可能指向非 TABLE 对象（如另一个 SYNONYM），依赖分析失败

### ⚠️ PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY
- 路径：显式 remap → ~~依附对象跟随父表~~（不在 `object_parent_map` 中）→ **依赖分析** → schema 映射
- 结论：**基本正常**（通过依赖分析跟随引用的表）
- 风险：如果代码中不引用任何表（只调用其他过程/函数），推导失败

### ⚠️ TYPE / TYPE BODY
- 路径：显式 remap → ~~依附对象跟随父表~~（不在 `object_parent_map` 中）→ 依赖分析 → schema 映射
- 结论：**有问题**
- 问题：TYPE 通常不依赖 TABLE，依赖分析失败

---

## 总结

### 当前实现的"跟着父表走"逻辑

#### ✅ 已实现（通过依赖分析）
- VIEW（如果引用表）
- PROCEDURE / FUNCTION（如果引用表）
- PACKAGE / PACKAGE BODY（如果引用表）
- TRIGGER（通过 `object_parent_map` + 依赖分析双重保障）

#### ⚠️ 部分实现（依赖 schema 映射回退）
- INDEX / CONSTRAINT / SEQUENCE（不在 `object_parent_map` 中）
- SYNONYM（可能不依赖 TABLE）
- TYPE / TYPE BODY（通常不依赖 TABLE）

#### ❌ 未实现的场景
1. **对象不依赖任何表**
   - 例如：只包含类型定义的 PACKAGE
   - 例如：指向 SYNONYM 的 SYNONYM
   - 回退到 schema 映射（可能不准确）

2. **对象依赖多个 schema 的表且次数相同**
   - 例如：VIEW 引用 SCHEMA_A.T1 和 SCHEMA_B.T2 各1次
   - 无法推导，回退到 schema 映射

3. **依附对象（INDEX/CONSTRAINT/SEQUENCE）**
   - 不在 `object_parent_map` 中
   - 依赖分析可能失败（INDEX/CONSTRAINT 本身不在 DBA_DEPENDENCIES 中）
   - 回退到 schema 映射

---

## 建议改进方案

### 方案1：扩展 `object_parent_map`（推荐）

**目标：** 让所有对象都能通过第2步（依附对象跟随父表）推导

**实现：**
```python
def get_object_parent_tables(ora_cfg: OraConfig, schemas_list: List[str]) -> ObjectParentMap:
    parent_map: ObjectParentMap = {}
    
    # 1. TRIGGER -> TABLE
    # （已实现）
    
    # 2. INDEX -> TABLE
    cursor.execute(f"""
        SELECT OWNER, INDEX_NAME, TABLE_OWNER, TABLE_NAME
        FROM DBA_INDEXES
        WHERE OWNER IN ({placeholders})
    """)
    
    # 3. CONSTRAINT -> TABLE
    cursor.execute(f"""
        SELECT OWNER, CONSTRAINT_NAME, TABLE_NAME
        FROM DBA_CONSTRAINTS
        WHERE OWNER IN ({placeholders})
    """)
    
    # 4. SEQUENCE -> TABLE（通过命名约定或依赖分析）
    # 例如：SEQ_TABLENAME -> TABLENAME
    
    # 5. VIEW -> 主要引用的表（通过依赖分析）
    # 6. SYNONYM -> 指向的对象（通过 DBA_SYNONYMS）
    # 7. PROCEDURE/FUNCTION/PACKAGE -> 主要引用的表（通过依赖分析）
    
    return parent_map
```

**优点：**
- ✅ 所有对象都能通过第2步推导
- ✅ 不依赖依赖分析的复杂逻辑
- ✅ 推导结果更确定

**缺点：**
- ❌ 需要大量查询
- ❌ 对于 VIEW/PROCEDURE 等，"主要引用的表"定义不明确

---

### 方案2：增强依赖分析（当前方案）

**目标：** 改进第3步（依赖分析），处理更多场景

**实现：**
```python
def infer_target_schema_from_dependencies(...):
    # 当前：只查找依赖的 TABLE
    if dep_full == src_name_u and ref_type_u == 'TABLE':
        referenced_tables.append(ref_full)
    
    # 改进：如果没有依赖 TABLE，查找依赖的其他对象
    if not referenced_tables:
        for dep_owner, dep_name, dep_type, ref_owner, ref_name, ref_type in source_dependencies:
            if dep_full == src_name_u:
                # 递归查找被依赖对象的 schema
                ref_full = f"{ref_owner}.{ref_name}"
                ref_target = remap_rules.get(ref_full)
                if ref_target:
                    # 使用被依赖对象的目标 schema
                    ...
```

**优点：**
- ✅ 不需要修改 `object_parent_map`
- ✅ 利用现有的依赖关系数据

**缺点：**
- ❌ 逻辑复杂，可能递归
- ❌ 性能问题

---

### 方案3：强制 schema 映射（最简单）

**目标：** 让第4步（schema 映射）成为主要推导方式

**实现：**
- 确保 `schema_mapping` 总是有值
- 基于 TABLE 的 remap 规则自动构建 schema 映射
- 所有非 TABLE 对象默认使用 schema 映射

**优点：**
- ✅ 实现简单
- ✅ 适用于多对一、一对一场景

**缺点：**
- ❌ 不适用于一对多场景（如 MONSTER_A → TITAN_A + TITAN_B）
- ❌ 无法处理同一 schema 内的对象需要分散到不同目标 schema 的情况

---

## 结论

### 当前实现评估

**符合"跟着父表走"的程度：** ⭐⭐⭐☆☆ (3/5)

**优点：**
- ✅ 核心逻辑正确：通过依赖分析让对象跟随引用的表
- ✅ 支持一对多场景（通过依赖分析）
- ✅ 有多层回退机制（依赖分析 → schema 映射）

**缺点：**
- ❌ `object_parent_map` 只包含 TRIGGER，覆盖不全
- ❌ 依赖分析有局限性（对象必须依赖 TABLE）
- ❌ 某些对象类型（SYNONYM、TYPE）可能无法正确推导

**建议：**
1. **短期：** 扩展 `object_parent_map` 包含 INDEX、CONSTRAINT
2. **中期：** 增强依赖分析，支持递归查找
3. **长期：** 考虑让用户在 remap_rules.txt 中显式指定所有需要特殊处理的对象

---

## 测试建议

### 测试场景1：VIEW 跟随表
```
TABLE: SCHEMA_A.T1 -> SCHEMA_B.T1
VIEW:  SCHEMA_A.V1 (引用 T1) -> 应推导为 SCHEMA_B.V1
```

### 测试场景2：PROCEDURE 跟随表
```
TABLE:     SCHEMA_A.T1 -> SCHEMA_B.T1
PROCEDURE: SCHEMA_A.P1 (引用 T1) -> 应推导为 SCHEMA_B.P1
```

### 测试场景3：SYNONYM 跟随表
```
TABLE:   SCHEMA_A.T1 -> SCHEMA_B.T1
SYNONYM: SCHEMA_A.SYN1 (指向 T1) -> 应推导为 SCHEMA_B.SYN1
```

### 测试场景4：一对多场景
```
TABLE: MONSTER_A.DUNGEONS -> TITAN_A.DUNGEON_INFO
TABLE: MONSTER_A.LAIRS -> TITAN_B.LAIR_INFO
VIEW:  MONSTER_A.VW_LAIR_RICHNESS (引用 LAIRS) -> 应推导为 TITAN_B.VW_LAIR_RICHNESS
```

### 测试场景5：不依赖表的对象
```
TABLE:   SCHEMA_A.T1 -> SCHEMA_B.T1
PACKAGE: SCHEMA_A.PKG1 (只包含类型定义，不引用表) -> 推导结果？
```

---

## 审核人
OceanBase Migration Team

## 审核日期
2025-12-10
