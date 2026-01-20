# Oracle to OceanBase 迁移工具代码审查报告

**审查日期**: 2026-01-20  
**代码版本**: V0.9.8 (git commit: 21f200c)  
**审查范围**: schema_diff_reconciler.py  
**审查视角**: 校验逻辑 + Fixup DDL生成 + 公共同义词处理

---

## 一、工具概述理解

### 1.1 业务场景
- **约8000张表** 需要 remap 到新用户下
- **剩余表** 保持 1:1 迁移
- **触发器/序列** 保持 1:1 迁移
- **其他依赖表的对象** 需要跟随 remap 关系

### 1.2 核心工作流程
```
1. 加载配置 → 2. 加载Remap规则 → 3. 收集源端元数据
4. 收集目标端元数据 → 5. 生成映射 → 6. 校验对象
7. 生成Fixup DDL → 8. 输出报告
```

---

## 二、Remap映射逻辑审查

### ✅ 已正确实现的功能

| 功能 | 代码位置 | 说明 |
|-----|---------|------|
| Remap规则加载 | `load_remap_rules()` @2917 | 支持 `SRC.OBJ=TGT.OBJ` 格式 |
| 规则验证 | `validate_remap_rules()` @4039 | 检测无效源对象并分离 |
| Schema级映射推导 | `derive_schema_mapping_from_rules()` @4111 | 从表映射推导schema映射 |
| 主导Schema推导 | `infer_dominant_schema_from_rules()` @4132 | 统计最高频目标schema |
| 依附对象跟随父表 | `resolve_remap_target()` @4573 | INDEX/CONSTRAINT/SEQUENCE跟随父表schema |
| 多对一检测 | `generate_master_list()` @4787 | 检测并回退冲突映射 |

### ✅ Remap逻辑正确性确认

```python
# @4656-4695: 依附对象正确跟随父表schema
if '.' in src_name and object_parent_map and obj_type_u in ('INDEX', 'CONSTRAINT', 'SEQUENCE'):
    parent_table = object_parent_map.get(src_name.upper())
    if parent_table:
        parent_target = remap_rules.get(parent_table.upper())
        # ... 正确使用父表的目标schema
```

### ⚠️ 潜在问题

| 编号 | 问题 | 位置 | 影响 | 优先级 |
|-----|------|-----|------|--------|
| R1 | TRIGGER不跟随父表remap | @4656 | 触发器schema与父表不一致时DDL引用错误 | P1 |
| R2 | 1:1表未显式加入remap_rules | 设计层面 | 依赖推导时可能找不到映射 | P2 |

#### R1 详情：TRIGGER不在跟随列表中
```python
# @4658: 仅 INDEX/CONSTRAINT/SEQUENCE 跟随父表
if obj_type_u in ('INDEX', 'CONSTRAINT', 'SEQUENCE'):  # ❌ 不含 TRIGGER
```

**当前行为**：触发器保持1:1映射，但其DDL中引用的表可能已remap

**实际处理**：代码在DDL生成阶段通过 `remap_trigger_table_references()` 和 `remap_trigger_object_references()` 处理了表引用替换，所以这个设计是**故意的**，不是缺陷。

---

## 三、校验逻辑审查

### ✅ 已正确实现的校验

| 校验类型 | 代码位置 | 说明 |
|---------|---------|------|
| TABLE存在性 | `check_primary_objects()` @9132-9137 | 检查目标端TABLE是否存在 |
| 列名集合对比 | @9154-9164 | 源端列 vs 目标端列 (忽略OMS_*) |
| VARCHAR长度校验 | @9173-9260 | 检查 ≥ src*1.5 向上取整 |
| LONG→CLOB映射 | @9180+ | 类型迁移验证 |
| VIRTUAL列表达式 | @9182-9208 | 虚拟列表达式对比 |
| VIEW存在性 | @9273-9286 | 检查目标端VIEW是否存在 |
| INDEX对比 | `compare_indexes_for_table()` @9826 | 列组合+唯一性 |
| CONSTRAINT对比 | `compare_constraints_for_table()` @10224 | PK/UK/FK/CHECK |
| TRIGGER对比 | `compare_triggers_for_table()` @10616 | 存在性+状态 |

### ⚠️ 校验逻辑潜在问题

| 编号 | 问题 | 位置 | 影响 | 优先级 |
|-----|------|-----|------|--------|
| V1 | NUMBER精度/标度未对比 | 类型对比逻辑 | 数值截断风险 | P0 |
| V2 | SEQUENCE属性未详细对比 | 仅存在性检查 | 序列行为不一致 | P1 |
| V3 | 约束表达式未语义对比 | CHECK约束 | 约束行为不一致 | P1 |

---

## 四、Fixup DDL生成审查

### ✅ 已实现的DDL生成类型

| 对象类型 | 生成方式 | 代码位置 |
|---------|---------|---------|
| SEQUENCE | DBMS_METADATA + adjust | @16820-16870 |
| TABLE | DBMS_METADATA/dbcat + adjust | @16880-17050 |
| VIEW | DBMS_METADATA + remap_view_dependencies | @17088-17265 |
| PROCEDURE/FUNCTION | dbcat + remap_plsql_object_references | @17355-17500 |
| PACKAGE/PACKAGE BODY | dbcat + 排序 | @17355-17500 |
| SYNONYM | 元数据构建 or dbcat | @15871-15876 |
| INDEX | 从TABLE DDL提取 | @17543-17610 |
| CONSTRAINT | 从TABLE DDL提取 | @17612-17760 |
| TRIGGER | dbcat + remap_trigger_object_references | @17800-17900 |

### ✅ DDL处理管道

```
原始DDL → clean_view_ddl_for_oceanbase() → sanitize_view_ddl()
        → remap_view_dependencies() → adjust_ddl_for_object()
        → cleanup_dbcat_wrappers() → prepend_set_schema()
        → normalize_ddl_for_ob() → apply_hint_filter()
        → apply_ddl_cleanup_rules() → enforce_schema_for_ddl()
```

### ❌ DDL生成遗漏项

| 编号 | 遗漏项 | 影响 | 优先级 | 建议 |
|-----|--------|------|--------|------|
| D1 | **视图中公共同义词未替换为schema.object** | DDL执行时无法解析 | **P0** | 详见第五节 |
| D2 | IDENTITY列未识别转换 | Oracle 12c+表迁移失败 | P1 | 检测IDENTITY_COLUMN并生成SEQUENCE+TRIGGER |
| D3 | DDL缺乏幂等性 | 脚本无法重复执行 | P1 | 添加IF NOT EXISTS或PL/SQL包装 |
| D4 | INVISIBLE列未处理 | 列可见性不一致 | P2 | 检测HIDDEN_COLUMN='YES'情况 |
| D5 | DEFAULT ON NULL未识别 | 默认值行为不一致 | P2 | Oracle 12c+特性检测 |
| D6 | PARALLEL/COMPRESS被丢弃 | 性能参数丢失 | P2 | 保留并生成注释提示 |

---

## 五、视图中公共同义词处理（关键缺陷）

### 问题描述

当VIEW的DDL中引用了**公共同义词(PUBLIC SYNONYM)**时，当前代码**不会**将其替换为实际的 `schema.object` 形式。

### 当前代码逻辑分析

```python
# @12845-12903: remap_view_dependencies()
def remap_view_dependencies(ddl, view_schema, remap_rules, full_object_mapping):
    dependencies = extract_view_dependencies(ddl, default_schema=view_schema)
    
    for dep in dependencies:
        # 问题在这里：只查找 full_object_mapping 和 remap_rules
        tgt_name = find_mapped_target_any_type(
            full_object_mapping,
            dep,  # 如 "DBMS_OUTPUT" (无schema前缀)
            preferred_types=("TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM", "FUNCTION")
        ) or remap_rules.get(dep)
        
        # 如果 dep 是公共同义词的裸名引用，且不在mapping中
        # tgt_name 将为 None，不会被替换！
```

### 问题场景示例

```sql
-- Oracle源端VIEW定义
CREATE VIEW SCHEMA_A.MY_VIEW AS
SELECT * FROM MY_TABLE       -- 引用公共同义词 MY_TABLE
WHERE ID IN (SELECT ID FROM DUAL);

-- PUBLIC SYNONYM定义
CREATE PUBLIC SYNONYM MY_TABLE FOR SCHEMA_B.REAL_TABLE;

-- 当前生成的DDL（错误）
CREATE VIEW NEW_SCHEMA.MY_VIEW AS
SELECT * FROM MY_TABLE       -- ❌ 公共同义词在OB可能不存在或指向不同对象
WHERE ID IN (SELECT ID FROM DUAL);

-- 正确应该生成
CREATE VIEW NEW_SCHEMA.MY_VIEW AS
SELECT * FROM SCHEMA_B.REAL_TABLE  -- ✅ 替换为实际schema.object
WHERE ID IN (SELECT ID FROM DUAL);
```

### 根因分析

1. `extract_view_dependencies()` 会提取裸名引用（无schema前缀）
2. 裸名会被补全为 `view_schema.裸名`（如 `SCHEMA_A.MY_TABLE`）
3. 但实际上 `MY_TABLE` 是PUBLIC SYNONYM，不是 `SCHEMA_A.MY_TABLE`
4. `full_object_mapping` 中不存在 `SCHEMA_A.MY_TABLE` 的映射
5. 结果：裸名引用未被替换

### 解决方案设计

```python
def remap_view_dependencies(ddl, view_schema, remap_rules, full_object_mapping,
                            synonym_meta=None):  # 新增参数
    dependencies = extract_view_dependencies(ddl, default_schema=view_schema)
    
    for dep in dependencies:
        dep_u = dep.upper()
        
        # 1. 先尝试常规映射
        tgt_name = find_mapped_target_any_type(full_object_mapping, dep, ...)
        
        # 2. 如果未找到，检查是否为公共同义词
        if not tgt_name and synonym_meta:
            # 检查裸名是否为PUBLIC同义词
            if '.' not in dep:
                pub_key = ('PUBLIC', dep_u)
                syn_info = synonym_meta.get(pub_key)
                if syn_info and syn_info.table_owner and syn_info.table_name:
                    # 解析到实际对象
                    real_full = f"{syn_info.table_owner}.{syn_info.table_name}"
                    # 再查找remap后的目标
                    tgt_name = find_mapped_target_any_type(
                        full_object_mapping, real_full, ...
                    ) or remap_rules.get(real_full) or real_full
            else:
                # 带schema的引用，检查是否为普通同义词
                owner, name = dep_u.split('.', 1)
                syn_key = (owner, name)
                syn_info = synonym_meta.get(syn_key)
                if syn_info and syn_info.table_owner and syn_info.table_name:
                    real_full = f"{syn_info.table_owner}.{syn_info.table_name}"
                    tgt_name = find_mapped_target_any_type(...) or real_full
        
        if tgt_name:
            replacements[dep_u] = tgt_name.upper()
```

### 修复优先级：**P0（必须修复）**

---

## 六、逻辑重复/冲突/缺陷识别

### 6.1 逻辑重复

| 编号 | 位置 | 说明 | 建议 |
|-----|------|------|------|
| LR1 | @4629-4654 | PUBLIC SYNONYM判断出现两次 | 合并为一个分支 |
| LR2 | @12888-12901 | 替换逻辑与adjust_ddl_for_object重复 | 考虑复用 |

#### LR1 详情
```python
# @4629-4632
if obj_type_u == 'SYNONYM' and '.' in src_name:
    src_schema, src_obj = src_name.split('.', 1)
    if src_schema.upper() == 'PUBLIC':
        return f"PUBLIC.{src_obj.upper()}"

# @4634-4654 (紧接着又处理SYNONYM)
if obj_type_u == 'SYNONYM' and '.' in src_name:
    src_schema, src_obj = src_name.split('.', 1)
    inferred_schema, conflict = infer_target_schema_from_direct_dependencies(...)
```

### 6.2 逻辑冲突

| 编号 | 位置 | 说明 | 影响 |
|-----|------|------|------|
| LC1 | 无明显冲突 | - | - |

### 6.3 逻辑缺陷

| 编号 | 位置 | 说明 | 优先级 |
|-----|------|------|--------|
| LD1 | `remap_view_dependencies()` | 不解析公共同义词 | **P0** |
| LD2 | `extract_view_dependencies()` | 子查询中的表引用可能漏提取 | P2 |
| LD3 | `adjust_ddl_for_object()` | 正则替换可能误伤变量名 | P2 |

#### LD2 详情
```python
# @12812-12816
for part in parts:
    part = part.strip()
    if not part or part.startswith('('):  # 跳过子查询
        continue
```
这会跳过 `(SELECT ... FROM TABLE_X)` 中的 `TABLE_X`。

#### LD3 详情
`replace_unqualified_identifier()` 使用上下文关键词判断，但复杂PL/SQL中可能存在同名变量被误替换的风险。

---

## 七、关键风险总结

### P0 级（必须修复）

| 编号 | 风险 | 根因 | 影响范围 |
|-----|------|------|---------|
| **P0-1** | 视图中公共同义词未替换 | `remap_view_dependencies()`不查询synonym_meta | 所有引用PUBLIC SYNONYM的VIEW |
| **P0-2** | NUMBER精度未对比 | 类型对比逻辑缺失 | 所有NUMBER列 |
| **P0-3** | LONG表被错误当作黑名单阻断依赖对象 | 未区分"黑名单"与"特殊规则" | 所有依赖LONG表的VIEW/TRIGGER/SYNONYM等 |

### P1 级（应在2周内修复）

| 编号 | 风险 | 根因 |
|-----|------|------|
| P1-1 | IDENTITY列未识别 | 元数据未收集 |
| P1-2 | DDL缺乏幂等性 | 未加IF EXISTS |
| P1-3 | SEQUENCE属性未详细对比 | 仅存在性检测 |
| P1-4 | CHECK约束表达式未语义对比 | 未实现 |

---

## 八、修复建议

### 8.1 公共同义词处理修复（P0-1）

**修改文件**: `schema_diff_reconciler.py`

**修改函数**: `remap_view_dependencies()`

**步骤**:
1. 添加 `synonym_meta` 参数
2. 对于未找到映射的引用，查询 `synonym_meta` 解析实际对象
3. 在调用处传入 `synonym_metadata`

### 8.2 NUMBER精度对比修复（P0-2）

在 `check_primary_objects()` 的列对比逻辑中添加：
```python
if src_dtype == 'NUMBER':
    src_precision = src_info.get('data_precision')
    src_scale = src_info.get('data_scale')
    tgt_precision = tgt_info.get('data_precision')
    tgt_scale = tgt_info.get('data_scale')
    if (src_precision, src_scale) != (tgt_precision, tgt_scale):
        type_mismatches.append(ColumnTypeIssue(...))
```

### 8.3 LONG表黑名单逻辑修复（P0-3）

#### 问题描述

LONG/LONG RAW类型的表被加入`blacklist_tables`后，即使目标端已成功将LONG转换为CLOB/BLOB，该表仍会阻断所有依赖对象的DDL生成。

#### 当前错误逻辑流程
```
1. LONG/LONG RAW表 → 加入 blacklist_tables (@7334-7340)
2. blacklist_tables 全部加入 unsupported_nodes (@3408-3413)
3. build_blocked_dependency_map() 标记所有依赖对象为 BLOCKED
4. 依赖这些表的 VIEW/TRIGGER/SYNONYM 等被标记为 "依赖不支持对象"
5. 这些对象不会生成 FIXUP DDL！❌
```

#### 黑名单 vs 特殊规则的区别

| 类型 | 含义 | 是否阻断依赖对象 |
|-----|------|----------------|
| **真正的黑名单** | 表无法迁移（如DIY类型、DBLINK表） | ✅ 应该阻断 |
| **特殊规则（LONG→CLOB）** | 表可以迁移，只是类型需要转换 | ❌ 不应阻断 |
| **临时表** | 表不需要迁移DDL | ✅ 应该阻断 |
| **LOB_OVERSIZE** | 表可以创建，但OMS不支持同步 | ⚠️ 看情况 |

#### 问题代码位置

```python
# @3406-3413: 问题所在 - 无条件将所有黑名单表加入unsupported_nodes
unsupported_nodes: Set[DependencyNode] = set()
for (schema, table), entries in (oracle_meta.blacklist_tables or {}).items():
    black_type, reason, detail = summarize_blacklist_entries(entries)
    full = f"{schema.upper()}.{table.upper()}"
    unsupported_nodes.add((full, "TABLE"))  # ❌ 即使LONG已转换为CLOB也会被加入
    unsupported_table_map[full] = (black_type, reason, detail)
    unsupported_table_keys.add((schema.upper(), table.upper()))
```

#### 修复方案

**修改文件**: `schema_diff_reconciler.py`

**修改函数**: `classify_missing_objects()` @3324

**修复代码**:
```python
# @3406-3413: 修复后
unsupported_nodes: Set[DependencyNode] = set()
verified_long_tables: Set[Tuple[str, str]] = set()

for (schema, table), entries in (oracle_meta.blacklist_tables or {}).items():
    black_type, reason, detail = summarize_blacklist_entries(entries)
    full = f"{schema.upper()}.{table.upper()}"
    
    # 检查是否为LONG类型且已成功转换
    is_long_only = all(
        e.black_type.upper() == "LONG" or is_long_type(e.data_type)
        for e in entries.values()
    )
    
    if is_long_only:
        # 检查目标端是否已转换成功
        tgt_schema, tgt_table = table_target_map.get(
            (schema.upper(), table.upper()), 
            (schema.upper(), table.upper())
        )
        status, _, verified = evaluate_long_conversion_status(
            oracle_meta, ob_meta, schema, table, tgt_schema, tgt_table
        )
        if verified:
            # LONG已成功转换，不应阻断依赖对象
            verified_long_tables.add((schema.upper(), table.upper()))
            continue  # 不加入 unsupported_nodes
    
    unsupported_nodes.add((full, "TABLE"))
    unsupported_table_map[full] = (black_type, reason, detail)
    unsupported_table_keys.add((schema.upper(), table.upper()))
```

#### 影响范围

**受影响的对象类型**：
- VIEW（引用LONG表）
- TRIGGER（挂在LONG表上）
- SYNONYM（指向LONG表）
- INDEX/CONSTRAINT（属于LONG表）
- 其他通过依赖链间接引用的对象

---

## 九、结论

### 9.1 总体评价

| 维度 | 评分 | 说明 |
|-----|------|------|
| Remap逻辑 | 8.5/10 | 完善，支持多种推导策略 |
| 校验逻辑 | 7.5/10 | 覆盖主要对象，缺少精度级别验证 |
| DDL生成 | 7.0/10 | 基础完善，**公共同义词是关键缺陷** |
| 代码质量 | 8.0/10 | 结构清晰，少量重复 |

### 9.2 优先修复项

1. **立即修复**: 视图中公共同义词替换 (P0-1)
2. **立即修复**: LONG表黑名单逻辑修复 (P0-3)
3. **本周内**: NUMBER精度对比 (P0-2)
4. **两周内**: IDENTITY列处理、DDL幂等性

### 9.3 工具定位建议

- ✅ 适合用于**迁移评估**和**差异识别**
- ⚠️ 生成的DDL需**人工审核**后再执行
- ⚠️ 引用PUBLIC SYNONYM的VIEW需**特别关注**
- ⚠️ 依赖LONG表的对象可能被错误过滤，需**检查blacklist_tables.txt**

---

**审查完成**
