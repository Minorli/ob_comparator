# 场景化交叉验证深度审查 - 发现问题汇总

## 审查日期
2026-01-20

## 审查方法
采用**场景化交叉验证**方法，系统性推演各种功能组合和边界条件。

---

## 🔴 P0 级问题（严重缺陷）

### P0-1: 触发器状态检查未过滤黑名单表依赖

**场景**: 触发器依赖黑名单表 × 状态差异报告

**问题描述**:
- 黑名单表未迁移到 OB
- 依赖该表的触发器在 OB 端必然 INVALID（表不存在）
- `collect_trigger_status_rows` 将此报告为异常

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:3070-3120`

**当前逻辑**:
```python
def collect_trigger_status_rows(...):
    for src_full, src_info in src_map.items():
        s_valid = lookup_trigger_validity(oracle_meta, src_owner, src_name)
        t_valid = lookup_trigger_validity(ob_meta, tgt_owner, tgt_name)
        
        if s_valid != t_valid:  # ❌ 未检查表是否在黑名单
            diffs.append("VALID")
```

**预期逻辑**:
```python
# 1. 获取触发器依赖的表
trigger_to_table = {}  # 从 oracle_meta.triggers 构建映射
table_key = trigger_to_table.get(src_full.upper())

# 2. 如果表在黑名单，跳过状态检查
if table_key and table_key in oracle_meta.blacklist_tables:
    continue  # OB 端 INVALID 是正常的
```

**影响**:
- 误报：将正常的黑名单表触发器 INVALID 标记为异常
- 运维困惑：报告中出现大量"假异常"

**修复优先级**: P0（立即修复）

---

### P0-2: INVALID 视图未被过滤，可能生成无效 DDL

**场景**: 源端 INVALID VIEW × DDL 生成

**问题描述**:
- Oracle 源端视图可能因权限、依赖缺失等原因 INVALID
- 程序未检查 VIEW 的 `object_statuses`
- INVALID VIEW 的 DDL 可能无法在 OB 执行

**代码位置**: 
- DDL 获取: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:16449-16536`
- 状态检查: **缺失**

**当前逻辑**:
```python
# lines 16449-16463
for src_schema, src_obj, tgt_schema, tgt_obj in view_missing_objects:
    raw_ddl = ...
    # ❌ 未检查源端 VIEW 是否 INVALID
```

**对比 PACKAGE 处理**:
```python
# lines 15723-15727 (PACKAGE 有正确的过滤)
if row.src_status == "INVALID":
    log.info("[FIXUP] 跳过源端 INVALID 的 %s %s (不生成 DDL)。", ...)
    continue
```

**预期逻辑**:
```python
src_status = oracle_meta.object_statuses.get(
    (src_schema.upper(), src_obj.upper(), "VIEW")
)
if normalize_object_status(src_status) == "INVALID":
    log.info("[FIXUP] 跳过源端 INVALID 的 VIEW %s.%s", src_schema, src_obj)
    continue
```

**影响**:
- 生成无法执行的 VIEW DDL
- OB 端执行失败，但用户不清楚根因

**修复优先级**: P0（立即修复）

---

### P0-3: INVALID 触发器未被过滤，可能生成无效 DDL

**场景**: 源端 INVALID TRIGGER × DDL 生成

**问题描述**:
- 与 P0-2 类似，TRIGGER 也未检查 INVALID 状态
- INVALID TRIGGER 的 DDL 可能无法编译

**代码位置**: TRIGGER DDL 生成逻辑

**搜索结果**: 
```
grep: "TRIGGER.*DDL.*获取|TRIGGER.*oracle_get_ddl"
结果: No results found
```

**分析**:
- TRIGGER DDL 生成使用通用的 `fetch_ddl_with_timing`
- 未找到专门的 TRIGGER INVALID 过滤逻辑

**预期逻辑**:
```python
# 在 TRIGGER DDL 生成前添加
src_status = oracle_meta.object_statuses.get(
    (src_schema.upper(), trg_name.upper(), "TRIGGER")
)
if normalize_object_status(src_status) == "INVALID":
    log.info("[FIXUP] 跳过源端 INVALID 的 TRIGGER %s.%s", src_schema, trg_name)
    continue
```

**影响**:
- 生成无法编译的 TRIGGER DDL
- 增加运维工作量

**修复优先级**: P0（立即修复）

---

## 🟡 P1 级问题（重要缺陷）

### P1-1: 外键 DELETE_RULE 未收集

**场景**: 外键约束 × CASCADE 删除规则

**问题描述**:
- Oracle `DBA_CONSTRAINTS.DELETE_RULE` 字段未收集
- 外键的 `ON DELETE CASCADE` / `ON DELETE SET NULL` 规则丢失

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:6494-6522`

**当前 SQL**:
```sql
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, R_OWNER, R_CONSTRAINT_NAME
FROM DBA_CONSTRAINTS
WHERE OWNER IN (...)
  AND CONSTRAINT_TYPE IN ('P','U','R')
```

**缺失字段**: `DELETE_RULE`

**影响**:
- 生成的 FK DDL 缺少 `ON DELETE CASCADE`
- 数据删除行为不一致
- 可能导致数据完整性问题

**修复建议**:
```sql
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, 
       R_OWNER, R_CONSTRAINT_NAME, DELETE_RULE
FROM DBA_CONSTRAINTS
...
```

**修复优先级**: P1（高优先级）

---

### P1-2: PACKAGE 循环依赖未处理

**场景**: PACKAGE/PACKAGE BODY × 循环依赖

**问题描述**:
- VIEW 有拓扑排序和循环检测（lines 16409-16436）
- PACKAGE/PACKAGE BODY 可能存在循环依赖
- 未找到 PACKAGE 拓扑排序逻辑

**搜索结果**:
```
grep: "PACKAGE.*dependency|PACKAGE.*topological"
结果: No results found
```

**影响**:
- PACKAGE 创建顺序可能错误
- 编译失败，需要手工调整顺序

**修复建议**:
- 为 PACKAGE/PACKAGE BODY 实现类似 VIEW 的拓扑排序
- 检测循环依赖并报告

**修复优先级**: P1（中高优先级）

---

### P1-3: INVALID 对象未传播到依赖分析

**场景**: INVALID 对象 × 依赖传播

**问题描述**:
- `unsupported_nodes` 包含黑名单表和不支持视图
- 但不包含 INVALID 对象
- 依赖 INVALID 对象的其他对象未被标记为 BLOCKED

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:3239-3259`

**当前逻辑**:
```python
unsupported_nodes: Set[DependencyNode] = set()

# ✅ 添加黑名单表
for (schema, table), entries in oracle_meta.blacklist_tables.items():
    unsupported_nodes.add((full, "TABLE"))

# ✅ 添加不支持视图
for (schema, view_name), compat in view_compat_map.items():
    if compat.support_state != SUPPORT_STATE_SUPPORTED:
        unsupported_nodes.add((full, "VIEW"))

# ❌ 未添加 INVALID 对象
```

**预期逻辑**:
```python
# 添加 INVALID 对象到 unsupported_nodes
for (owner, name, obj_type), status in oracle_meta.object_statuses.items():
    if normalize_object_status(status) == "INVALID":
        full = f"{owner}.{name}"
        unsupported_nodes.add((full, obj_type))
```

**影响**:
- 依赖 INVALID VIEW/PACKAGE 的对象未被正确分类
- 可能生成无法执行的 DDL

**修复优先级**: P1（中优先级）

---

## 🟢 P2 级问题（建议优化）

### P2-1: 临时表依赖对象未标记为 BLOCKED

**场景**: 视图依赖临时表 × 支持性分类

**问题描述**:
- 临时表被正确识别为 `TEMPORARY_TABLE`（lines 15651-15653）
- 但依赖临时表的 VIEW/SYNONYM 未被自动标记为 BLOCKED

**当前行为**:
- 临时表不生成 DDL ✅
- 依赖临时表的 VIEW 仍会尝试生成 DDL ❌

**建议**:
- 将临时表加入 `unsupported_nodes`
- 通过 `blocked_by_map` 传播到依赖对象

**修复优先级**: P2（建议优化）

---

### P2-2: 分区表 INTERVAL 分区信息收集不完整

**场景**: INTERVAL 分区表 × 兼容性检查

**问题描述**:
- 代码支持 `interval_partitions` 收集（lines 6610-6692）
- 但未验证 OceanBase 对 INTERVAL 分区的兼容性
- 未检查 `INTERVAL` 语法是否被 `sanitization_rules` 覆盖

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:6610-6692`

**当前逻辑**:
```python
# 收集 INTERVAL 分区信息
interval_partitions: Dict[Tuple[str, str], IntervalPartitionInfo] = {}
sql_interval_tpl = """
    SELECT OWNER, TABLE_NAME, PARTITIONING_TYPE, SUBPARTITIONING_TYPE, INTERVAL
    FROM DBA_PART_TABLES
    WHERE OWNER IN (...)
      AND INTERVAL IS NOT NULL
"""
```

**潜在问题**:
- OceanBase 可能不支持 INTERVAL 分区
- DDL 包含 INTERVAL 语法可能执行失败

**建议**:
- 检查 TABLE DDL 是否包含 INTERVAL 语法
- 如不兼容，标记表为 UNSUPPORTED 或移除 INTERVAL 语法

**修复优先级**: P2（建议评估）

---

### P2-3: 同义词指向 INVALID 对象未检查

**场景**: SYNONYM 指向 INVALID VIEW/TABLE

**问题描述**:
- SYNONYM 指向不存在对象会被标记为 BLOCKED（lines 3293-3302）
- 但未检查目标对象是否 INVALID

**当前逻辑**:
```python
# lines 3293-3302
if syn_meta and syn_meta.table_owner and syn_meta.table_name:
    ref_full = f"{syn_meta.table_owner}.{syn_meta.table_name}"
    if ref_full in unsupported_table_map or (ref_full, "VIEW") in unsupported_nodes:
        support_state = SUPPORT_STATE_BLOCKED
```

**建议**:
```python
# 检查目标对象是否 INVALID
ref_status = oracle_meta.object_statuses.get(
    (syn_meta.table_owner, syn_meta.table_name, "TABLE")
)
if normalize_object_status(ref_status) == "INVALID":
    support_state = SUPPORT_STATE_BLOCKED
    reason = "同义词指向 INVALID 对象"
```

**修复优先级**: P2（建议优化）

---

## ✅ 已正确实现的场景

### ✅ 黑名单表 × INDEX/CONSTRAINT/TRIGGER Fixup 过滤

**验证结果**: 正确实现

**代码位置**:
- INDEX: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:15763-15778`
- CONSTRAINT: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:15779-15793`
- TRIGGER: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:15800-15841`

**逻辑**:
```python
if (src_schema.upper(), src_table.upper()) in unsupported_table_keys:
    continue  # ✅ 跳过黑名单表的索引/约束/触发器
```

---

### ✅ 黑名单表 × 依赖对象状态分类

**验证结果**: 正确实现

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:3341-3417`

**逻辑**:
```python
# ✅ 黑名单表的 INDEX/CONSTRAINT/TRIGGER 标记为 BLOCKED
for item in extra_results.get('index_mismatched', []):
    if (src_schema.upper(), src_table.upper()) not in unsupported_table_keys:
        continue
    row = ObjectSupportReportRow(
        support_state=SUPPORT_STATE_BLOCKED,
        reason="依赖不支持表"
    )
```

---

### ✅ VIEW 循环依赖检测

**验证结果**: 正确实现

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:16432-16436`

**逻辑**:
```python
if len(sorted_view_tuples) < len(view_deps):
    circular = [v for v, d in in_degree.items() if d > 0]
    log.warning(f"[FIXUP] 发现 {len(circular)} 个循环依赖的VIEW，将最后创建")
    sorted_view_tuples.extend(circular)
```

---

### ✅ PACKAGE INVALID 对象过滤

**验证结果**: 正确实现

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:15723-15727`

**逻辑**:
```python
if row.src_status == "INVALID":
    log.info("[FIXUP] 跳过源端 INVALID 的 %s %s (不生成 DDL)。", ...)
    continue
```

---

### ✅ 临时表识别和分离

**验证结果**: 正确实现

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:15651-15696`

**逻辑**:
```python
def is_temporary_support_row(row):
    code = (row.reason_code or "").upper()
    if "TEMPORARY_TABLE" in code or "TEMP_TABLE" in code:
        return True

# 临时表分离到 missing_tables_unsupported
if support_state == SUPPORT_STATE_SUPPORTED:
    missing_tables_supported.append(...)
else:
    missing_tables_unsupported.append(...)  # ✅ 包含临时表
```

---

### ✅ 外键 remap 处理

**验证结果**: 正确实现

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:16889-16897`

**逻辑**:
```python
# FK 引用表的 remap 映射
if cons_meta and ctype == 'R':
    ref_owner = cons_meta.get("ref_table_owner") or cons_meta.get("r_owner")
    ref_table = cons_meta.get("ref_table_name")
    # ... remap 处理
```

---

### ✅ PUBLIC SYNONYM 范围过滤

**验证结果**: 正确实现

**代码位置**: `@C:\github_repo\ob_comparator\schema_diff_reconciler.py:15668-15681`

**逻辑**:
```python
if syn_meta and allowed_synonym_targets and syn_meta.table_owner:
    table_owner_u = syn_meta.table_owner.upper()
    if table_owner_u not in allowed_synonym_targets:
        log.info("[FIXUP] 跳过同义词 %s (table_owner 不在范围内)", ...)
        continue
```

---

## 📊 问题统计

| 优先级 | 数量 | 类型 |
|-------|------|------|
| P0 | 3 | INVALID 对象未过滤 |
| P1 | 3 | 功能缺失 |
| P2 | 3 | 建议优化 |
| ✅ 正确 | 7 | 已验证通过 |

---

## 🎯 修复优先级建议

### 立即修复（P0）
1. **P0-1**: 触发器状态检查未过滤黑名单表依赖
2. **P0-2**: INVALID 视图未被过滤
3. **P0-3**: INVALID 触发器未被过滤

### 高优先级（P1）
4. **P1-1**: 外键 DELETE_RULE 未收集
5. **P1-2**: PACKAGE 循环依赖未处理
6. **P1-3**: INVALID 对象未传播到依赖分析

### 建议优化（P2）
7. **P2-1**: 临时表依赖对象未标记为 BLOCKED
8. **P2-2**: 分区表 INTERVAL 兼容性未验证
9. **P2-3**: 同义词指向 INVALID 对象未检查

---

## 🔍 审查覆盖度

### 已审查场景
- ✅ 黑名单表 × 所有依赖对象类型（INDEX/CONSTRAINT/TRIGGER/VIEW/SYNONYM）
- ✅ INVALID 对象 × DDL 生成（PACKAGE/VIEW/TRIGGER）
- ✅ 循环依赖 × 拓扑排序（VIEW/PACKAGE）
- ✅ 临时表 × 迁移策略
- ✅ 外键 × 跨 schema 引用和 remap
- ✅ 同义词 × 目标对象状态
- ⚠️ 分区表 × 兼容性（部分审查）
- ⚠️ 权限缺失 × 元数据访问（部分审查）

### 未深入审查场景
- ⏳ 大规模迁移 × 内存/性能优化
- ⏳ 事务/回滚 × DDL 执行失败
- ⏳ 超时策略 × 大对象处理
- ⏳ 虚拟列 × DDL 生成
- ⏳ 函数索引 × 表达式提取

---

## 📝 审查结论

通过**场景化交叉验证**方法，发现了 **9 个问题**，其中：
- **3 个 P0 级严重缺陷**：INVALID 对象未过滤，可能导致无效 DDL 生成
- **3 个 P1 级重要缺陷**：功能缺失，影响迁移完整性
- **3 个 P2 级建议优化**：边界情况未覆盖

同时验证了 **7 个场景正确实现**，说明主程序在黑名单过滤、循环依赖、临时表处理等核心功能上是可靠的。

**关键发现**：
1. **不一致性**：PACKAGE 有 INVALID 过滤，但 VIEW/TRIGGER 没有
2. **传播缺失**：INVALID 对象未加入依赖传播机制
3. **元数据不完整**：DELETE_RULE 等关键字段未收集

这些问题说明之前的审查确实"浮于表面"，未能通过场景推演发现隐藏的逻辑缺陷。
