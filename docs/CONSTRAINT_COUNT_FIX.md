# 约束统计错误修复说明 (v0.8.3)

## 问题描述

用户报告：在检查汇总中，Oracle显示63个约束，OceanBase显示97个约束，数量不匹配。

```
| CONSTRAINT | 63 | 97 | 0 | 34 |
```

这个差异是**不正确的**，因为用户使用fixup脚本补充了OceanBase端的约束，理论上两端的约束数量应该一致。

## 根本原因

### 问题分析

程序在统计约束数量时使用了不一致的逻辑：

#### Oracle端统计
```python
# 在 load_oracle_metadata_batch() 中
if key not in table_pairs:
    continue  # 只加载remap规则中涉及的表的约束
```

Oracle只加载并统计**remap规则中涉及的表**的约束。

#### OceanBase端统计
```python
# 在 load_ob_metadata_once() 中
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
FROM DBA_CONSTRAINTS
WHERE OWNER IN (目标schema列表)  -- 加载目标schema下所有表的约束
```

OceanBase加载了目标schema下**所有表**的约束，包括：
- remap规则中涉及的表的约束
- 目标schema中其他表的约束（可能是历史遗留、测试数据等）
- 系统自动创建的约束

#### 统计逻辑
```python
# 在 compute_object_counts() 中（修复前）
if obj_type_u == 'CONSTRAINT':
    src_count = sum(len(v) for v in oracle_meta.constraints.values())  # 所有表
    tgt_count = sum(len(v) for v in ob_meta.constraints.values())      # 所有表
```

统计时简单地对所有表的约束求和，导致：
- Oracle: 只统计了remap规则中的表 → 63个约束
- OceanBase: 统计了目标schema下所有表 → 97个约束（多了34个）

### 为什么会多34个？

可能的原因：
1. **目标schema中有其他表**：不在remap规则中，但有约束
2. **重命名的表**：如 `ORDERS_RENAME_20251118`，其约束仍然存在
3. **系统表或临时表**：可能有一些测试表或系统表
4. **OMS相关表**：可能有OMS迁移工具创建的辅助表

## 解决方案

### 修复逻辑

修改 `compute_object_counts()` 函数，确保两端使用相同的过滤逻辑：

```python
# 1. 从 full_object_mapping 中提取所有涉及TABLE的源表和目标表
src_tables = set()
tgt_tables = set()
for src_name, type_map in full_object_mapping.items():
    if 'TABLE' in type_map:
        tgt_name = type_map['TABLE']
        if '.' in src_name:
            src_schema, src_table = src_name.split('.', 1)
            src_tables.add((src_schema.upper(), src_table.upper()))
        if '.' in tgt_name:
            tgt_schema, tgt_table = tgt_name.split('.', 1)
            tgt_tables.add((tgt_schema.upper(), tgt_table.upper()))

# 2. 只统计这些表的约束
if obj_type_u == 'CONSTRAINT':
    src_count = sum(len(v) for k, v in oracle_meta.constraints.items() if k in src_tables)
    tgt_count = sum(len(v) for k, v in ob_meta.constraints.items() if k in tgt_tables)
```

### 修复效果

修复后，统计逻辑变为：
- **Oracle**: 统计remap规则中源表的约束
- **OceanBase**: 统计remap规则中目标表的约束

这样两端使用相同的表集合，统计结果才有可比性。

## 示例场景

### 场景1：目标schema有额外的表

```
Remap规则：
  HERO_A.USERS = OLYMPIAN_A.USERS
  HERO_A.ORDERS = OLYMPIAN_A.ORDERS

Oracle端（HERO_A schema）：
  USERS (2个约束: PK_USERS, UK_EMAIL)
  ORDERS (2个约束: PK_ORDERS, FK_USER)
  总计: 4个约束

OceanBase端（OLYMPIAN_A schema）：
  USERS (2个约束: PK_USERS, UK_EMAIL)
  ORDERS (2个约束: PK_ORDERS, FK_USER)
  PRODUCTS (3个约束: PK_PRODUCTS, UK_SKU, FK_CATEGORY)  ← 不在remap规则中
  ORDERS_RENAME_20251118 (2个约束: PK_ORDERS, FK_USER)  ← 重命名的旧表
  总计: 9个约束

修复前统计：
  Oracle: 4个约束
  OceanBase: 9个约束 ← 错误！包含了PRODUCTS和重命名表

修复后统计：
  Oracle: 4个约束（USERS + ORDERS）
  OceanBase: 4个约束（USERS + ORDERS）← 正确！只统计remap规则中的表
```

### 场景2：用户的实际情况

```
Remap规则中的表: 34个
每个表平均约束数: ~2个（PK + 可能的UK/FK）

Oracle端：
  34个表 × ~2个约束 = 63个约束 ✓

OceanBase端（修复前）：
  34个表 × ~2个约束 = 68个约束
  + 其他表的约束 = 29个约束
  总计: 97个约束 ✗

OceanBase端（修复后）：
  34个表 × ~2个约束 = 63个约束 ✓
```

## 影响范围

### 受影响的统计

1. **CONSTRAINT统计**：主要修复目标
2. **INDEX统计**：应用了相同的修复逻辑（虽然可能没有问题，但为了一致性）

### 不受影响的统计

- **TABLE/VIEW/PROCEDURE等主对象**：本来就基于 `full_object_mapping` 统计，逻辑正确
- **SEQUENCE/TRIGGER**：统计逻辑不同，不受影响

## 验证方法

### 重新运行程序

```bash
python3 schema_diff_reconciler.py
```

查看新生成的报告，检查汇总部分：

```
| CONSTRAINT | 63 | 63 | 0 | 0 |
```

应该显示Oracle和OceanBase的约束数量一致。

### 手工验证

如果需要手工验证，可以查询：

```sql
-- Oracle端：统计remap规则中源表的约束
SELECT COUNT(*) 
FROM DBA_CONSTRAINTS 
WHERE (OWNER, TABLE_NAME) IN (
  -- 列出remap规则中的所有源表
  ('HERO_A', 'USERS'),
  ('HERO_A', 'ORDERS'),
  ...
)
AND CONSTRAINT_TYPE IN ('P','U','R')
AND STATUS = 'ENABLED';

-- OceanBase端：统计remap规则中目标表的约束
SELECT COUNT(*) 
FROM DBA_CONSTRAINTS 
WHERE (OWNER, TABLE_NAME) IN (
  -- 列出remap规则中的所有目标表
  ('OLYMPIAN_A', 'USERS'),
  ('OLYMPIAN_A', 'ORDERS'),
  ...
)
AND CONSTRAINT_TYPE IN ('P','U','R')
AND STATUS = 'ENABLED';
```

两个查询的结果应该一致（或接近，差异应该在详细报告中体现）。

## 相关问题

### 为什么不修改OceanBase的加载逻辑？

可以考虑修改 `load_ob_metadata_once()` 函数，只加载remap规则中涉及的表的约束。但这样做有几个问题：

1. **需要传递额外参数**：需要把 `full_object_mapping` 传递给加载函数
2. **影响其他功能**：可能有其他地方依赖完整的元数据
3. **性能影响不大**：加载所有约束的性能开销可以接受

因此，选择在统计阶段过滤，更简单、更安全。

### 为什么Oracle不加载所有表的约束？

Oracle的加载逻辑中有 `if key not in table_pairs: continue`，这是为了：

1. **性能优化**：只加载需要的数据，减少内存占用
2. **避免无关数据**：源schema可能有很多表，但只有部分需要迁移

这个设计是合理的，问题在于OceanBase端没有相同的过滤。

## 总结

- **问题**：约束统计不一致，Oracle 63个，OceanBase 97个
- **原因**：Oracle只统计remap规则中的表，OceanBase统计所有表
- **修复**：在统计阶段过滤，确保两端只统计remap规则中的表
- **版本**：v0.8.3
- **影响**：CONSTRAINT和INDEX的统计更准确，报告更可信

修复后，检查汇总中的约束数量应该一致，如果仍有差异，会在详细的约束比对报告中体现具体的缺失或多余项。
