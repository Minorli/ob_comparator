# 表对象检查规格说明

**文档版本**: 1.0  
**生成日期**: 2026-01-25  
**适用程序**: schema_diff_reconciler.py  

---

## 1. 概述

ob_comparator 程序在比对表对象时，会从 Oracle 源端和 OceanBase 目标端读取元数据，并逐项检查以下内容：

- **列结构** (Columns)
- **索引** (Indexes)
- **约束** (Constraints)
- **触发器** (Triggers)
- **注释** (Comments) - 可选
- **序列** (Sequences) - Schema 级别

---

## 2. 列结构检查

### 2.1 数据来源

| 端 | 数据源 |
|----|--------|
| Oracle | `DBA_TAB_COLUMNS` |
| OceanBase | `DBA_TAB_COLUMNS` (Oracle 模式) |

### 2.2 检查项明细

| 检查项 | 检查逻辑 | 不一致时的处理 |
|--------|----------|----------------|
| **缺失列** | 源端有但目标端没有的列 | 生成 `ALTER TABLE ADD COLUMN` |
| **多余列** | 目标端有但源端没有的列 | 生成注释掉的 `DROP COLUMN` 建议 |
| **VARCHAR/VARCHAR2 长度** | BYTE 语义需放大 1.5 倍，CHAR 语义精确匹配 | 生成 `MODIFY COLUMN` |
| **NUMBER 精度/刻度** | 等价性检查（见 2.3 节） | 生成 `MODIFY COLUMN` |
| **虚拟列表达式** | 比较标准化后的表达式内容 | 报告类型不一致 |
| **IDENTITY 属性** | 源端是 IDENTITY 列，目标端也应是 | 报告类型不一致 |
| **DEFAULT ON NULL** | 源端有此属性，目标端也应有 | 报告类型不一致 |
| **INVISIBLE 可见性** | 源端 INVISIBLE，目标端也应是 | 报告类型不一致 |
| **LONG/LONG RAW 类型** | 映射为 CLOB/BLOB | 生成 `MODIFY COLUMN` |

### 2.3 NUMBER 类型等价性规则

```
normalize_number_signature(prec, scale):
    if prec is None and scale is None:
        return (None, None, unbounded=True)   # NUMBER 无参数
    if prec is None:
        return (38, scale, unbounded=False)   # NUMBER(*,scale) -> NUMBER(38,scale)
    return (prec, scale, unbounded=False)
```

**等价判定**:

| 源端 | 目标端 | 结果 |
|------|--------|------|
| `NUMBER(*,0)` | `NUMBER(38,0)` | ✅ 等价 |
| `NUMBER(*,0)` | `NUMBER(37,0)` | ❌ 不等价 (精度不足) |
| `NUMBER` | `NUMBER` | ✅ 等价 (两端均无界) |
| `NUMBER(10,2)` | `NUMBER(12,2)` | ✅ 等价 (目标精度更大) |
| `NUMBER(10,2)` | `NUMBER(10,3)` | ❌ 不等价 (刻度不同) |

### 2.4 VARCHAR 长度规则

| 源端语义 | 目标端要求 | 说明 |
|----------|------------|------|
| BYTE (`char_used='B'`) | `length >= src_length * 1.5` | 预留多字节字符空间 |
| CHAR (`char_used='C'`) | `length == src_length` | 精确匹配 |

**过大长度警告**: 目标端长度超过源端 3 倍时会标记为 `oversize`。

---

## 3. 索引检查

### 3.1 数据来源

| 端 | 数据源 |
|----|--------|
| Oracle | `DBA_INDEXES` + `DBA_IND_COLUMNS` + `DBA_IND_EXPRESSIONS` |
| OceanBase | `DBA_INDEXES` + `DBA_IND_COLUMNS` + `DBA_IND_EXPRESSIONS` |

### 3.2 检查项明细

| 检查项 | 检查逻辑 | 说明 |
|--------|----------|------|
| **索引列集** | 比较标准化后的列元组 | 忽略索引名差异 |
| **唯一性** | UNIQUE vs NONUNIQUE | 约束支撑的索引可忽略差异 |
| **函数表达式** | 大小写标准化后比较 | `DECODE(...)` 等表达式 |
| **SYS_NC 列** | 归一化处理 | `SYS_NC0001$` → `SYS_NC$` |

### 3.3 索引列标准化

```python
def normalize_index_columns(columns, expr_map):
    for idx, col in enumerate(columns, start=1):
        expr = expr_map.get(idx)
        # 函数表达式：大小写标准化
        token = normalize_sql_expression_casefold(expr) if expr else col.upper()
```

### 3.4 约束覆盖逻辑

如果源端存在索引列集 `(A, B)`，目标端没有对应索引，但存在 `PRIMARY KEY(A, B)` 或 `UNIQUE(A, B)` 约束，则**不报告为缺失索引**。

---

## 4. 约束检查

### 4.1 数据来源

| 端 | 数据源 |
|----|--------|
| Oracle | `DBA_CONSTRAINTS` + `DBA_CONS_COLUMNS` |
| OceanBase | `DBA_CONSTRAINTS` + `DBA_CONS_COLUMNS` |

### 4.2 约束类型

| 类型代码 | 约束类型 | 检查项 |
|----------|----------|--------|
| `P` | PRIMARY KEY | 列集、分区键包含性 |
| `U` | UNIQUE | 列集 |
| `R` | FOREIGN KEY | 列集、引用表、DELETE_RULE |
| `C` | CHECK | 表达式内容、DEFERRABLE/DEFERRED |

### 4.3 PRIMARY KEY 分区键处理

当 Oracle 表使用分区且 PK 列不包含分区键时：

```
源端: PRIMARY KEY (ID)
分区键: (CREATED_DATE)

→ OceanBase 要求 PK 必须包含分区键
→ 程序将 PK 降级为 UNIQUE KEY 进行匹配
```

### 4.4 CHECK 约束标准化

```python
def normalize_check_constraint_expression(expr, cons_name):
    # 1. 去除多余空白
    # 2. 大小写标准化 (单引号内保持原样)
    # 3. 去除冗余括号
    # 4. 可选: 附加 DEFERRABLE/DEFERRED 属性
```

**示例**:

| 源端 | 目标端 | 标准化后 | 结果 |
|------|--------|----------|------|
| `"COL" IS NOT NULL` | `"COL" is not null` | `COL IS NOT NULL` | ✅ 匹配 |
| `status = 'Active'` | `STATUS = 'Active'` | `STATUS = 'Active'` | ✅ 匹配 |

### 4.5 系统 NOT NULL 约束过滤

以 `SYS_` 开头且表达式为 `COL IS NOT NULL` 的约束会被自动过滤，不参与比对。

### 4.6 FOREIGN KEY 检查

| 检查项 | 说明 |
|--------|------|
| 列集 | 外键列必须匹配 |
| 引用表 | 应用 remap 规则后比较 |
| DELETE_RULE | `CASCADE` / `SET NULL` / `NO ACTION` |

---

## 5. 触发器检查

### 5.1 数据来源

| 端 | 数据源 |
|----|--------|
| Oracle | `DBA_TRIGGERS` |
| OceanBase | `DBA_TRIGGERS` |

### 5.2 检查项明细

| 检查项 | 说明 |
|--------|------|
| **触发器名称** | 按 remap 规则匹配 |
| **事件类型** | `INSERT` / `UPDATE` / `DELETE` 等 |
| **状态** | `ENABLED` / `DISABLED` |
| **有效性** | `VALID` / `INVALID` (从 `DBA_OBJECTS` 获取) |

---

## 6. 注释检查 (可选)

### 6.1 数据来源

| 类型 | 数据源 |
|------|--------|
| 表注释 | `DBA_TAB_COMMENTS` |
| 列注释 | `DBA_COL_COMMENTS` |

### 6.2 检查逻辑

- 仅在两端元数据均完整加载时进行比对
- 注释内容精确匹配
- 不一致时可生成 `COMMENT ON` 修复语句

---

## 7. 序列检查

### 7.1 数据来源

| 端 | 数据源 |
|----|--------|
| Oracle | `DBA_SEQUENCES` |
| OceanBase | `DBA_SEQUENCES` |

### 7.2 检查项明细

| 属性 | 说明 |
|------|------|
| `INCREMENT_BY` | 步长 |
| `MIN_VALUE` | 最小值 |
| `MAX_VALUE` | 最大值 |
| `CYCLE_FLAG` | 是否循环 (`Y`/`N`) |
| `ORDER_FLAG` | 是否排序 (`Y`/`N`) |
| `CACHE_SIZE` | 缓存大小 |

---

## 8. 检查流程图

```
┌─────────────────────────────────────────────────────────────┐
│                    check_primary_objects()                   │
│  ┌─────────────────────────────────────────────────────────┐│
│  │  对每个 TABLE 对象:                                      ││
│  │    1. 比对列结构 (missing/extra/length/type)            ││
│  │    2. 记录不一致项                                       ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    check_extra_objects()                     │
│  ┌─────────────────────────────────────────────────────────┐│
│  │  对每个 TABLE 对象:                                      ││
│  │    1. compare_indexes_for_table()                        ││
│  │    2. compare_constraints_for_table()                    ││
│  │    3. compare_triggers_for_table()                       ││
│  └─────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────┐│
│  │  对每个 SCHEMA 映射:                                     ││
│  │    - compare_sequences_for_schema()                      ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  生成修复 DDL (可选)                         │
│    - generate_column_fixup_ddl()                            │
│    - generate_index_ddl()                                   │
│    - generate_constraint_ddl()                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 9. 配置项

以下配置项影响检查行为：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled_extra_types` | `INDEX,CONSTRAINT,SEQUENCE,TRIGGER` | 启用的扩展检查类型 |
| `extra_check_workers` | `1` | 并发 worker 数量 |
| `extra_check_chunk_size` | `200` | 批处理大小 |
| `extra_check_progress_interval` | `10` | 进度日志间隔 (秒) |

---

## 10. 相关代码位置

| 功能 | 函数名 | 行号范围 |
|------|--------|----------|
| 列结构检查 | `check_primary_objects()` | 10200-10470 |
| 索引检查 | `compare_indexes_for_table()` | 11002-11034 |
| 索引列比对 | `compare_index_maps()` | 10916-11000 |
| 约束检查 | `compare_constraints_for_table()` | 11396-11746 |
| 触发器检查 | `compare_triggers_for_table()` | 11889-11990 |
| 序列检查 | `compare_sequences_for_schema()` | 11749-11885 |
| NUMBER 等价性 | `is_number_equivalent()` | 1285-1301 |
| CHECK 标准化 | `normalize_check_constraint_expression()` | 1186-1196 |

---

## 11. 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 1.0 | 2026-01-25 | 初始版本 |

---

*本文档由 Cascade AI 自动生成*
