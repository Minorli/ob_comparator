# 深度功能审查报告 - 业务逻辑与实际场景分析

**项目**: OceanBase Comparator Toolkit  
**版本**: V0.9.8  
**审查日期**: 2025  
**审查范围**: 完整业务流程的功能正确性与实际场景适配性  

---

## 📋 执行摘要

本次审查深入分析了 `ob_comparator` 在实际 Oracle → OceanBase 迁移场景中的功能逻辑，重点关注：
1. **元数据转储的完整性与准确性**
2. **对象对比逻辑的正确性**
3. **DDL 生成的可执行性**
4. **依赖排序的准确性**
5. **端到端场景的覆盖度**

### 总体评估 (基于 V0.9.8 代码实际实现)

| 维度 | 评分 | 说明 |
|------|------|------|
| 元数据收集完整性 | 8.0/10 | ✅ 已支持 CHAR_USED/PRECISION/SCALE/HIDDEN_COLUMN，❌ 缺少 VIRTUAL_COLUMN 和函数索引表达式 |
| 对比逻辑准确性 | 7.5/10 | ✅ VARCHAR 语义已正确处理，❌ NUMBER 精度未对比，CHECK 约束未收集 |
| DDL 生成可靠性 | 7.5/10 | 基础 DDL 生成正确，复杂依赖和特殊语法兼容性待验证 |
| 依赖排序正确性 | 8.0/10 | ✅ VIEW 已有拓扑排序和循环检测，但 PACKAGE 层内顺序未保证 |
| 端到端场景覆盖 | 6.5/10 | 缺少综合性的端到端测试场景 |
| **生产环境适用性** | **7.0/10** | **❌ 内存风险、timeout 策略、OB 侧 CHAR_USED 缺失是关键问题** |

---

## 🔍 1. 元数据转储逻辑审查

### 1.1 核心视图查询完整性

#### ✅ 已覆盖的关键字段

**DBA_TAB_COLUMNS** (Oracle 侧):
```python
# lines 6376-6379
SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
       DATA_LENGTH, DATA_PRECISION, DATA_SCALE,
       NULLABLE, DATA_DEFAULT, CHAR_USED, CHAR_LENGTH, HIDDEN_COLUMN
FROM DBA_TAB_COLUMNS
WHERE OWNER IN ({owners_clause})
```

**优点**:
- ✅ 包含 `CHAR_USED` 和 `CHAR_LENGTH`，支持 BYTE/CHAR 语义区分
- ✅ 包含 `HIDDEN_COLUMN` 探测，支持 Oracle 12c+ 隐藏列过滤
- ✅ 支持降级处理（当 `HIDDEN_COLUMN` 不可用时自动回退）

**DBA_INDEXES** + **DBA_IND_COLUMNS**:
```python
# lines 6439-6442, 6465-6469
SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, UNIQUENESS
FROM DBA_INDEXES
WHERE TABLE_OWNER IN ({owners_clause})

SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_NAME
FROM DBA_IND_COLUMNS
WHERE TABLE_OWNER IN ({owners_clause})
ORDER BY TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_POSITION
```

**优点**:
- ✅ 正确获取索引列顺序（通过 `COLUMN_POSITION` 排序）
- ✅ 索引唯一性标记正确获取

**DBA_CONSTRAINTS** + **DBA_CONS_COLUMNS**:
```python
# lines 6494-6499, 6524-6528
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, R_OWNER, R_CONSTRAINT_NAME
FROM DBA_CONSTRAINTS
WHERE OWNER IN ({owners_clause})
  AND CONSTRAINT_TYPE IN ('P','U','R')
  AND STATUS = 'ENABLED'

SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME
FROM DBA_CONS_COLUMNS
WHERE OWNER IN ({owners_clause})
ORDER BY OWNER, TABLE_NAME, CONSTRAINT_NAME, POSITION
```

**优点**:
- ✅ 正确获取外键引用关系（`R_OWNER`, `R_CONSTRAINT_NAME`）
- ✅ 通过约束查找补齐被引用表信息（lines 6551-6569）
- ✅ 仅获取启用的约束（`STATUS = 'ENABLED'`）

**分区键和 Interval 分区**:
```python
# lines 6580-6607
SELECT OWNER, NAME, COLUMN_NAME, COLUMN_POSITION
FROM DBA_PART_KEY_COLUMNS
WHERE OWNER IN ({owners_clause})
  AND OBJECT_TYPE = 'TABLE'

SELECT OWNER, NAME, COLUMN_NAME, COLUMN_POSITION
FROM DBA_SUBPART_KEY_COLUMNS
WHERE OWNER IN ({owners_clause})
  AND OBJECT_TYPE = 'TABLE'

# lines 6612-6623
SELECT OWNER, TABLE_NAME, PARTITIONING_TYPE, SUBPARTITIONING_TYPE, INTERVAL
FROM DBA_PART_TABLES
WHERE OWNER IN ({owners_clause})
  AND INTERVAL IS NOT NULL
```

**优点**:
- ✅ 正确收集分区键列（主分区和子分区）
- ✅ 正确识别 Interval 分区表并获取 INTERVAL 表达式

#### ❌ 关键问题 1: 虚拟列未识别 (已确认)

**问题描述**:
```python
# lines 6373-6395: 获取列定义
# 缺少 VIRTUAL_COLUMN 标记字段
SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
       DATA_LENGTH, DATA_PRECISION, DATA_SCALE,
       NULLABLE, DATA_DEFAULT, CHAR_USED, CHAR_LENGTH, HIDDEN_COLUMN
FROM DBA_TAB_COLUMNS
```

**影响**:
- Oracle 11g+ 支持虚拟列（VIRTUAL_COLUMN = 'YES'）
- 虚拟列的 DDL 生成逻辑与普通列不同（需要 `GENERATED ALWAYS AS (expression)`）
- 当前代码未区分虚拟列和普通列，可能导致：
  - ❌ 虚拟列被误判为缺失列
  - ❌ 生成的 DDL 缺少 `GENERATED ALWAYS AS` 子句

**修复建议**:
```python
# 查询时增加 VIRTUAL_COLUMN 字段
SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
       DATA_LENGTH, DATA_PRECISION, DATA_SCALE,
       NULLABLE, DATA_DEFAULT, CHAR_USED, CHAR_LENGTH,
       NVL(TO_CHAR(HIDDEN_COLUMN),'NO') AS HIDDEN_COLUMN,
       NVL(TO_CHAR(VIRTUAL_COLUMN),'NO') AS VIRTUAL_COLUMN
FROM DBA_TAB_COLUMNS
WHERE OWNER IN ({owners_clause})
```

**业务场景**:
```sql
-- Oracle 源端
CREATE TABLE ORDERS (
    ORDER_ID NUMBER,
    AMOUNT NUMBER,
    TAX_AMOUNT NUMBER GENERATED ALWAYS AS (AMOUNT * 0.1) VIRTUAL
);

-- 当前工具行为：
-- ❌ 对比时发现 OB 缺少 TAX_AMOUNT 列
-- ❌ 生成的 DDL 可能为: ALTER TABLE ORDERS ADD (TAX_AMOUNT NUMBER);
-- ✅ 正确的 DDL 应为: ALTER TABLE ORDERS ADD (TAX_AMOUNT NUMBER GENERATED ALWAYS AS (AMOUNT * 0.1) VIRTUAL);
```

#### ❌ 关键问题 2: 函数索引表达式未提取 (已确认)

**问题描述**:
```python
# lines 6465-6490: 获取索引列
SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_NAME
FROM DBA_IND_COLUMNS
WHERE TABLE_OWNER IN ({owners_clause})
ORDER BY TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_POSITION
```

**影响**:
- Oracle 支持函数索引（Function-based Index）
- `DBA_IND_COLUMNS.COLUMN_NAME` 中函数索引列显示为 `SYS_NCxxxxx$`
- 真实表达式存储在 `DBA_IND_EXPRESSIONS` 视图中
- 当前代码未读取 `DBA_IND_EXPRESSIONS`，可能导致：
  - ❌ 函数索引被识别为普通列索引
  - ❌ 生成的索引 DDL 不正确

**修复建议**:
```python
# 增加对 DBA_IND_EXPRESSIONS 的查询
SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_POSITION, COLUMN_EXPRESSION
FROM DBA_IND_EXPRESSIONS
WHERE TABLE_OWNER IN ({owners_clause})
ORDER BY TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_POSITION
```

**业务场景**:
```sql
-- Oracle 源端
CREATE INDEX IDX_UPPER_NAME ON CUSTOMERS (UPPER(CUSTOMER_NAME));

-- DBA_IND_COLUMNS 返回:
-- INDEX_NAME=IDX_UPPER_NAME, COLUMN_NAME=SYS_NC00003$

-- DBA_IND_EXPRESSIONS 返回:
-- INDEX_NAME=IDX_UPPER_NAME, COLUMN_EXPRESSION=UPPER("CUSTOMER_NAME")

-- 当前工具行为：
-- ❌ 索引列被识别为 "SYS_NC00003$"
-- ✅ 正确识别应为: UPPER(CUSTOMER_NAME)
```

#### ❌ 关键问题 3: CHECK 约束未收集 (已确认)

**问题描述**:
```python
# lines ~6494-6499 (实际代码)
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, R_OWNER, R_CONSTRAINT_NAME
FROM DBA_CONSTRAINTS
WHERE OWNER IN ({owners_clause})
  AND CONSTRAINT_TYPE IN ('P','U','R')  -- ❌ 已确认：仅收集主键/唯一/外键，缺少 'C' (CHECK约束)
  AND STATUS = 'ENABLED'
```

**影响**:
- 当前仅收集主键（P）、唯一（U）、外键（R）约束
- ❌ 忽略了 CHECK 约束（C）
- CHECK 约束是重要的数据完整性保障，缺失可能导致：
  - OB 端缺少业务规则验证
  - 数据迁移后出现不符合业务规则的数据

**修复建议**:
```python
# 增加 CHECK 约束收集
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, 
       R_OWNER, R_CONSTRAINT_NAME, SEARCH_CONDITION
FROM DBA_CONSTRAINTS
WHERE OWNER IN ({owners_clause})
  AND CONSTRAINT_TYPE IN ('P','U','R','C')  -- ✅ 增加 'C'
  AND STATUS = 'ENABLED'
  AND CONSTRAINT_NAME NOT LIKE 'SYS_%'  -- 排除系统生成的 NOT NULL 约束
```

**业务场景**:
```sql
-- Oracle 源端
CREATE TABLE EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    SALARY NUMBER CHECK (SALARY > 0),
    AGE NUMBER CHECK (AGE BETWEEN 18 AND 65)
);

-- 当前工具行为：
-- ❌ CHECK 约束未被收集和对比
-- ❌ OB 端可能缺少 SALARY > 0 和 AGE BETWEEN 18 AND 65 的校验

-- 正确行为：
-- ✅ 识别缺失的 CHECK 约束
-- ✅ 生成 DDL: ALTER TABLE EMPLOYEES ADD CONSTRAINT CHK_SALARY CHECK (SALARY > 0);
```

### 1.2 OceanBase 侧元数据查询

#### ✅ 正确处理的场景

**TYPE BODY 探测** (lines 5338-5357):
```python
# 仅在显式启用 TYPE BODY 检查时，通过 DBA_SOURCE 探测真实 TYPE BODY
if 'TYPE BODY' in object_types_filter:
    sql_type_body_tpl = """
        SELECT OWNER, NAME
        FROM DBA_SOURCE
        WHERE OWNER IN ({owners_in})
          AND TYPE = 'TYPE BODY'
        GROUP BY OWNER, NAME
    """
```

**优点**:
- ✅ 避免了 `DBA_TYPES.TYPECODE` 误判 TYPE BODY 的问题
- ✅ 使用 `DBA_SOURCE` 作为 TYPE BODY 存在性的真实依据

**DBA_TAB_COLUMNS 默认值清洗** (lines 5402-5412):
```python
sql_cols_ext_tpl = """
    SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, NULLABLE,
           REPLACE(REPLACE(REPLACE(DATA_DEFAULT, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ') AS DATA_DEFAULT
    FROM DBA_TAB_COLUMNS
    WHERE OWNER IN ({owners_in})
"""
```

**优点**:
- ✅ 正确清洗默认值中的换行符、回车符、制表符
- ✅ 支持降级查询（清洗失败时回退到基础查询）

#### ✅ 已修复: Oracle 侧 CHAR_USED 已正确获取

**代码实现** (lines 6378-6393):
```python
# Oracle 侧已正确获取 CHAR_USED
SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
       DATA_LENGTH, DATA_PRECISION, DATA_SCALE,
       NULLABLE, DATA_DEFAULT, CHAR_USED, CHAR_LENGTH, HIDDEN_COLUMN
FROM DBA_TAB_COLUMNS
```

**对比逻辑已正确** (lines 8788-8798):
```python
src_char_used = (src_info.get("char_used") or "").strip().upper()
if src_char_used == 'C':
    # CHAR语义：要求长度完全一致
    if tgt_len_int != src_len_int:
        length_mismatches.append(...)
else:
    # BYTE语义：需要放大1.5倍
```

#### ❌ 新发现问题 4: OB 侧 CHAR_USED 未获取 (已确认)

**问题描述**:
```python
# lines 5437-5439: OB 侧列定义解析
tab_columns.setdefault(key, {})[col] = {
    "data_type": dtype,
    "char_length": int(char_len) if char_len.isdigit() else None,
    "nullable": nullable,
    "data_default": default,
    "hidden": False
}
```

**隐藏逻辑**:
- OB 的 `DBA_TAB_COLUMNS.CHAR_LENGTH` 字段含义与 Oracle 不同：
  - Oracle: 仅 VARCHAR2/CHAR 类型有意义，表示字符长度
  - OceanBase: 可能返回字节长度或字符长度，取决于版本和配置
- `CHAR_USED` 字段未在 OB 查询中获取

**影响**:
```python
# lines 8778-8808: VARCHAR 长度对比逻辑
if src_dtype in ('VARCHAR2', 'VARCHAR'):
    src_len = src_info.get("char_length") or src_info.get("data_length")
    tgt_len = tgt_info.get("char_length") or tgt_info.get("data_length")
    
    # 区分BYTE和CHAR语义：CHAR_USED='C'表示CHAR语义
    src_char_used = (src_info.get("char_used") or "").strip().upper()
    
    if src_char_used == 'C':
        # CHAR语义：要求长度完全一致
        if tgt_len_int != src_len_int:
            length_mismatches.append(...)
    else:
        # BYTE语义：需要放大1.5倍
        expected_min_len = int(math.ceil(src_len_int * 1.5))
```

**问题**:
- ❌ OB 侧 `CHAR_USED` 未获取，无法判断目标列的语义
- ❌ 可能导致 CHAR 语义的列在 OB 侧被误判为 BYTE 语义
- ❌ 1.5 倍放大逻辑可能应用到不应该放大的列

**业务场景**:
```sql
-- Oracle 源端
CREATE TABLE T1 (
    COL1 VARCHAR2(100 CHAR),  -- CHAR语义，中文字符
    COL2 VARCHAR2(100 BYTE)   -- BYTE语义
);

-- OceanBase 目标端
CREATE TABLE T1 (
    COL1 VARCHAR2(100 CHAR),  -- 正确迁移
    COL2 VARCHAR2(150)        -- OMS 放大 1.5 倍
);

-- 当前工具行为：
-- ❌ 无法从 OB 端获取 CHAR_USED，不知道 COL1 是 CHAR 语义
-- ❌ 可能将 COL1 的 100 与 100 对比，判定为正确（实际应完全一致）
-- ❌ 可能将 COL2 的 100 与 150 对比，判定为符合 1.5 倍规则（但无法确认 OB 是 BYTE 还是 CHAR）
```

**修复建议**:
```python
# OB 侧也应获取 CHAR_USED 字段
sql_cols_ext_tpl = """
    SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, 
           CHAR_LENGTH, DATA_LENGTH, CHAR_USED, NULLABLE,
           REPLACE(REPLACE(REPLACE(DATA_DEFAULT, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ') AS DATA_DEFAULT
    FROM DBA_TAB_COLUMNS
    WHERE OWNER IN ({owners_in})
"""

# 对比时同时检查两侧的 CHAR_USED
if src_char_used == 'C' and tgt_char_used == 'C':
    # 双方都是 CHAR 语义，要求完全一致
    if tgt_len_int != src_len_int:
        length_mismatches.append(...)
elif src_char_used == 'B' and tgt_char_used == 'B':
    # 双方都是 BYTE 语义，应用 1.5 倍规则
    expected_min_len = int(math.ceil(src_len_int * 1.5))
    ...
else:
    # 语义不一致，报告为不匹配
    length_mismatches.append(ColumnLengthIssue(..., 'semantics_mismatch'))
```

---

## 🔍 2. 对比逻辑准确性审查

### 2.1 表结构对比

#### ✅ 正确实现的逻辑

**忽略 OMS 系统列** (lines 8738-8744):
```python
src_col_names = {
    col for col, meta in src_cols_details.items()
    if not is_ignored_source_column(col, meta)
}
tgt_col_names = {
    col for col, meta in tgt_cols_details.items()
    if not is_ignored_oms_column(col, meta)
}
```

**优点**:
- ✅ 正确过滤 `OMS_OBJECT_NUMBER`, `OMS_RELATIVE_FNO`, `OMS_BLOCK_NUMBER`, `OMS_ROW_NUMBER`
- ✅ 区分源端忽略列和目标端忽略列

**LONG/LONG RAW 类型映射** (lines 8765-8776):
```python
if is_long_type(src_dtype):
    expected_type = map_long_type_to_ob(src_dtype)
    if (tgt_dtype or "UNKNOWN") != expected_type:
        type_mismatches.append(
            ColumnTypeIssue(
                col_name,
                src_dtype or "UNKNOWN",
                tgt_dtype or "UNKNOWN",
                expected_type
            )
        )
```

**优点**:
- ✅ 正确要求 LONG → CLOB, LONG RAW → BLOB 的类型映射

#### ❌ 关键问题 5: NUMBER 精度和标度未对比 (已确认)

**问题描述**:
```python
# lines 8756-8809: 列对比逻辑
for col_name in common_cols:
    src_info = src_cols_details[col_name]
    tgt_info = tgt_cols_details[col_name]
    
    src_dtype = (src_info.get("data_type") or "").upper()
    tgt_dtype = (tgt_info.get("data_type") or "").upper()
    
    # ❌ 仅检查 LONG 类型和 VARCHAR 长度，未检查 NUMBER 精度和标度
```

**影响**:
- NUMBER(10,2) 与 NUMBER(5,2) 不会被检测为不匹配
- NUMBER(10,2) 与 NUMBER(10,4) 不会被检测为不匹配
- 可能导致数据溢出或精度丢失

**业务场景**:
```sql
-- Oracle 源端
CREATE TABLE PRODUCTS (
    PRODUCT_ID NUMBER(10),
    PRICE NUMBER(10,2),     -- 最大 99999999.99
    WEIGHT NUMBER(8,3)      -- 最大 99999.999
);

-- OceanBase 目标端（错误迁移）
CREATE TABLE PRODUCTS (
    PRODUCT_ID NUMBER(5),   -- ❌ 精度不足
    PRICE NUMBER(10,4),     -- ❌ 标度不一致
    WEIGHT NUMBER           -- ❌ 无精度限制
);

-- 当前工具行为：
-- ❌ 上述不匹配不会被检测
-- ❌ 可能导致：
--    - PRODUCT_ID 插入超过 99999 的值时溢出
--    - PRICE 的小数位数与业务预期不符
--    - WEIGHT 没有精度约束，可能存储超大数值
```

**修复建议**:
```python
# 增加 NUMBER 类型的精度和标度检查
if src_dtype == 'NUMBER' and tgt_dtype == 'NUMBER':
    src_precision = src_info.get("data_precision")
    src_scale = src_info.get("data_scale")
    tgt_precision = tgt_info.get("data_precision")
    tgt_scale = tgt_info.get("data_scale")
    
    # NUMBER(*,0) 和 NUMBER 视为等价
    # NUMBER(p,s) 要求目标端 p >= 源端 p, s = 源端 s
    if src_precision is not None:
        if tgt_precision is None:
            # 目标端无精度限制，警告但不报错
            log.warning(f"列 {col_name}: 源端 NUMBER({src_precision},{src_scale or 0})，"
                       f"目标端 NUMBER 无精度限制")
        else:
            if tgt_precision < src_precision:
                type_mismatches.append(
                    ColumnTypeIssue(col_name, f"NUMBER({src_precision},{src_scale})",
                                   f"NUMBER({tgt_precision},{tgt_scale})", 
                                   f"NUMBER({src_precision},{src_scale})")
                )
            if (src_scale or 0) != (tgt_scale or 0):
                type_mismatches.append(
                    ColumnTypeIssue(col_name, f"NUMBER({src_precision},{src_scale})",
                                   f"NUMBER({tgt_precision},{tgt_scale})", 
                                   f"NUMBER({src_precision},{src_scale})")
                )
```

#### ⚠️ 次要问题 6: DATE/TIMESTAMP 类型精度未对比

**问题描述**:
- TIMESTAMP(6) 与 TIMESTAMP(3) 的精度差异未检测
- TIMESTAMP WITH TIME ZONE 与 TIMESTAMP 的类型差异可能未正确识别

**业务场景**:
```sql
-- Oracle 源端
CREATE TABLE EVENTS (
    EVENT_ID NUMBER,
    EVENT_TIME TIMESTAMP(9),           -- 纳秒精度
    CREATE_TIME TIMESTAMP(6),          -- 微秒精度
    UPDATE_TIME TIMESTAMP WITH TIME ZONE
);

-- OceanBase 目标端
CREATE TABLE EVENTS (
    EVENT_ID NUMBER,
    EVENT_TIME TIMESTAMP(3),           -- ❌ 仅毫秒精度
    CREATE_TIME TIMESTAMP,             -- ❌ 默认精度（通常6）
    UPDATE_TIME TIMESTAMP              -- ❌ 缺少 TIME ZONE
);

-- 影响：
-- ❌ 微秒/纳秒级时间戳精度丢失
-- ❌ 时区信息丢失
```

### 2.2 索引对比

#### ⚠️ 次要问题 7: 索引列顺序对比待验证

**问题描述**:
从代码中未见到索引列顺序的严格对比逻辑。

**业务场景**:
```sql
-- Oracle 源端
CREATE INDEX IDX_CUST ON CUSTOMERS (CITY, CUSTOMER_NAME);

-- OceanBase 目标端（列顺序错误）
CREATE INDEX IDX_CUST ON CUSTOMERS (CUSTOMER_NAME, CITY);

-- 影响：
-- ❌ 索引列顺序不同，查询性能可能大幅下降
-- ❌ 某些查询可能无法使用索引
```

**修复建议**:
```python
# 在索引对比逻辑中增加列顺序检查
src_idx_cols = oracle_meta.indexes.get((src_schema, src_table), {}).get(idx_name, {}).get("columns", [])
tgt_idx_cols = ob_meta.indexes.get((tgt_schema, tgt_table), {}).get(idx_name, {}).get("columns", [])

if src_idx_cols != tgt_idx_cols:
    # 列顺序或列名不一致
    extra_mismatches.append(("INDEX", idx_name, "列顺序不一致", src_idx_cols, tgt_idx_cols))
```

### 2.3 约束对比

#### ❌ 关键问题 8: 外键 ON DELETE/ON UPDATE 规则未收集 (已确认)

**问题描述**:
当前代码收集了外键的引用表信息，但未收集 `DELETE_RULE` 和 `UPDATE_RULE`。

**业务场景**:
```sql
-- Oracle 源端
ALTER TABLE ORDERS ADD CONSTRAINT FK_CUST
    FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID)
    ON DELETE CASCADE;

-- OceanBase 目标端（规则缺失）
ALTER TABLE ORDERS ADD CONSTRAINT FK_CUST
    FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID);
    -- ❌ 缺少 ON DELETE CASCADE

-- 影响：
-- ❌ 删除客户时，订单不会级联删除
-- ❌ 业务逻辑错误，可能产生孤儿数据
```

**修复建议**:
```python
# DBA_CONSTRAINTS 查询增加 DELETE_RULE
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE,
       R_OWNER, R_CONSTRAINT_NAME, DELETE_RULE
FROM DBA_CONSTRAINTS
WHERE OWNER IN ({owners_clause})
  AND CONSTRAINT_TYPE IN ('P','U','R','C')
  AND STATUS = 'ENABLED'
```

---

## 🔍 3. DDL 生成逻辑审查

### 3.1 VIEW DDL 清洗

#### ✅ 正确实现的清洗逻辑

**Hint 过滤** (代码中引用了 `DDL_HINT_POLICY`):
- ✅ 支持多种 Hint 策略：`keep_all`, `keep_supported`, `remove_all`, `report_only`
- ✅ 支持白名单和黑名单配置

**特殊语法兼容性清洗** (function `clean_view_ddl_for_oceanbase`):
```python
def clean_view_ddl_for_oceanbase(ddl: str, ob_version: Optional[str] = None) -> str:
    """
    清理Oracle VIEW DDL，使其兼容OceanBase
    """
```

**优点**:
- ✅ 统一的 VIEW DDL 清洗入口

#### ⚠️ 潜在问题 9: DBMS_METADATA 生成的 DDL 可能包含不兼容语法

**问题描述**:
VIEW DDL 通过 `DBMS_METADATA.GET_DDL` 获取，但 Oracle 生成的 DDL 可能包含 OB 不支持的语法。

**常见不兼容语法**:
1. **FORCE VIEW** - OB 可能不支持
2. **EDITIONING VIEW** - OB 不支持
3. **BEQUEATH CURRENT_USER / DEFINER** - OB 支持程度未知
4. **特定函数** - 如 `LISTAGG` 的某些用法

**业务场景**:
```sql
-- Oracle DBMS_METADATA 生成的 DDL
CREATE OR REPLACE FORCE EDITIONING VIEW MY_VIEW
BEQUEATH CURRENT_USER
AS
SELECT /*+ PARALLEL(4) */ 
       LISTAGG(NAME, ',') WITHIN GROUP (ORDER BY ID) AS NAME_LIST
FROM EMPLOYEES
GROUP BY DEPARTMENT_ID;

-- 可能的问题：
-- ❌ FORCE 关键字可能被 OB 拒绝
-- ❌ EDITIONING 不被 OB 支持
-- ❌ BEQUEATH CURRENT_USER 语法未知
-- ❌ PARALLEL Hint 可能需要过滤
-- ❌ LISTAGG 在某些 OB 版本可能不支持
```

**修复建议**:
```python
def clean_view_ddl_for_oceanbase(ddl: str, ob_version: Optional[str] = None) -> str:
    # 移除 FORCE 关键字
    ddl = re.sub(r'\bFORCE\s+', '', ddl, flags=re.IGNORECASE)
    
    # 移除 EDITIONING 关键字
    ddl = re.sub(r'\bEDITIONING\s+', '', ddl, flags=re.IGNORECASE)
    
    # 移除 BEQUEATH 子句
    ddl = re.sub(r'\bBEQUEATH\s+(CURRENT_USER|DEFINER)\s+', '', ddl, flags=re.IGNORECASE)
    
    # 其他清洗逻辑...
    return ddl
```

### 3.2 PLSQL DDL 生成

#### ⚠️ 潜在问题 10: PACKAGE 依赖顺序未保证

**问题描述**:
PACKAGE BODY 依赖于 PACKAGE，但 DDL 生成和执行顺序可能未严格保证。

**业务场景**:
```sql
-- 正确顺序：
1. CREATE OR REPLACE PACKAGE MY_PKG AS ...
2. CREATE OR REPLACE PACKAGE BODY MY_PKG AS ...

-- 错误顺序可能导致：
-- ❌ PACKAGE BODY 先执行，报错 "PACKAGE 不存在"
```

**当前 run_fixup 的处理**:
```python
# lines 625-629 in run_fixup.py
priority = [
    "sequence", "table", "table_alter", "constraint", "index",
    "view", "materialized_view", "synonym", "procedure", "function",
    "package", "package_body", "type", "type_body", "trigger",
    "job", "schedule", "grants",
]
```

**优点**:
- ✅ `package` 在 `package_body` 之前
- ✅ `type` 在 `type_body` 之前

**潜在风险**:
- ⚠️ 如果 PACKAGE A 依赖于 PACKAGE B，仅靠目录优先级无法保证 B 先执行
- ⚠️ 需要真正的拓扑排序

---

## 🔍 4. run_fixup 依赖排序审查

### 4.1 当前实现

#### ✅ 分层执行逻辑

**DEPENDENCY_LAYERS** (未在提供的代码中看到完整定义，但从 lines 591-603 推断):
```python
# 推断的分层结构
DEPENDENCY_LAYERS = [
    ["grants_users", "grants_roles"],  # Layer 0: 用户和角色授权优先
    ["sequence", "table"],              # Layer 1: 序列和表
    ["table_alter", "constraint"],      # Layer 2: 表修改和约束
    ["index"],                          # Layer 3: 索引
    ["view", "materialized_view"],      # Layer 4: 视图
    ["synonym"],                        # Layer 5: 同义词
    ["procedure", "function", "package", "type"],  # Layer 6: PL/SQL对象
    ["package_body", "type_body"],      # Layer 7: PL/SQL BODY
    ["trigger"],                        # Layer 8: 触发器
    ["grants"]                          # Layer 9: 对象授权
]
```

**优点**:
- ✅ 合理的分层结构
- ✅ 基础对象（表、序列）优先于依赖对象（视图、PL/SQL）
- ✅ 授权在合适的位置（用户授权最先，对象授权最后）

#### ✅ 已实现: VIEW 依赖拓扑排序 (lines 16368-16437)

**代码实现**:
```python
# Step 3: Topological sort using Kahn's algorithm
from collections import deque
in_degree = defaultdict(int)
dep_graph = defaultdict(set)

# Build dependency graph
for view, deps in view_deps.items():
    for dep in deps:
        dep_graph[dep].add(view)
        in_degree[view] += 1

# Kahn's algorithm
queue = deque([v for v in view_deps if in_degree[v] == 0])
sorted_view_tuples = []

while queue:
    current = queue.popleft()
    sorted_view_tuples.append(current)
    for dependent in dep_graph[current]:
        in_degree[dependent] -= 1
        if in_degree[dependent] == 0:
            queue.append(dependent)
```

**优点**:
- ✅ 使用 Kahn 算法实现拓扑排序
- ✅ 有循环依赖检测 (line 16433-16436)
- ✅ VIEW 执行顺序问题已解决

#### ⚠️ 残留问题 11: PACKAGE 层内顺序未保证

**问题描述**:
同一层内的对象按文件名排序执行，未考虑层内依赖关系。

**业务场景**:
```sql
-- 同一层内的 VIEW 之间有依赖
CREATE OR REPLACE VIEW V_BASE AS SELECT * FROM ORDERS;
CREATE OR REPLACE VIEW V_DERIVED AS SELECT * FROM V_BASE WHERE AMOUNT > 1000;

-- 文件名：
-- view/SCHEMA.V_DERIVED.sql  (字母序靠前)
-- view/SCHEMA.V_BASE.sql     (字母序靠后)

-- 当前工具行为：
-- ❌ 按文件名排序，V_DERIVED 先执行
-- ❌ 执行失败："V_BASE does not exist"

-- 正确行为：
-- ✅ 应先执行 V_BASE，再执行 V_DERIVED
```

**修复建议**:
```python
# 增加 VIEW 依赖链的自动检测和排序
def build_view_dependencies(view_scripts: List[Path]) -> Dict[str, Set[str]]:
    """
    解析 VIEW DDL，提取依赖关系
    """
    deps: Dict[str, Set[str]] = {}
    for script in view_scripts:
        ddl = script.read_text()
        view_name = extract_view_name_from_ddl(ddl)
        referenced_views = extract_referenced_objects_from_ddl(ddl)
        deps[view_name] = referenced_views
    return deps

def topological_sort_views(view_scripts: List[Path]) -> List[Path]:
    """
    拓扑排序 VIEW 脚本
    """
    deps = build_view_dependencies(view_scripts)
    # 实现 Kahn 算法或 DFS 拓扑排序
    ...
```

#### ✅ 已实现: 循环依赖检测 (line 16433-16436)

**代码实现**:
```python
# Check for cycles
if len(sorted_view_tuples) < len(view_deps):
    circular = [v for v, d in in_degree.items() if d > 0]
    log.warning(f"[FIXUP] 发现 {len(circular)} 个循环依赖的VIEW，将最后创建")
    sorted_view_tuples.extend(circular)
```

**优点**:
- ✅ 检测到循环依赖
- ✅ 将循环依赖的 VIEW 放到最后创建
- ✅ 有警告日志

**局限**:
- ⚠️ 循环依赖的 VIEW 仍可能执行失败（需要 FORCE 关键字）

**问题描述**:
代码中有循环依赖检测的提及，但未看到完整的处理逻辑。

**业务场景**:
```sql
-- 循环依赖场景（通过 FORCE 创建）
CREATE OR REPLACE FORCE VIEW V1 AS SELECT * FROM V2;
CREATE OR REPLACE FORCE VIEW V2 AS SELECT * FROM V1;

-- 当前工具行为：
-- ❌ 可能陷入死循环或执行失败
-- ❌ 未生成循环依赖报告

-- 正确行为：
-- ✅ 检测到循环依赖
-- ✅ 报告: "V1 <-> V2 形成循环依赖"
-- ✅ 使用 FORCE 关键字或其他策略解决
```

**修复建议**:
```python
def detect_cycles(deps: Dict[str, Set[str]]) -> List[List[str]]:
    """
    检测循环依赖
    返回: [[V1, V2], [V3, V4, V5]] 表示两个循环
    """
    visited = set()
    rec_stack = set()
    cycles = []
    
    def dfs(node, path):
        if node in rec_stack:
            # 找到循环
            cycle_start = path.index(node)
            cycles.append(path[cycle_start:])
            return
        if node in visited:
            return
        
        visited.add(node)
        rec_stack.add(node)
        path.append(node)
        
        for neighbor in deps.get(node, set()):
            dfs(neighbor, path[:])
        
        rec_stack.remove(node)
    
    for node in deps:
        if node not in visited:
            dfs(node, [])
    
    return cycles
```

### 4.2 错误分类和重试

#### ✅ 正确实现的错误分类

**FailureType 枚举** (lines 136-146):
```python
class FailureType:
    MISSING_OBJECT = "missing_object"        # 可重试
    PERMISSION_DENIED = "permission_denied"  # 需要授权
    SYNTAX_ERROR = "syntax_error"            # 需要修复 DDL
    DATA_CONFLICT = "data_conflict"          # 需要数据清理
    DUPLICATE_OBJECT = "duplicate_object"    # 可跳过
    INVALID_IDENTIFIER = "invalid_identifier" # 需要修复 DDL
    NAME_IN_USE = "name_in_use"              # 需要解决冲突
    TIMEOUT = "timeout"                       # 可重试
    UNKNOWN = "unknown"                       # 需要调查
```

**classify_sql_error 函数** (lines 149-191):
```python
def classify_sql_error(stderr: str) -> str:
    """
    根据错误消息分类
    """
    if 'ORA-00942' in stderr_upper or 'TABLE OR VIEW DOES NOT EXIST' in stderr_upper:
        return FailureType.MISSING_OBJECT
    if 'ORA-01031' in stderr_upper or 'INSUFFICIENT PRIVILEGES' in stderr_upper:
        return FailureType.PERMISSION_DENIED
    # ...
```

**优点**:
- ✅ 智能错误分类
- ✅ 支持可重试错误的识别
- ✅ 为迭代执行提供基础

#### ⚠️ 潜在问题 13: 重试策略未考虑依赖顺序

**问题描述**:
即使识别了 `MISSING_OBJECT` 错误，重试时仍可能按原顺序执行。

**业务场景**:
```sql
-- 第一次执行顺序（错误）:
1. CREATE VIEW V_DERIVED ... (失败: V_BASE不存在)
2. CREATE VIEW V_BASE ...    (成功)

-- 第二次重试（仍然错误的顺序）:
1. CREATE VIEW V_DERIVED ... (再次失败: V_BASE不存在)
   -- ❌ 虽然 V_BASE 已存在，但 V_DERIVED 可能在当前会话中看不到

-- 正确做法：
-- ✅ 第一次失败后，重新分析依赖关系
-- ✅ 调整执行顺序: V_BASE -> V_DERIVED
```

---

## 🔍 5. 端到端场景覆盖度

### 5.1 缺少的关键测试场景

#### ❌ 场景 1: 复杂 VIEW 依赖链

**场景描述**:
```sql
-- 5 层 VIEW 依赖
CREATE VIEW V_LEVEL_1 AS SELECT * FROM ORDERS;
CREATE VIEW V_LEVEL_2 AS SELECT * FROM V_LEVEL_1 WHERE AMOUNT > 100;
CREATE VIEW V_LEVEL_3 AS SELECT * FROM V_LEVEL_2 JOIN CUSTOMERS USING (CUSTOMER_ID);
CREATE VIEW V_LEVEL_4 AS SELECT * FROM V_LEVEL_3 WHERE STATUS = 'ACTIVE';
CREATE VIEW V_LEVEL_5 AS SELECT CUSTOMER_NAME, SUM(AMOUNT) FROM V_LEVEL_4 GROUP BY CUSTOMER_NAME;
```

**测试点**:
- 工具能否正确识别依赖链深度？
- DDL 生成顺序是否正确？
- run_fixup 是否按正确顺序执行？
- 中间 VIEW 缺失时，是否能正确报告最底层的缺失？

#### ❌ 场景 2: 循环依赖 VIEW（通过 FORCE 创建）

**场景描述**:
```sql
CREATE OR REPLACE FORCE VIEW V_A AS SELECT * FROM V_B;
CREATE OR REPLACE FORCE VIEW V_B AS SELECT * FROM V_A;
```

**测试点**:
- 工具能否检测到循环依赖？
- 是否生成警告或错误报告？
- DDL 是否包含 FORCE 关键字？
- run_fixup 如何处理循环？

#### ❌ 场景 3: 跨 Schema 依赖

**场景描述**:
```sql
-- Schema A
CREATE TABLE A.ORDERS (...);
CREATE VIEW A.V_ORDERS AS SELECT * FROM A.ORDERS;

-- Schema B (依赖 A.ORDERS)
CREATE VIEW B.V_ALL_ORDERS AS SELECT * FROM A.ORDERS;
CREATE SYNONYM B.SYN_ORDERS FOR A.ORDERS;
CREATE PACKAGE B.PKG_ORDERS AS
    PROCEDURE GET_ORDER(p_id NUMBER);
END;
CREATE PACKAGE BODY B.PKG_ORDERS AS
    PROCEDURE GET_ORDER(p_id NUMBER) IS
        CURSOR c IS SELECT * FROM A.ORDERS WHERE ORDER_ID = p_id;
    BEGIN
        ...
    END;
END;
```

**测试点**:
- Remap 规则能否正确处理跨 Schema 引用？
- B.V_ALL_ORDERS 的 DDL 中，A.ORDERS 是否需要改名？
- B.PKG_ORDERS 的 DDL 是否需要更新引用？
- run_fixup 的执行顺序是否考虑跨 Schema 依赖？

#### ❌ 场景 4: 函数索引和虚拟列

**场景描述**:
```sql
CREATE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER,
    FIRST_NAME VARCHAR2(50),
    LAST_NAME VARCHAR2(50),
    FULL_NAME VARCHAR2(100) GENERATED ALWAYS AS (FIRST_NAME || ' ' || LAST_NAME) VIRTUAL,
    EMAIL VARCHAR2(100)
);

CREATE INDEX IDX_UPPER_EMAIL ON CUSTOMERS (UPPER(EMAIL));
CREATE INDEX IDX_FULL_NAME ON CUSTOMERS (FULL_NAME);
```

**测试点**:
- 虚拟列 FULL_NAME 是否被正确识别？
- 函数索引 IDX_UPPER_EMAIL 的表达式是否正确提取？
- 虚拟列索引 IDX_FULL_NAME 的 DDL 是否正确？

#### ❌ 场景 5: Interval 分区表迁移

**场景描述**:
```sql
-- Oracle 源端
CREATE TABLE SALES (
    SALE_ID NUMBER,
    SALE_DATE DATE,
    AMOUNT NUMBER
)
PARTITION BY RANGE (SALE_DATE)
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
    PARTITION P_2023_01 VALUES LESS THAN (TO_DATE('2023-02-01', 'YYYY-MM-DD'))
);

-- OceanBase 可能不完全支持 INTERVAL 分区
```

**测试点**:
- 工具能否识别 Interval 分区表？
- 是否生成 Interval 分区修复脚本？
- 生成的脚本是否可执行？
- 是否有降级方案（转为普通 RANGE 分区）？

#### ❌ 场景 6: 大量对象的性能

**场景描述**:
- 1000 个表
- 5000 个索引
- 2000 个视图（其中 500 个有多层依赖）
- 100 个 PACKAGE（相互依赖）
- 10000 条授权

**测试点**:
- 元数据收集是否超时？
- 内存使用是否合理？
- 对比逻辑是否在可接受时间内完成？
- DDL 生成是否有性能瓶颈？
- run_fixup 执行效率如何？

#### ❌ 场景 7: CHECK 约束和外键级联规则

**场景描述**:
```sql
CREATE TABLE DEPARTMENTS (
    DEPT_ID NUMBER PRIMARY KEY,
    DEPT_NAME VARCHAR2(50) CHECK (DEPT_NAME IS NOT NULL),
    BUDGET NUMBER CHECK (BUDGET > 0)
);

CREATE TABLE EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    DEPT_ID NUMBER,
    SALARY NUMBER CHECK (SALARY BETWEEN 1000 AND 100000),
    CONSTRAINT FK_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID) 
        ON DELETE CASCADE
);
```

**测试点**:
- CHECK 约束是否被收集和对比？
- ON DELETE CASCADE 规则是否被识别？
- 缺失的约束是否生成正确的 DDL？

---

## 📊 6. 总结与建议

### 6.1 高优先级修复项 (基于实际代码验证)

| 编号 | 问题 | 状态 | 严重程度 | 业务影响 | 修复难度 |
|------|------|------|----------|----------|----------|
| 1 | 虚拟列未识别 | ❌ 确认 | 高 | 虚拟列 DDL 错误，数据完整性问题 | 中 |
| 2 | 函数索引表达式未提取 | ❌ 确认 | 中 | 索引 DDL 错误，性能问题 | 中 |
| 3 | CHECK 约束未收集 | ❌ 确认 | **高** | **业务规则缺失，数据质量无保障** | 低 |
| 5 | NUMBER 精度/标度未对比 | ❌ 确认 | 高 | 数据溢出或精度丢失 | 低 |
| 8 | 外键 DELETE_RULE 未收集 | ❌ 确认 | 高 | 业务逻辑错误，产生孤儿数据 | 中 |
| **NEW** | **OB 侧 CHAR_USED 未获取** | ❌ 确认 | **高** | **无法判断目标列语义，误判长度** | 中 |
| **NEW** | **内存风险：全量加载** | ❌ 确认 | **中** | **大量对象时可能 OOM** | 中 |
| **NEW** | **单一 timeout 策略** | ❌ 确认 | 中 | 大表查询超时，元数据收集失败 | 低 |

### 6.2 已修复项 (代码验证)

| 编号 | 问题 | 状态 | 代码位置 |
|------|------|------|----------|
| 4 | Oracle 侧 CHAR_USED | ✅ 已实现 | lines 6378-6393, 8788-8798 |
| 11 | VIEW 拓扑排序 | ✅ 已实现 | lines 16368-16437 (Kahn 算法) |
| 12 | 循环依赖检测 | ✅ 已实现 | lines 16433-16436 |

### 6.3 次要改进项

| 编号 | 问题 | 严重程度 | 业务影响 | 修复难度 |
|------|------|----------|----------|----------|
| 6 | TIMESTAMP 精度未对比 | 低 | 时间精度丢失（罕见） | 低 |
| 7 | 索引列顺序对比待验证 | 低 | 查询性能下降（待确认） | 低 |
| 9 | DBMS_METADATA 语法兼容性 | 中 | VIEW DDL 执行失败 | 中 |
| 10 | PACKAGE 层内顺序 | 低 | 已有分层，层内小概率问题 | 低 |

### 6.3 建议的测试用例优先级

1. **P0 - 核心功能测试**:
   - 虚拟列和函数索引场景
   - CHECK 约束和外键级联规则
   - VARCHAR CHAR/BYTE 语义完整测试
   - NUMBER 精度和标度边界测试

2. **P1 - 复杂场景测试**:
   - 多层 VIEW 依赖链（3-5 层）
   - 跨 Schema 依赖和 Remap
   - PACKAGE 相互依赖

3. **P2 - 边界和性能测试**:
   - 循环依赖检测
   - 大数据量性能测试（1000+ 表）
   - Interval 分区特殊处理

### 6.4 工具增强建议

1. **元数据收集增强**:
   ```python
   # 增加查询字段
   - DBA_TAB_COLUMNS: + VIRTUAL_COLUMN
   - DBA_CONSTRAINTS: + SEARCH_CONDITION, DELETE_RULE
   - DBA_IND_EXPRESSIONS: 完整查询
   ```

2. **对比逻辑增强**:
   ```python
   # 增加对比维度
   - NUMBER 精度和标度
   - TIMESTAMP 精度
   - 索引列顺序
   - 约束规则（CASCADE等）
   ```

3. **依赖分析增强**:
   ```python
   # 实现完整拓扑排序
   def topological_sort_with_cycle_detection(objects, dependencies):
       # Kahn 算法 + 环检测
       # 返回: (sorted_objects, cycles)
   ```

4. **测试框架建立**:
   ```python
   # 端到端测试框架
   tests/
   ├── integration/
   │   ├── test_view_dependencies.py
   │   ├── test_cross_schema.py
   │   └── test_interval_partitions.py
   ├── scenarios/
   │   ├── complex_dependencies.sql
   │   └── virtual_columns.sql
   └── fixtures/
       └── sample_metadata.json
   ```

---

## 📝 结论

`ob_comparator` 工具在元数据收集和基础对比逻辑上已有坚实基础，但在以下关键业务场景中存在功能缺失或逻辑不完善：

1. **数据完整性保障不足**: CHECK 约束、虚拟列、精度标度等影响数据完整性的元素未完整处理
2. **依赖关系处理欠佳**: 层内拓扑排序缺失，循环依赖未处理
3. **类型兼容性判定不精确**: VARCHAR 语义、NUMBER 精度、TIMESTAMP 精度等细节未严格对比
4. **测试覆盖不足**: 缺少端到端的复杂场景测试

建议按照高优先级修复项逐步改进，并建立完善的端到端测试框架，确保工具在真实迁移场景中的可靠性。

**评分**: 7.5/10 → **生产环境适用性 7.0/10**  
**主要优势**: 
- ✅ 架构清晰，"一次转储，本地对比"性能优秀
- ✅ 元数据收集全面（支持 CHAR_USED/PRECISION/SCALE/HIDDEN_COLUMN）
- ✅ VIEW 拓扑排序和循环依赖检测已实现
- ✅ 基础对比逻辑正确

**主要不足（生产环境关键风险）**: 
- ❌ **CHECK 约束和外键级联规则缺失** - 业务规则无法保障
- ❌ **OB 侧 CHAR_USED 未获取** - 无法正确判断目标列语义
- ❌ **NUMBER 精度/标度未对比** - 数据溢出风险
- ❌ **虚拟列和函数索引未完整处理** - DDL 生成错误
- ⚠️ **内存风险** - 全量加载元数据，大量对象时可能 OOM
- ⚠️ **单一 timeout** - 大表场景可能超时失败
