# 生产环境关键风险分析 - schema_diff_reconciler.py

**工具版本**: V0.9.8  
**分析日期**: 2026-01-20  
**分析范围**: 基于实际代码的生产环境适用性深度审查

---

## 🚨 严重风险 (P0 - 必须解决)

### 1. CHECK 约束完全缺失

**代码位置**: lines 6494-6499, 5586-5590

**问题**:
```python
# Oracle 和 OceanBase 侧都仅收集 P/U/R 约束
CONSTRAINT_TYPE IN ('P','U','R')  # ❌ 缺少 'C'
```

**影响**:
- ❌ CHECK 约束是业务规则的核心保障
- ❌ 迁移后数据质量无法保证
- ❌ 可能插入不符合业务规则的脏数据

**业务场景**:
```sql
-- Oracle 源端
CREATE TABLE EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    SALARY NUMBER CHECK (SALARY > 0 AND SALARY < 1000000),
    AGE NUMBER CHECK (AGE BETWEEN 18 AND 65),
    STATUS VARCHAR2(10) CHECK (STATUS IN ('ACTIVE', 'INACTIVE', 'SUSPENDED'))
);

-- 当前工具行为：
-- ❌ 上述 3 个 CHECK 约束完全不会被检测
-- ❌ OB 端即使缺少这些约束，也不会报告
-- ❌ 可能插入 SALARY = -1000 或 AGE = 5 的非法数据
```

**修复建议**:
```python
# 1. 修改 SQL 增加 'C'
CONSTRAINT_TYPE IN ('P','U','R','C')

# 2. 增加 SEARCH_CONDITION 字段
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, 
       R_OWNER, R_CONSTRAINT_NAME, SEARCH_CONDITION
FROM DBA_CONSTRAINTS

# 3. 对比逻辑中增加 CHECK 约束处理
# 4. DDL 生成中包含 CHECK 约束
```

**风险等级**: 🔴 **严重** - 数据完整性无保障  
**修复难度**: 低  
**修复优先级**: P0

---

### 2. 外键级联规则 (ON DELETE/UPDATE) 缺失

**代码位置**: lines 6494-6522, 5586-5607

**问题**:
```python
# 未收集 DELETE_RULE, UPDATE_RULE 字段
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, R_OWNER, R_CONSTRAINT_NAME
FROM DBA_CONSTRAINTS
-- ❌ 缺少 DELETE_RULE
```

**影响**:
- ❌ 无法检测 ON DELETE CASCADE/SET NULL 等规则
- ❌ 业务逻辑严重偏差，可能产生孤儿数据
- ❌ 级联删除失效，导致数据不一致

**业务场景**:
```sql
-- Oracle 源端
CREATE TABLE DEPARTMENTS (DEPT_ID NUMBER PRIMARY KEY, ...);
CREATE TABLE EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    DEPT_ID NUMBER,
    CONSTRAINT FK_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID) 
        ON DELETE CASCADE  -- ❌ 此规则不会被检测
);

-- 当前工具行为：
-- ✅ 能检测外键存在
-- ❌ 不能检测 ON DELETE CASCADE
-- ❌ OB 端可能缺少级联删除，删除部门时员工记录变成孤儿数据
```

**修复建议**:
```python
# 增加 DELETE_RULE 字段
SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE,
       R_OWNER, R_CONSTRAINT_NAME, DELETE_RULE
FROM DBA_CONSTRAINTS
```

**风险等级**: 🔴 **严重** - 业务逻辑错误  
**修复难度**: 低  
**修复优先级**: P0

---

### 3. OB 侧 CHAR_USED 字段缺失

**代码位置**: lines 5402-5443

**问题**:
```python
# OceanBase 查询未包含 CHAR_USED
sql_cols_ext_tpl = """
    SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, NULLABLE, DATA_DEFAULT
    FROM DBA_TAB_COLUMNS
    WHERE OWNER IN ({owners_in})
"""
# ❌ 缺少 CHAR_USED 字段
```

**影响**:
- ❌ 无法判断 OB 端列是 CHAR 还是 BYTE 语义
- ❌ VARCHAR 长度对比逻辑失效
- ❌ 可能误判长度不匹配或漏判实际不匹配

**业务场景**:
```sql
-- Oracle 源端
CREATE TABLE T1 (
    COL1 VARCHAR2(100 CHAR),  -- CHAR 语义
    COL2 VARCHAR2(100 BYTE)   -- BYTE 语义
);

-- OceanBase 目标端
CREATE TABLE T1 (
    COL1 VARCHAR2(100 CHAR),  -- 正确
    COL2 VARCHAR2(150)        -- OMS 放大 1.5 倍
);

-- 当前工具行为：
-- ❌ 无法从 OB 获取 CHAR_USED，不知道 COL1 是 CHAR 还是 BYTE
-- ❌ 可能将 COL1 误判为需要放大 1.5 倍
-- ❌ 或者将实际不匹配的列判定为正确
```

**修复建议**:
```python
# OB 侧也获取 CHAR_USED
sql_cols_ext_tpl = """
    SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, 
           CHAR_LENGTH, DATA_LENGTH, CHAR_USED, NULLABLE, DATA_DEFAULT
    FROM DBA_TAB_COLUMNS
    WHERE OWNER IN ({owners_in})
"""

# 对比时检查双方的 CHAR_USED
if src_char_used != tgt_char_used:
    # 语义不一致，报告为不匹配
```

**风险等级**: 🔴 **严重** - 数据截断风险  
**修复难度**: 中  
**修复优先级**: P0

---

### 4. NUMBER 精度和标度未对比

**代码位置**: lines 8756-8809

**问题**:
```python
# 列对比逻辑仅检查 LONG 类型和 VARCHAR 长度
for col_name in common_cols:
    src_info = src_cols_details[col_name]
    tgt_info = tgt_cols_details[col_name]
    # ❌ 未检查 NUMBER 的 DATA_PRECISION 和 DATA_SCALE
```

**影响**:
- ❌ NUMBER(10,2) vs NUMBER(5,2) 不会被检测
- ❌ 数据溢出风险：大数值无法插入
- ❌ 精度丢失：小数位数不一致

**业务场景**:
```sql
-- Oracle 源端
CREATE TABLE PRODUCTS (
    PRODUCT_ID NUMBER(10),     -- 最大 9999999999
    PRICE NUMBER(10,2),        -- 最大 99999999.99
    WEIGHT NUMBER(8,3)         -- 最大 99999.999
);

-- OB 端（错误迁移）
CREATE TABLE PRODUCTS (
    PRODUCT_ID NUMBER(5),      -- ❌ 最大仅 99999
    PRICE NUMBER(10,4),        -- ❌ 小数位 4 位
    WEIGHT NUMBER              -- ❌ 无限制
);

-- 当前工具行为：
-- ❌ 上述不匹配不会被检测
-- ❌ PRODUCT_ID 插入超过 99999 的值时溢出
-- ❌ PRICE 小数位与业务预期不符
```

**修复建议**:
```python
if src_dtype == 'NUMBER' and tgt_dtype == 'NUMBER':
    src_precision = src_info.get("data_precision")
    src_scale = src_info.get("data_scale")
    tgt_precision = tgt_info.get("data_precision")
    tgt_scale = tgt_info.get("data_scale")
    
    if src_precision and tgt_precision:
        if tgt_precision < src_precision:
            # 精度不足
            type_mismatches.append(...)
        if (src_scale or 0) != (tgt_scale or 0):
            # 标度不一致
            type_mismatches.append(...)
```

**风险等级**: 🔴 **严重** - 数据溢出或精度丢失  
**修复难度**: 低  
**修复优先级**: P0

---

## ⚠️ 高风险 (P1 - 强烈建议解决)

### 5. 虚拟列未识别

**代码位置**: lines 6373-6395

**问题**:
```python
# Oracle 查询未包含 VIRTUAL_COLUMN 字段
SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
       DATA_LENGTH, DATA_PRECISION, DATA_SCALE,
       NULLABLE, DATA_DEFAULT, CHAR_USED, CHAR_LENGTH, HIDDEN_COLUMN
FROM DBA_TAB_COLUMNS
-- ❌ 缺少 VIRTUAL_COLUMN
```

**影响**:
- ❌ 虚拟列被误判为缺失的普通列
- ❌ 生成的 DDL 缺少 GENERATED ALWAYS AS 子句
- ❌ OB 端创建的列不是虚拟列，浪费存储空间

**修复建议**:
```python
# 增加 VIRTUAL_COLUMN 字段
SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
       DATA_LENGTH, DATA_PRECISION, DATA_SCALE,
       NULLABLE, DATA_DEFAULT, CHAR_USED, CHAR_LENGTH,
       NVL(TO_CHAR(HIDDEN_COLUMN),'NO') AS HIDDEN_COLUMN,
       NVL(TO_CHAR(VIRTUAL_COLUMN),'NO') AS VIRTUAL_COLUMN
FROM DBA_TAB_COLUMNS
```

**风险等级**: 🟠 **高** - DDL 错误  
**修复难度**: 中  
**修复优先级**: P1

---

### 6. 函数索引表达式未提取

**代码位置**: lines 6439-6490 (索引列收集)

**问题**:
```python
# 仅查询 DBA_IND_COLUMNS，函数索引列显示为 SYS_NCxxxxx$
SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_NAME
FROM DBA_IND_COLUMNS
-- ❌ 未查询 DBA_IND_EXPRESSIONS 获取真实表达式
```

**影响**:
- ❌ 函数索引被识别为 SYS_NC 列
- ❌ 生成的索引 DDL 不正确
- ❌ 重建索引时失败

**修复建议**:
```python
# 增加对 DBA_IND_EXPRESSIONS 的查询
SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_POSITION, COLUMN_EXPRESSION
FROM DBA_IND_EXPRESSIONS
WHERE TABLE_OWNER IN ({owners_clause})
```

**风险等级**: 🟠 **高** - 索引 DDL 错误  
**修复难度**: 中  
**修复优先级**: P1

---

### 7. 内存风险：全量加载元数据

**代码位置**: lines 5236-5800 (dump_ob_metadata), 6236-6800 (load_oracle_metadata)

**问题**:
```python
# 所有元数据一次性加载到内存
def dump_ob_metadata(...):
    objects_by_type: Dict[str, Set[str]] = {}
    tab_columns: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    indexes: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    # ... 数千个表 × 数十列 × 多个索引 = 数百 MB
```

**影响**:
- ⚠️ 1000 表 × 平均 50 列 × 5 索引 = ~250MB 内存
- ⚠️ 10000 表场景可能 OOM
- ⚠️ Python Dict 开销较大，实际内存使用更高

**业务场景**:
```
# 大型企业迁移场景
- 源端 schema: 50 个
- 总表数: 5000 个
- 平均列数: 40
- 平均索引数: 8
- 估算内存: 5000 × 40 × 8 × 500 bytes ≈ 800 MB

# 超大型场景
- 总表数: 20000 个
- 估算内存: 3.2 GB
- ❌ 可能导致 32 位 Python OOM
- ❌ 容器环境内存限制可能不足
```

**修复建议**:
```python
# 方案 1: 分批加载
def dump_ob_metadata_chunked(ob_cfg, target_schemas, chunk_size=1000):
    for schema_chunk in chunks(target_schemas, chunk_size):
        yield dump_ob_metadata(ob_cfg, schema_chunk, ...)

# 方案 2: 流式对比
def compare_tables_streaming(oracle_conn, ob_cfg, table_pairs):
    for table_pair in table_pairs:
        # 仅加载当前表的元数据
        oracle_cols = fetch_table_columns(oracle_conn, table_pair)
        ob_cols = fetch_ob_table_columns(ob_cfg, table_pair)
        compare_and_report(oracle_cols, ob_cols)

# 方案 3: 增加内存监控
import psutil
if psutil.virtual_memory().percent > 80:
    log.warning("内存使用超过 80%，建议分批执行")
```

**风险等级**: 🟠 **高** - 大规模场景 OOM  
**修复难度**: 高  
**修复优先级**: P1

---

### 8. 单一 timeout 策略

**代码位置**: lines 5133-5177, OBC_TIMEOUT=60

**问题**:
```python
# 所有 obclient 查询使用相同的 60 秒 timeout
result = subprocess.run(
    command_args,
    timeout=OBC_TIMEOUT  # 全局 60 秒
)
```

**影响**:
- ⚠️ 大表场景：DBA_TAB_COLUMNS 查询可能超过 60 秒
- ⚠️ 一次查询失败导致整个程序退出
- ⚠️ 无法针对不同查询设置不同 timeout

**业务场景**:
```sql
-- 查询 DBA_TAB_COLUMNS，包含 5000 个表，每表 50 列
SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ...
FROM DBA_TAB_COLUMNS
WHERE OWNER IN ('SCHEMA1', 'SCHEMA2', ..., 'SCHEMA50')
-- 可能返回 250,000 行
-- 网络传输 + 解析可能超过 60 秒
```

**当前行为**:
```python
except subprocess.TimeoutExpired:
    log.error(f"严重错误: obclient 执行超时 (>{OBC_TIMEOUT} 秒)...")
    return False, "", "TimeoutExpired"

# 上层处理
if not ok:
    log.error("无法从 OB 读取 DBA_TAB_COLUMNS，程序退出。")
    sys.exit(1)  # ❌ 直接退出
```

**修复建议**:
```python
# 方案 1: 分级 timeout
TIMEOUT_QUICK = 30      # DBA_OBJECTS 等轻量查询
TIMEOUT_NORMAL = 120    # DBA_TAB_COLUMNS 等常规查询
TIMEOUT_LONG = 300      # 大表场景

# 方案 2: 自动重试
def obclient_run_sql_with_retry(ob_cfg, sql, max_retries=3, timeout=60):
    for attempt in range(max_retries):
        try:
            result = subprocess.run(..., timeout=timeout * (attempt + 1))
            return result
        except subprocess.TimeoutExpired:
            if attempt < max_retries - 1:
                log.warning(f"查询超时，第 {attempt+2} 次重试（timeout={timeout*(attempt+2)}s）")
                continue
            raise

# 方案 3: 分块查询
# 将大查询拆分为多个小查询，每个查询 timeout 更短
```

**风险等级**: 🟠 **高** - 大规模场景执行失败  
**修复难度**: 中  
**修复优先级**: P1

---

## 📊 风险总结

| 风险等级 | 数量 | 关键问题 |
|---------|------|----------|
| 🔴 严重 (P0) | 4 | CHECK 约束缺失、外键规则缺失、OB CHAR_USED 缺失、NUMBER 精度未对比 |
| 🟠 高 (P1) | 4 | 虚拟列未识别、函数索引未提取、内存风险、timeout 策略 |
| 🟡 中 (P2) | 若干 | TIMESTAMP 精度、索引列顺序等 |

---

## 🎯 修复优先级建议

### 立即修复 (本周内)
1. ✅ 增加 CHECK 约束收集和对比
2. ✅ 增加外键 DELETE_RULE 收集
3. ✅ OB 侧增加 CHAR_USED 字段获取
4. ✅ 增加 NUMBER 精度和标度对比

### 近期修复 (本月内)
5. ✅ 增加 VIRTUAL_COLUMN 识别
6. ✅ 增加 DBA_IND_EXPRESSIONS 查询
7. ✅ 实现分批加载或内存监控
8. ✅ 优化 timeout 策略，增加重试机制

---

## 📝 部署建议

### 生产环境检查清单

**运行前检查**:
- [ ] 确认数据库账号有 SELECT ANY DICTIONARY 权限
- [ ] 检查可用内存（建议 ≥ 表数量 × 1MB）
- [ ] 调大 obclient_timeout（大规模场景建议 ≥ 300）
- [ ] 启用详细日志（log_level=DEBUG）

**运行中监控**:
- [ ] 监控内存使用（推荐 psutil）
- [ ] 监控 obclient 执行时间
- [ ] 记录 timeout 异常

**运行后验证**:
- [ ] 检查报告中的 CHECK 约束（手动补充）
- [ ] 检查外键级联规则（手动补充）
- [ ] 验证 NUMBER 列精度（抽样检查）
- [ ] 验证虚拟列（手动检查）

---

*分析完成时间: 2026-01-20*  
*建议复审周期: 每季度*
