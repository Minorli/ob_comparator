# 综合测试用例设计 - 基于功能深度审查

**项目**: OceanBase Comparator Toolkit  
**版本**: V0.9.8  
**测试类型**: 端到端业务场景测试  
**基于**: REVIEW_10_FUNCTIONAL_DEEP_DIVE.md 发现的问题  

---

## 📋 测试策略概述

本测试计划基于实际 Oracle → OceanBase 迁移场景，覆盖工具在真实业务环境中的关键功能点。测试分为三个优先级：

- **P0 (关键)**: 影响数据完整性和迁移成功率的核心功能
- **P1 (重要)**: 影响迁移效率和复杂场景支持的功能
- **P2 (优化)**: 边界情况和性能优化

---

## 🎯 P0 测试用例 - 数据完整性保障

### TC-P0-01: 虚拟列识别与 DDL 生成

**测试目标**: 验证工具能否正确识别和处理 Oracle 虚拟列

**前置条件**:
```sql
-- Oracle 源端
CREATE TABLE ORDERS (
    ORDER_ID NUMBER PRIMARY KEY,
    QUANTITY NUMBER NOT NULL,
    UNIT_PRICE NUMBER(10,2) NOT NULL,
    TAX_RATE NUMBER(3,2) DEFAULT 0.13,
    -- 虚拟列：总价 = 数量 * 单价 * (1 + 税率)
    TOTAL_AMOUNT NUMBER GENERATED ALWAYS AS (QUANTITY * UNIT_PRICE * (1 + TAX_RATE)) VIRTUAL,
    -- 虚拟列：是否大额订单
    IS_LARGE_ORDER VARCHAR2(1) GENERATED ALWAYS AS (CASE WHEN QUANTITY * UNIT_PRICE > 10000 THEN 'Y' ELSE 'N' END) VIRTUAL
);

-- OceanBase 目标端（仅迁移了物理列）
CREATE TABLE ORDERS (
    ORDER_ID NUMBER PRIMARY KEY,
    QUANTITY NUMBER NOT NULL,
    UNIT_PRICE NUMBER(10,2) NOT NULL,
    TAX_RATE NUMBER(3,2) DEFAULT 0.13
);
```

**执行步骤**:
1. 运行 `schema_diff_reconciler.py` 进行对比
2. 检查对比报告中的缺失列
3. 检查生成的 fixup DDL

**预期结果**:
```
✅ 报告应识别缺失的虚拟列：
   - TOTAL_AMOUNT (VIRTUAL)
   - IS_LARGE_ORDER (VIRTUAL)

✅ 生成的 DDL 应包含 GENERATED ALWAYS AS 子句：
   ALTER TABLE ORDERS ADD (
       TOTAL_AMOUNT NUMBER GENERATED ALWAYS AS (QUANTITY * UNIT_PRICE * (1 + TAX_RATE)) VIRTUAL,
       IS_LARGE_ORDER VARCHAR2(1) GENERATED ALWAYS AS (CASE WHEN QUANTITY * UNIT_PRICE > 10000 THEN 'Y' ELSE 'N' END) VIRTUAL
   );

❌ 当前预期行为（BUG）：
   - 虚拟列被识别为普通缺失列
   - DDL 缺少 GENERATED ALWAYS AS 子句
   - 执行会失败或创建错误的列定义
```

**验证方法**:
```python
# 检查元数据收集是否包含 VIRTUAL_COLUMN 标记
oracle_meta = dump_oracle_metadata(...)
col_info = oracle_meta.table_columns[('SCHEMA', 'ORDERS')]['TOTAL_AMOUNT']
assert col_info.get('virtual') == True, "虚拟列未被标记"

# 检查 DDL 生成
ddl = generate_column_ddl(col_info)
assert 'GENERATED ALWAYS AS' in ddl, "DDL 缺少虚拟列定义"
```

**关联问题**: REVIEW_10 问题 #1

---

### TC-P0-02: CHECK 约束完整性验证

**测试目标**: 验证 CHECK 约束的收集、对比和 DDL 生成

**前置条件**:
```sql
-- Oracle 源端
CREATE TABLE EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    EMP_NAME VARCHAR2(100) NOT NULL,
    SALARY NUMBER(10,2),
    AGE NUMBER(3),
    EMAIL VARCHAR2(100),
    DEPT_ID NUMBER,
    HIRE_DATE DATE,
    -- CHECK 约束
    CONSTRAINT CHK_SALARY CHECK (SALARY > 0 AND SALARY < 1000000),
    CONSTRAINT CHK_AGE CHECK (AGE BETWEEN 18 AND 65),
    CONSTRAINT CHK_EMAIL CHECK (EMAIL LIKE '%@%'),
    CONSTRAINT CHK_HIRE_DATE CHECK (HIRE_DATE >= DATE '2000-01-01')
);

-- OceanBase 目标端（缺少 CHECK 约束）
CREATE TABLE EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    EMP_NAME VARCHAR2(100) NOT NULL,
    SALARY NUMBER(10,2),
    AGE NUMBER(3),
    EMAIL VARCHAR2(100),
    DEPT_ID NUMBER,
    HIRE_DATE DATE
);
```

**执行步骤**:
1. 运行对比工具
2. 检查约束对比结果
3. 验证生成的 DDL

**预期结果**:
```
✅ 应识别缺失的 CHECK 约束：
   - CHK_SALARY
   - CHK_AGE
   - CHK_EMAIL
   - CHK_HIRE_DATE

✅ 生成的 DDL：
   ALTER TABLE EMPLOYEES ADD CONSTRAINT CHK_SALARY CHECK (SALARY > 0 AND SALARY < 1000000);
   ALTER TABLE EMPLOYEES ADD CONSTRAINT CHK_AGE CHECK (AGE BETWEEN 18 AND 65);
   ALTER TABLE EMPLOYEES ADD CONSTRAINT CHK_EMAIL CHECK (EMAIL LIKE '%@%');
   ALTER TABLE EMPLOYEES ADD CONSTRAINT CHK_HIRE_DATE CHECK (HIRE_DATE >= DATE '2000-01-01');

❌ 当前预期行为（BUG）：
   - CHECK 约束未被收集（DBA_CONSTRAINTS 查询中 CONSTRAINT_TYPE 缺少 'C'）
   - 报告中不显示缺失的 CHECK 约束
   - 无 DDL 生成
```

**数据完整性影响**:
```sql
-- 没有 CHECK 约束，可能插入非法数据
INSERT INTO EMPLOYEES (EMP_ID, SALARY, AGE, EMAIL) 
VALUES (1, -1000, 100, 'invalid_email');  -- 应被 CHECK 约束阻止

-- 后果：
-- ❌ SALARY < 0 的非法数据
-- ❌ AGE > 65 的非法数据
-- ❌ EMAIL 格式错误的数据
```

**关联问题**: REVIEW_10 问题 #3

---

### TC-P0-03: VARCHAR2 CHAR/BYTE 语义对比

**测试目标**: 验证 VARCHAR2 列的字符/字节语义是否正确对比

**前置条件**:
```sql
-- Oracle 源端（中文环境）
CREATE TABLE PRODUCTS (
    PRODUCT_ID NUMBER PRIMARY KEY,
    -- CHAR 语义：最多 50 个字符（无论单字节还是多字节）
    PRODUCT_NAME_CN VARCHAR2(50 CHAR),
    -- BYTE 语义：最多 50 字节（中文约 16-17 个字符，UTF-8 编码）
    PRODUCT_CODE VARCHAR2(50 BYTE),
    -- 默认语义（取决于 NLS_LENGTH_SEMANTICS，假设为 BYTE）
    DESCRIPTION VARCHAR2(200)
);

-- OceanBase 目标端（OMS 迁移后）
CREATE TABLE PRODUCTS (
    PRODUCT_ID NUMBER PRIMARY KEY,
    -- 正确：保持 CHAR 语义
    PRODUCT_NAME_CN VARCHAR2(50 CHAR),
    -- OMS 放大 1.5 倍（50 * 1.5 = 75）
    PRODUCT_CODE VARCHAR2(75),
    -- OMS 放大 1.5 倍（200 * 1.5 = 300）
    DESCRIPTION VARCHAR2(300)
);
```

**执行步骤**:
1. 运行对比工具
2. 检查 VARCHAR2 列长度对比结果
3. 验证 CHAR_USED 标记的处理

**预期结果**:
```
✅ PRODUCT_NAME_CN: 
   - 源端 50 CHAR vs 目标端 50 CHAR
   - 判定：✓ 完全匹配

✅ PRODUCT_CODE:
   - 源端 50 BYTE vs 目标端 75 BYTE
   - 判定：✓ 符合 1.5 倍规则（50 * 1.5 = 75）

✅ DESCRIPTION:
   - 源端 200 BYTE vs 目标端 300 BYTE
   - 判定：✓ 符合 1.5 倍规则（200 * 1.5 = 300）

❌ 当前预期行为（BUG）：
   - OB 侧未获取 CHAR_USED，无法区分语义
   - PRODUCT_NAME_CN 可能被误判（如果工具假设都是 BYTE 语义）
   - PRODUCT_CODE 和 DESCRIPTION 无法确认是否真的是 BYTE 语义
```

**边界测试**:
```sql
-- 测试用例 1: CHAR 语义不应放大
-- 源端: VARCHAR2(100 CHAR)
-- 目标端错误: VARCHAR2(150) -- 如果这是 BYTE 语义，则不匹配
-- 目标端正确: VARCHAR2(100 CHAR)

-- 测试用例 2: BYTE 语义应放大
-- 源端: VARCHAR2(100 BYTE)
-- 目标端错误: VARCHAR2(100 BYTE) -- 不满足 1.5 倍
-- 目标端正确: VARCHAR2(150 BYTE) -- ceil(100 * 1.5) = 150

-- 测试用例 3: 语义不一致应报错
-- 源端: VARCHAR2(100 CHAR)
-- 目标端: VARCHAR2(150 BYTE)
-- 判定: ✗ 语义不一致
```

**关联问题**: REVIEW_10 问题 #4

---

### TC-P0-04: NUMBER 精度和标度验证

**测试目标**: 验证 NUMBER 类型的精度(precision)和标度(scale)对比

**前置条件**:
```sql
-- Oracle 源端
CREATE TABLE FINANCIAL_DATA (
    RECORD_ID NUMBER PRIMARY KEY,
    -- 精度 10，标度 2：最大 99999999.99
    AMOUNT NUMBER(10,2),
    -- 精度 15，标度 4：科学计算用高精度
    EXCHANGE_RATE NUMBER(15,4),
    -- 仅精度：整数，最大 10 位
    QUANTITY NUMBER(10),
    -- 无限制精度
    BIG_NUMBER NUMBER
);

-- OceanBase 目标端（错误迁移）
CREATE TABLE FINANCIAL_DATA (
    RECORD_ID NUMBER PRIMARY KEY,
    -- ❌ 精度不足
    AMOUNT NUMBER(8,2),
    -- ❌ 标度不一致
    EXCHANGE_RATE NUMBER(15,2),
    -- ❌ 标度错误
    QUANTITY NUMBER(10,2),
    -- ✓ 无限制精度匹配
    BIG_NUMBER NUMBER
);
```

**执行步骤**:
1. 运行对比工具
2. 检查 NUMBER 类型的精度标度对比
3. 验证报告中的不匹配项

**预期结果**:
```
✅ 应识别以下不匹配：
   1. AMOUNT: 
      - 源端 NUMBER(10,2) vs 目标端 NUMBER(8,2)
      - 错误类型: 精度不足 (8 < 10)
      
   2. EXCHANGE_RATE:
      - 源端 NUMBER(15,4) vs 目标端 NUMBER(15,2)
      - 错误类型: 标度不一致 (2 ≠ 4)
      
   3. QUANTITY:
      - 源端 NUMBER(10) vs 目标端 NUMBER(10,2)
      - 错误类型: 标度不一致 (2 ≠ 0)
      
   4. BIG_NUMBER:
      - 源端 NUMBER vs 目标端 NUMBER
      - 判定: ✓ 匹配

❌ 当前预期行为（BUG）：
   - 所有 NUMBER 列均判定为匹配（因为只检查了 data_type = 'NUMBER'）
   - 精度和标度差异未被检测
```

**数据风险场景**:
```sql
-- AMOUNT 精度不足的风险
-- 源端允许: 99999999.99
-- 目标端最大: 999999.99
INSERT INTO FINANCIAL_DATA (RECORD_ID, AMOUNT) VALUES (1, 5000000.00);
-- ❌ ORA-01438: value larger than specified precision

-- EXCHANGE_RATE 标度不一致的风险
-- 源端: 1.2345 (4 位小数)
-- 目标端: 1.23 (2 位小数，精度丢失)

-- QUANTITY 应该是整数，但目标端允许小数
-- 可能导致业务逻辑错误
```

**关联问题**: REVIEW_10 问题 #5

---

### TC-P0-05: 外键级联规则验证

**测试目标**: 验证外键的 ON DELETE/ON UPDATE 规则是否正确对比

**前置条件**:
```sql
-- Oracle 源端
CREATE TABLE DEPARTMENTS (
    DEPT_ID NUMBER PRIMARY KEY,
    DEPT_NAME VARCHAR2(50)
);

CREATE TABLE EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    EMP_NAME VARCHAR2(100),
    DEPT_ID NUMBER,
    -- CASCADE: 删除部门时，级联删除员工
    CONSTRAINT FK_EMP_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID) 
        ON DELETE CASCADE
);

CREATE TABLE PROJECTS (
    PROJECT_ID NUMBER PRIMARY KEY,
    PROJECT_NAME VARCHAR2(100),
    DEPT_ID NUMBER,
    -- SET NULL: 删除部门时，项目的部门ID设为NULL
    CONSTRAINT FK_PROJ_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID) 
        ON DELETE SET NULL
);

CREATE TABLE BUDGETS (
    BUDGET_ID NUMBER PRIMARY KEY,
    DEPT_ID NUMBER,
    AMOUNT NUMBER(15,2),
    -- NO ACTION (默认): 如果有预算记录，不能删除部门
    CONSTRAINT FK_BUDGET_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID)
);

-- OceanBase 目标端（缺少级联规则）
CREATE TABLE DEPARTMENTS (
    DEPT_ID NUMBER PRIMARY KEY,
    DEPT_NAME VARCHAR2(50)
);

CREATE TABLE EMPLOYEES (
    EMP_ID NUMBER PRIMARY KEY,
    EMP_NAME VARCHAR2(100),
    DEPT_ID NUMBER,
    -- ❌ 缺少 ON DELETE CASCADE
    CONSTRAINT FK_EMP_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID)
);

CREATE TABLE PROJECTS (
    PROJECT_ID NUMBER PRIMARY KEY,
    PROJECT_NAME VARCHAR2(100),
    DEPT_ID NUMBER,
    -- ❌ 缺少 ON DELETE SET NULL
    CONSTRAINT FK_PROJ_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID)
);

CREATE TABLE BUDGETS (
    BUDGET_ID NUMBER PRIMARY KEY,
    DEPT_ID NUMBER,
    AMOUNT NUMBER(15,2),
    -- ✓ NO ACTION 是默认值
    CONSTRAINT FK_BUDGET_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID)
);
```

**执行步骤**:
1. 运行对比工具
2. 检查外键约束的详细对比
3. 验证级联规则的识别

**预期结果**:
```
✅ 应识别以下不匹配：
   1. FK_EMP_DEPT:
      - 源端: ON DELETE CASCADE
      - 目标端: ON DELETE NO ACTION (默认)
      - 判定: ✗ 级联规则不一致
      
   2. FK_PROJ_DEPT:
      - 源端: ON DELETE SET NULL
      - 目标端: ON DELETE NO ACTION (默认)
      - 判定: ✗ 级联规则不一致
      
   3. FK_BUDGET_DEPT:
      - 源端: ON DELETE NO ACTION (默认)
      - 目标端: ON DELETE NO ACTION (默认)
      - 判定: ✓ 匹配

✅ 生成的修复 DDL：
   -- 需要先删除错误的约束，再重建
   ALTER TABLE EMPLOYEES DROP CONSTRAINT FK_EMP_DEPT;
   ALTER TABLE EMPLOYEES ADD CONSTRAINT FK_EMP_DEPT 
       FOREIGN KEY (DEPT_ID) REFERENCES DEPARTMENTS(DEPT_ID) ON DELETE CASCADE;
   
   ALTER TABLE PROJECTS DROP CONSTRAINT FK_PROJ_DEPT;
   ALTER TABLE PROJECTS ADD CONSTRAINT FK_PROJ_DEPT 
       FOREIGN KEY (DEPT_ID) REFERENCES DEPARTMENTS(DEPT_ID) ON DELETE SET NULL;

❌ 当前预期行为（BUG）：
   - DELETE_RULE 未被收集
   - 所有外键只要引用关系正确就判定为匹配
   - 级联规则差异未被检测
```

**业务影响测试**:
```sql
-- 测试场景: 删除部门
DELETE FROM DEPARTMENTS WHERE DEPT_ID = 10;

-- 预期行为（有 CASCADE）：
-- ✓ EMPLOYEES 表中 DEPT_ID=10 的记录被自动删除
-- ✓ PROJECTS 表中 DEPT_ID=10 的记录的 DEPT_ID 被设为 NULL
-- ✓ 如果 BUDGETS 表中有 DEPT_ID=10 的记录，DELETE 失败

-- 实际行为（无 CASCADE）：
-- ❌ DELETE 失败: ORA-02292 integrity constraint violated - child record found
-- ❌ 需要手动删除 EMPLOYEES 和处理 PROJECTS
```

**关联问题**: REVIEW_10 问题 #8

---

## 🎯 P1 测试用例 - 复杂场景支持

### TC-P1-01: 函数索引识别与 DDL 生成

**测试目标**: 验证函数索引(Function-based Index)的正确处理

**前置条件**:
```sql
-- Oracle 源端
CREATE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER PRIMARY KEY,
    FIRST_NAME VARCHAR2(50),
    LAST_NAME VARCHAR2(50),
    EMAIL VARCHAR2(100),
    PHONE VARCHAR2(20),
    CREATE_DATE DATE
);

-- 函数索引
CREATE INDEX IDX_UPPER_EMAIL ON CUSTOMERS (UPPER(EMAIL));
CREATE INDEX IDX_LOWER_NAME ON CUSTOMERS (LOWER(LAST_NAME || ', ' || FIRST_NAME));
CREATE INDEX IDX_YEAR_MONTH ON CUSTOMERS (TO_CHAR(CREATE_DATE, 'YYYYMM'));
CREATE INDEX IDX_PHONE_CLEAN ON CUSTOMERS (REPLACE(REPLACE(PHONE, '-', ''), ' ', ''));

-- OceanBase 目标端（缺少函数索引）
CREATE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER PRIMARY KEY,
    FIRST_NAME VARCHAR2(50),
    LAST_NAME VARCHAR2(50),
    EMAIL VARCHAR2(100),
    PHONE VARCHAR2(20),
    CREATE_DATE DATE
);
```

**执行步骤**:
1. 运行对比工具
2. 检查 `DBA_IND_COLUMNS` 和 `DBA_IND_EXPRESSIONS` 的读取
3. 验证函数索引的识别

**预期结果**:
```
✅ 应正确识别函数索引：
   1. IDX_UPPER_EMAIL
      - 列: 非 SYS_NCxxxxx$
      - 表达式: UPPER(EMAIL)
      
   2. IDX_LOWER_NAME
      - 列: 非 SYS_NCxxxxx$
      - 表达式: LOWER(LAST_NAME || ', ' || FIRST_NAME)
      
   3. IDX_YEAR_MONTH
      - 列: 非 SYS_NCxxxxx$
      - 表达式: TO_CHAR(CREATE_DATE, 'YYYYMM')
      
   4. IDX_PHONE_CLEAN
      - 列: 非 SYS_NCxxxxx$
      - 表达式: REPLACE(REPLACE(PHONE, '-', ''), ' ', '')

✅ 生成的 DDL：
   CREATE INDEX IDX_UPPER_EMAIL ON CUSTOMERS (UPPER(EMAIL));
   CREATE INDEX IDX_LOWER_NAME ON CUSTOMERS (LOWER(LAST_NAME || ', ' || FIRST_NAME));
   CREATE INDEX IDX_YEAR_MONTH ON CUSTOMERS (TO_CHAR(CREATE_DATE, 'YYYYMM'));
   CREATE INDEX IDX_PHONE_CLEAN ON CUSTOMERS (REPLACE(REPLACE(PHONE, '-', ''), ' ', ''));

❌ 当前预期行为（BUG）：
   - DBA_IND_COLUMNS 返回 SYS_NC00001$, SYS_NC00002$ 等
   - DBA_IND_EXPRESSIONS 未被查询
   - 生成的 DDL 可能为: CREATE INDEX IDX_UPPER_EMAIL ON CUSTOMERS (SYS_NC00001$);
   - 执行失败: invalid identifier
```

**性能影响**:
```sql
-- 查询使用函数索引
SELECT * FROM CUSTOMERS WHERE UPPER(EMAIL) = 'JOHN@EXAMPLE.COM';

-- 有函数索引：
-- ✓ INDEX RANGE SCAN (IDX_UPPER_EMAIL)
-- ✓ 执行时间: 0.01秒

-- 无函数索引：
-- ❌ FULL TABLE SCAN
-- ❌ 执行时间: 2.5秒 (100万行数据)
```

**关联问题**: REVIEW_10 问题 #2

---

### TC-P1-02: 多层 VIEW 依赖链

**测试目标**: 验证复杂 VIEW 依赖关系的处理和执行顺序

**前置条件**:
```sql
-- Oracle 源端: 5 层 VIEW 依赖
-- Layer 1: 基础视图
CREATE OR REPLACE VIEW V_ACTIVE_CUSTOMERS AS
SELECT * FROM CUSTOMERS WHERE STATUS = 'ACTIVE';

CREATE OR REPLACE VIEW V_ACTIVE_PRODUCTS AS
SELECT * FROM PRODUCTS WHERE STATUS = 'ACTIVE';

-- Layer 2: 依赖 Layer 1
CREATE OR REPLACE VIEW V_CUSTOMER_ORDERS AS
SELECT 
    c.CUSTOMER_ID, c.CUSTOMER_NAME,
    o.ORDER_ID, o.ORDER_DATE, o.AMOUNT
FROM V_ACTIVE_CUSTOMERS c
JOIN ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID;

CREATE OR REPLACE VIEW V_PRODUCT_SALES AS
SELECT 
    p.PRODUCT_ID, p.PRODUCT_NAME,
    oi.ORDER_ID, oi.QUANTITY, oi.UNIT_PRICE
FROM V_ACTIVE_PRODUCTS p
JOIN ORDER_ITEMS oi ON p.PRODUCT_ID = oi.PRODUCT_ID;

-- Layer 3: 依赖 Layer 2
CREATE OR REPLACE VIEW V_CUSTOMER_REVENUE AS
SELECT 
    CUSTOMER_ID, CUSTOMER_NAME,
    SUM(AMOUNT) AS TOTAL_REVENUE,
    COUNT(ORDER_ID) AS ORDER_COUNT
FROM V_CUSTOMER_ORDERS
GROUP BY CUSTOMER_ID, CUSTOMER_NAME;

-- Layer 4: 依赖 Layer 2 和 Layer 3
CREATE OR REPLACE VIEW V_HIGH_VALUE_CUSTOMERS AS
SELECT 
    cr.CUSTOMER_ID, cr.CUSTOMER_NAME, cr.TOTAL_REVENUE,
    ps.PRODUCT_ID, ps.PRODUCT_NAME
FROM V_CUSTOMER_REVENUE cr
JOIN V_CUSTOMER_ORDERS co ON cr.CUSTOMER_ID = co.CUSTOMER_ID
JOIN V_PRODUCT_SALES ps ON co.ORDER_ID = ps.ORDER_ID
WHERE cr.TOTAL_REVENUE > 100000;

-- Layer 5: 依赖 Layer 4
CREATE OR REPLACE VIEW V_PREMIUM_CUSTOMER_SUMMARY AS
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    TOTAL_REVENUE,
    COUNT(DISTINCT PRODUCT_ID) AS PRODUCT_VARIETY
FROM V_HIGH_VALUE_CUSTOMERS
GROUP BY CUSTOMER_ID, CUSTOMER_NAME, TOTAL_REVENUE
HAVING COUNT(DISTINCT PRODUCT_ID) >= 5;

-- OceanBase 目标端: 所有 VIEW 均缺失
-- 仅有基础表 CUSTOMERS, PRODUCTS, ORDERS, ORDER_ITEMS
```

**执行步骤**:
1. 运行对比工具生成 VIEW fixup 脚本
2. 检查 VIEW 依赖链分析报告
3. 执行 `run_fixup.py --smart-order`
4. 验证执行顺序

**预期结果**:
```
✅ 依赖链分析报告应正确识别：
   Layer 1 (无依赖):
   - V_ACTIVE_CUSTOMERS
   - V_ACTIVE_PRODUCTS
   
   Layer 2 (依赖 Layer 1):
   - V_CUSTOMER_ORDERS → V_ACTIVE_CUSTOMERS
   - V_PRODUCT_SALES → V_ACTIVE_PRODUCTS
   
   Layer 3 (依赖 Layer 2):
   - V_CUSTOMER_REVENUE → V_CUSTOMER_ORDERS
   
   Layer 4 (依赖 Layer 2, 3):
   - V_HIGH_VALUE_CUSTOMERS → V_CUSTOMER_REVENUE, V_CUSTOMER_ORDERS, V_PRODUCT_SALES
   
   Layer 5 (依赖 Layer 4):
   - V_PREMIUM_CUSTOMER_SUMMARY → V_HIGH_VALUE_CUSTOMERS

✅ run_fixup 执行顺序：
   [1] V_ACTIVE_CUSTOMERS
   [2] V_ACTIVE_PRODUCTS
   [3] V_CUSTOMER_ORDERS
   [4] V_PRODUCT_SALES
   [5] V_CUSTOMER_REVENUE
   [6] V_HIGH_VALUE_CUSTOMERS
   [7] V_PREMIUM_CUSTOMER_SUMMARY

✅ 所有 VIEW 创建成功，无 "table or view does not exist" 错误

❌ 当前预期行为（可能的问题）：
   - 如果按文件名排序: V_ACTIVE_CUSTOMERS, V_ACTIVE_PRODUCTS, V_CUSTOMER_ORDERS, 
     V_CUSTOMER_REVENUE, V_HIGH_VALUE_CUSTOMERS, V_PREMIUM_CUSTOMER_SUMMARY, V_PRODUCT_SALES
   - V_PRODUCT_SALES 最后执行，但 V_HIGH_VALUE_CUSTOMERS 依赖它
   - V_HIGH_VALUE_CUSTOMERS 执行失败
   - 需要迭代重试（--iterative 模式）
```

**压力测试**:
```
- 测试 10 层深度的依赖链
- 测试 100 个 VIEW 的复杂网络（多对多依赖）
- 测试依赖链中包含同义词和物化视图
```

**关联问题**: REVIEW_10 问题 #11

---

### TC-P1-03: 跨 Schema 依赖与 Remap

**测试目标**: 验证跨 Schema 对象依赖和 Remap 规则的正确处理

**前置条件**:
```sql
-- Oracle 源端
-- Schema A: 基础数据
CREATE TABLE SCHEMA_A.CUSTOMERS (...);
CREATE TABLE SCHEMA_A.ORDERS (...);
CREATE VIEW SCHEMA_A.V_CUSTOMER_ORDERS AS 
    SELECT * FROM SCHEMA_A.CUSTOMERS c 
    JOIN SCHEMA_A.ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID;

-- Schema B: 依赖 Schema A
CREATE SYNONYM SCHEMA_B.CUSTOMERS FOR SCHEMA_A.CUSTOMERS;
CREATE SYNONYM SCHEMA_B.ORDERS FOR SCHEMA_A.ORDERS;

CREATE VIEW SCHEMA_B.V_MY_ORDERS AS
    SELECT * FROM SCHEMA_A.ORDERS WHERE CREATED_BY = USER;

CREATE PACKAGE SCHEMA_B.PKG_ORDER_MGMT AS
    PROCEDURE CREATE_ORDER(p_customer_id NUMBER, p_amount NUMBER);
END;
/
CREATE PACKAGE BODY SCHEMA_B.PKG_ORDER_MGMT AS
    PROCEDURE CREATE_ORDER(p_customer_id NUMBER, p_amount NUMBER) IS
    BEGIN
        -- 直接引用 SCHEMA_A 的表
        INSERT INTO SCHEMA_A.ORDERS (CUSTOMER_ID, AMOUNT, CREATED_BY)
        VALUES (p_customer_id, p_amount, USER);
    END;
END;
/

-- Schema C: 依赖 Schema A 和 B
CREATE VIEW SCHEMA_C.V_SALES_SUMMARY AS
    SELECT * FROM SCHEMA_A.V_CUSTOMER_ORDERS
    UNION ALL
    SELECT * FROM SCHEMA_B.V_MY_ORDERS;

-- Remap 规则
-- SCHEMA_A → OB_CORE
-- SCHEMA_B → OB_APP
-- SCHEMA_C → OB_REPORT
```

**配置文件**:
```ini
# remap_rules.txt
SCHEMA_A.CUSTOMERS = OB_CORE.CUSTOMERS
SCHEMA_A.ORDERS = OB_CORE.ORDERS
SCHEMA_A.V_CUSTOMER_ORDERS = OB_CORE.V_CUSTOMER_ORDERS

SCHEMA_B.V_MY_ORDERS = OB_APP.V_MY_ORDERS
SCHEMA_B.PKG_ORDER_MGMT = OB_APP.PKG_ORDER_MGMT

SCHEMA_C.V_SALES_SUMMARY = OB_REPORT.V_SALES_SUMMARY
```

**执行步骤**:
1. 运行对比工具
2. 检查 Remap 推导结果
3. 检查生成的 VIEW/PACKAGE DDL 中的 Schema 引用
4. 验证 run_fixup 的执行顺序（跨 Schema 依赖）

**预期结果**:
```
✅ Remap 推导应正确处理：
   - SCHEMA_B.CUSTOMERS (SYNONYM) → OB_APP.CUSTOMERS (推导自依赖的表)
   - SCHEMA_B.ORDERS (SYNONYM) → OB_APP.ORDERS

✅ DDL 中的 Schema 引用应正确替换：
   -- V_MY_ORDERS 的 DDL
   CREATE OR REPLACE VIEW OB_APP.V_MY_ORDERS AS
       SELECT * FROM OB_CORE.ORDERS WHERE CREATED_BY = USER;
       -- ✓ SCHEMA_A.ORDERS → OB_CORE.ORDERS
   
   -- PKG_ORDER_MGMT BODY 的 DDL
   CREATE OR REPLACE PACKAGE BODY OB_APP.PKG_ORDER_MGMT AS
       PROCEDURE CREATE_ORDER(p_customer_id NUMBER, p_amount NUMBER) IS
       BEGIN
           INSERT INTO OB_CORE.ORDERS (CUSTOMER_ID, AMOUNT, CREATED_BY)
           VALUES (p_customer_id, p_amount, USER);
           -- ✓ SCHEMA_A.ORDERS → OB_CORE.ORDERS
       END;
   END;
   /
   
   -- V_SALES_SUMMARY 的 DDL
   CREATE OR REPLACE VIEW OB_REPORT.V_SALES_SUMMARY AS
       SELECT * FROM OB_CORE.V_CUSTOMER_ORDERS
       UNION ALL
       SELECT * FROM OB_APP.V_MY_ORDERS;
       -- ✓ 两个引用都正确替换

✅ run_fixup 执行顺序应考虑跨 Schema 依赖：
   [1] OB_CORE.CUSTOMERS, OB_CORE.ORDERS (基础表)
   [2] OB_CORE.V_CUSTOMER_ORDERS (依赖 OB_CORE 表)
   [3] OB_APP.V_MY_ORDERS (依赖 OB_CORE 表)
   [4] OB_APP.PKG_ORDER_MGMT (依赖 OB_CORE 表)
   [5] OB_REPORT.V_SALES_SUMMARY (依赖 OB_CORE 和 OB_APP 视图)

❌ 可能的问题：
   - DDL 中的 Schema 引用未替换，仍为 SCHEMA_A/B
   - PACKAGE BODY 中的硬编码 Schema 未被识别和替换
   - 跨 Schema 依赖未被考虑，执行顺序错误
```

**关联问题**: REVIEW_10 端到端场景 #3

---

### TC-P1-04: PACKAGE 相互依赖

**测试目标**: 验证 PACKAGE 之间的循环依赖和执行顺序

**前置条件**:
```sql
-- Oracle 源端
-- PACKAGE A 依赖 PACKAGE B
CREATE OR REPLACE PACKAGE PKG_A AS
    FUNCTION GET_VALUE RETURN NUMBER;
END;
/
CREATE OR REPLACE PACKAGE BODY PKG_A AS
    FUNCTION GET_VALUE RETURN NUMBER IS
        v_result NUMBER;
    BEGIN
        v_result := PKG_B.CALCULATE() * 2;  -- 调用 PKG_B
        RETURN v_result;
    END;
END;
/

-- PACKAGE B 依赖 PACKAGE A（形成循环）
CREATE OR REPLACE PACKAGE PKG_B AS
    FUNCTION CALCULATE RETURN NUMBER;
END;
/
CREATE OR REPLACE PACKAGE BODY PKG_B AS
    FUNCTION CALCULATE RETURN NUMBER IS
        v_base NUMBER := 100;
    BEGIN
        IF v_base > 50 THEN
            RETURN PKG_A.GET_VALUE() + 10;  -- 调用 PKG_A（条件性循环）
        ELSE
            RETURN v_base;
        END IF;
    END;
END;
/

-- PACKAGE C 独立，但应在 A 和 B 之后
CREATE OR REPLACE PACKAGE PKG_C AS
    PROCEDURE PROCESS;
END;
/
CREATE OR REPLACE PACKAGE BODY PKG_C AS
    PROCEDURE PROCESS IS
        v_a NUMBER;
        v_b NUMBER;
    BEGIN
        v_a := PKG_A.GET_VALUE();
        v_b := PKG_B.CALCULATE();
        DBMS_OUTPUT.PUT_LINE('A=' || v_a || ', B=' || v_b);
    END;
END;
/
```

**执行步骤**:
1. 运行对比工具
2. 检查 PACKAGE 依赖分析
3. 检查循环依赖检测
4. 验证 run_fixup 的执行策略

**预期结果**:
```
✅ 依赖分析应识别：
   - PKG_A.BODY → PKG_B.PACKAGE (调用 PKG_B.CALCULATE)
   - PKG_B.BODY → PKG_A.PACKAGE (调用 PKG_A.GET_VALUE)
   - 循环依赖: PKG_A ↔ PKG_B
   - PKG_C 依赖 PKG_A 和 PKG_B

✅ 循环依赖报告：
   检测到循环依赖: PKG_A <-> PKG_B
   建议: 先创建 PACKAGE 定义，再创建 PACKAGE BODY

✅ run_fixup 执行策略：
   方案 1（推荐）:
   [1] CREATE PACKAGE PKG_A (仅定义)
   [2] CREATE PACKAGE PKG_B (仅定义)
   [3] CREATE PACKAGE BODY PKG_A (可能失败，因为 PKG_B.CALCULATE 还不存在)
   [4] CREATE PACKAGE BODY PKG_B (可能失败，因为 PKG_A.GET_VALUE 还不存在)
   [5] 重新编译 PKG_A BODY
   [6] 重新编译 PKG_B BODY
   [7] CREATE PACKAGE PKG_C
   [8] CREATE PACKAGE BODY PKG_C
   
   方案 2（降级）:
   [1] 所有 PACKAGE 定义
   [2] 所有 PACKAGE BODY（允许部分失败）
   [3] 迭代重新编译 INVALID 对象

❌ 可能的问题：
   - 循环依赖未被检测
   - 执行时 PKG_A BODY 失败: "PKG_B.CALCULATE must be declared"
   - 需要多轮迭代才能成功
```

**关联问题**: REVIEW_10 问题 #12

---

## 🎯 P2 测试用例 - 边界与性能

### TC-P2-01: Interval 分区表处理

**测试目标**: 验证 Oracle Interval 分区表的识别和处理策略

**前置条件**:
```sql
-- Oracle 源端: Interval 分区表
CREATE TABLE SALES_HISTORY (
    SALE_ID NUMBER,
    SALE_DATE DATE,
    AMOUNT NUMBER(15,2)
)
PARTITION BY RANGE (SALE_DATE)
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
    PARTITION P_2023_01 VALUES LESS THAN (TO_DATE('2023-02-01', 'YYYY-MM-DD')),
    PARTITION P_2023_02 VALUES LESS THAN (TO_DATE('2023-03-01', 'YYYY-MM-DD'))
    -- Oracle 会自动创建后续月份的分区
);

-- OceanBase 目标端
-- 假设 OB 某版本不完全支持 INTERVAL 分区
CREATE TABLE SALES_HISTORY (
    SALE_ID NUMBER,
    SALE_DATE DATE,
    AMOUNT NUMBER(15,2)
)
PARTITION BY RANGE (SALE_DATE)
(
    PARTITION P_2023_01 VALUES LESS THAN (TO_DATE('2023-02-01', 'YYYY-MM-DD')),
    PARTITION P_2023_02 VALUES LESS THAN (TO_DATE('2023-03-01', 'YYYY-MM-DD'))
    -- ❌ 缺少 INTERVAL 子句，后续分区需手动创建
);
```

**执行步骤**:
1. 运行对比工具
2. 检查 Interval 分区识别
3. 检查生成的修复脚本

**预期结果**:
```
✅ 应识别 Interval 分区表：
   - SALES_HISTORY: INTERVAL(NUMTOYMINTERVAL(1, 'MONTH'))
   - 分区键: SALE_DATE
   - 最后分区: P_2023_02, HIGH_VALUE = TO_DATE('2023-03-01')

✅ 生成修复脚本（如果 OB 支持 INTERVAL）：
   ALTER TABLE SALES_HISTORY SET INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'));

✅ 生成补偿脚本（如果 OB 不支持 INTERVAL）：
   -- 生成未来 12 个月的分区
   ALTER TABLE SALES_HISTORY ADD PARTITION P_2023_03 VALUES LESS THAN (TO_DATE('2023-04-01', 'YYYY-MM-DD'));
   ALTER TABLE SALES_HISTORY ADD PARTITION P_2023_04 VALUES LESS THAN (TO_DATE('2023-05-01', 'YYYY-MM-DD'));
   ...
   ALTER TABLE SALES_HISTORY ADD PARTITION P_2024_02 VALUES LESS THAN (TO_DATE('2024-03-01', 'YYYY-MM-DD'));
   
   -- 并生成维护脚本或监控提示
   -- 注意: 需要定期手动添加新分区

✅ 报告中应包含警告：
   [警告] SALES_HISTORY 使用 Interval 分区，OceanBase 可能不完全支持
   [建议] 已生成未来 12 个月的分区脚本，需要定期维护
```

**关联问题**: REVIEW_10 端到端场景 #5

---

### TC-P2-02: 大数据量性能测试

**测试目标**: 验证工具在大规模对象场景下的性能和稳定性

**测试场景**:
```
模拟真实企业环境：
- 50 个 Schema
- 每个 Schema:
  - 20 个 TABLE (共 1000 个)
  - 每个表平均 15 列
  - 每个表平均 3 个索引 (共 3000 个)
  - 每个表平均 2 个约束 (共 2000 个)
- 总计:
  - 1000 个 TABLE
  - 15000 列定义
  - 3000 个 INDEX
  - 2000 个 CONSTRAINT
  - 500 个 VIEW (其中 200 个有依赖链)
  - 100 个 PROCEDURE
  - 50 个 PACKAGE
  - 20 个 TRIGGER
  - 10000 条对象级授权记录
```

**性能指标**:
```
✅ 元数据收集阶段：
   - Oracle 侧: < 5 分钟
   - OceanBase 侧: < 5 分钟
   - 内存使用: < 2 GB
   - 无超时错误

✅ 对比阶段：
   - TABLE 对比: < 2 分钟
   - INDEX/CONSTRAINT 对比: < 3 分钟
   - VIEW 依赖分析: < 2 分钟
   - 总时间: < 10 分钟

✅ DDL 生成阶段：
   - 生成 500 个 DDL 文件: < 3 分钟
   - 文件总大小: < 50 MB
   - 内存峰值: < 2 GB

✅ run_fixup 执行阶段（模拟，不实际执行）：
   - 依赖排序: < 1 分钟
   - 生成执行计划: < 30 秒
```

**压力边界**:
```
⚠️ 已知限制：
   - 单个 Schema 超过 1000 个表时，可能需要分批处理
   - VIEW 依赖深度超过 10 层时，分析时间显著增加
   - PACKAGE 源码超过 1 MB 时，DDL 清洗可能较慢
```

**关联问题**: REVIEW_10 端到端场景 #6

---

### TC-P2-03: 循环依赖检测与报告

**测试目标**: 验证复杂循环依赖的检测能力

**测试场景**:
```sql
-- 3 个 VIEW 形成循环
CREATE OR REPLACE FORCE VIEW V_A AS SELECT * FROM V_B;
CREATE OR REPLACE FORCE VIEW V_B AS SELECT * FROM V_C;
CREATE OR REPLACE FORCE VIEW V_C AS SELECT * FROM V_A;

-- 更复杂：4 个 VIEW 交叉依赖
CREATE OR REPLACE FORCE VIEW V_X AS 
    SELECT * FROM V_Y UNION ALL SELECT * FROM V_Z;
CREATE OR REPLACE FORCE VIEW V_Y AS 
    SELECT * FROM V_Z;
CREATE OR REPLACE FORCE VIEW V_Z AS 
    SELECT * FROM V_W;
CREATE OR REPLACE FORCE VIEW V_W AS 
    SELECT * FROM V_X;
-- 循环: V_X → V_Y → V_Z → V_W → V_X
--       V_X → V_Z → V_W → V_X

-- 自引用（边界情况）
CREATE OR REPLACE FORCE VIEW V_SELF AS 
    SELECT * FROM V_SELF WHERE ROWNUM < 10;
```

**预期结果**:
```
✅ 应检测并报告所有循环：
   循环 1: V_A → V_B → V_C → V_A (3 节点)
   循环 2: V_X → V_Y → V_Z → V_W → V_X (4 节点)
   循环 3: V_X → V_Z → V_W → V_X (3 节点，子循环)
   循环 4: V_SELF → V_SELF (自引用)

✅ 处理建议：
   - 使用 FORCE 关键字创建
   - 或按以下顺序尝试创建（允许部分失败，后续重新编译）:
     1. V_A (失败: V_B 不存在)
     2. V_B (失败: V_C 不存在)
     3. V_C (失败: V_A 不存在)
     4. 重新编译所有 INVALID VIEW

✅ run_fixup 应支持：
   - --force-views 选项: 为 VIEW DDL 自动添加 FORCE 关键字
   - --allow-invalid: 允许创建 INVALID 对象，后续重新编译
```

**关联问题**: REVIEW_10 问题 #12

---

## 📊 测试执行计划

### 阶段 1: P0 核心功能验证（1-2 周）
```
Week 1:
- TC-P0-01: 虚拟列
- TC-P0-02: CHECK 约束
- TC-P0-03: VARCHAR CHAR/BYTE 语义

Week 2:
- TC-P0-04: NUMBER 精度标度
- TC-P0-05: 外键级联规则
- 修复发现的 P0 问题
```

### 阶段 2: P1 复杂场景（2-3 周）
```
Week 3:
- TC-P1-01: 函数索引
- TC-P1-02: 多层 VIEW 依赖

Week 4:
- TC-P1-03: 跨 Schema 依赖
- TC-P1-04: PACKAGE 循环依赖

Week 5:
- 修复发现的 P1 问题
- 回归测试
```

### 阶段 3: P2 边界与性能（1-2 周）
```
Week 6:
- TC-P2-01: Interval 分区
- TC-P2-02: 大数据量性能

Week 7:
- TC-P2-03: 循环依赖检测
- 性能优化
- 完整回归测试
```

---

## 🔧 测试环境要求

### 数据库环境
```
Oracle 测试库:
- 版本: Oracle 11g/12c/19c
- 权限: DBA 或足够的 DBA_* 视图访问权限
- 测试 Schema: TEST_SRC_A, TEST_SRC_B, TEST_SRC_C

OceanBase 测试库:
- 版本: OceanBase 3.x/4.x (Oracle 模式)
- 权限: 管理员或足够的系统视图访问权限
- 测试 Schema: TEST_TGT_A, TEST_TGT_B, TEST_TGT_C
```

### 工具环境
```
Python: 3.7+
依赖库: requirements.txt
Oracle Client: 19c+
obclient: 最新版本
dbcat: 2.5.0+
SQLcl: 最新版本（可选，用于 DDL 格式化测试）
```

### 测试数据
```
建议使用专门的测试脚本生成，避免污染生产环境：
- tests/fixtures/generate_test_data.py
- tests/fixtures/oracle_test_schema.sql
- tests/fixtures/ob_test_schema.sql
```

---

## 📈 成功标准

### 代码覆盖率
```
- 元数据收集模块: > 90%
- 对比逻辑模块: > 85%
- DDL 生成模块: > 80%
- run_fixup 执行模块: > 75%
- 总体覆盖率: > 80%
```

### 缺陷密度
```
- P0 缺陷: 0 个（必须全部修复）
- P1 缺陷: < 5 个（修复或有明确的 workaround）
- P2 缺陷: < 10 个（可接受，但需文档化）
```

### 性能指标
```
- 1000 表环境: 元数据收集 + 对比 < 15 分钟
- 500 VIEW 依赖分析: < 5 分钟
- run_fixup 依赖排序: < 2 分钟
- 内存峰值: < 4 GB
```

---

## 📝 结论

本测试计划覆盖了从 REVIEW_10 深度功能审查中识别的关键业务场景和潜在问题。通过系统化的测试执行，可以：

1. **验证数据完整性保障**: CHECK 约束、虚拟列、精度标度等
2. **确保复杂场景支持**: 多层依赖、跨 Schema、循环依赖
3. **优化性能和稳定性**: 大数据量、边界情况
4. **提升工具可靠性**: 端到端覆盖真实迁移流程

**建议优先级**: P0 → P1 → P2，确保核心功能稳定后再扩展复杂场景支持。
