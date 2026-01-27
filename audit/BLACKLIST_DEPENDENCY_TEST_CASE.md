# 黑名单表依赖链测试案例设计

**设计日期**: 2026-01-27  
**目的**: 验证程序对不支持表及其依赖对象的处理逻辑  
**验证公式**: `缺失 + 不支持/阻断 - 多余 = Oracle源端数量`

---

## 一、测试目标

### 1.1 核心验证点

1. **统计准确性**: 检查汇总表中各对象类型的数量满足公式
2. **阻断传播**: 不支持表的依赖对象正确标记为 BLOCKED
3. **Fixup 生成**: 
   - 不支持/阻断对象 → 不生成 fixup
   - 真正缺失对象 → 生成 fixup
4. **分类输出**: 按对象类型输出不支持对象明细

### 1.2 黑名单表分类

**重要**: 黑名单表分为两类，处理逻辑不同：

| 类型 | BLACK_TYPE | 示例 | 依赖对象处理 |
|-----|-----------|------|------------|
| **类型转换表** | LONG | LONG→CLOB, LONG RAW→BLOB | 若目标端已转换，依赖对象**不阻断** |
| **真正不支持表** | SPE, DIY | XMLType, SDO_GEOMETRY, 自定义类型 | 依赖对象**阻断** |

### 1.3 测试场景覆盖

| 场景 | 源表类型 | 目标端状态 | 预期结果 |
|-----|---------|----------|---------|
| **场景A** | LONG 表 | 已转为 CLOB | 表匹配，依赖对象正常校验 |
| **场景B** | LONG 表 | 未转换/不存在 | 表缺失(需转换)，依赖对象正常校验 |
| **场景C** | SPE 表 (XMLType) | 不存在 | 表不支持，依赖对象全部阻断 |
| **场景D** | DIY 表 (自定义类型) | 不存在 | 表不支持，依赖对象全部阻断 |

### 1.4 对象类型覆盖

| 对象类型 | 测试内容 |
|---------|---------|
| TABLE | SPE表(XMLType)、DIY表(自定义类型)、LONG表(类型转换) |
| VIEW | 4级依赖链视图 |
| SYNONYM | 表同义词、视图同义词 |
| TRIGGER | 表触发器 |
| INDEX | 表索引（含普通索引、唯一索引） |
| CONSTRAINT | 表约束（含 PK、FK、CHECK、UNIQUE） |
| PROCEDURE | 依赖黑名单表的存储过程 |
| FUNCTION | 依赖黑名单表的函数 |
| PACKAGE | 依赖黑名单表的包 |

---

## 二、测试数据设计

### 2.1 Schema 规划

```
源端 Schema: TEST_BLACKLIST_SRC
目标端 Schema: TEST_BLACKLIST_TGT
```

### 2.2 对象依赖关系图

```
┌─────────────────────────────────────────────────────────────────────┐
│                   ★真正不支持表 (SPE/DIY) - 依赖阻断★                  │
│  ┌──────────────────┐  ┌──────────────────┐                         │
│  │ BL_TABLE_XMLTYPE │  │ BL_TABLE_UDT     │                         │
│  │ (XMLType 列)     │  │ (自定义类型列)    │                         │
│  │ BLACK_TYPE=SPE   │  │ BLACK_TYPE=DIY   │                         │
│  └────────┬─────────┘  └────────┬─────────┘                         │
│           │                      │                                   │
└───────────┼──────────────────────┼───────────────────────────────────┘
            │ (依赖全部阻断)         │
┌───────────┼──────────────────────┼───────────────────────────────────┐
│           │    类型转换表 (LONG) - 依赖不阻断                          │
│           │  ┌──────────────────┐  ┌──────────────────┐              │
│           │  │ CV_TABLE_LONG    │  │ CV_TABLE_LONGRAW │              │
│           │  │ (LONG→CLOB)      │  │ (LONG RAW→BLOB)  │              │
│           │  │ BLACK_TYPE=LONG  │  │ BLACK_TYPE=LONG  │              │
│           │  └────────┬─────────┘  └────────┬─────────┘              │
│           │           │ (依赖正常校验)       │                        │
└───────────┼───────────┼──────────────────────┼────────────────────────┘
            │                      │
            ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Level 1 依赖对象                              │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐        │
│  │ V_LEVEL1_A │ │ V_LEVEL1_B │ │ SYN_TABLE1 │ │ SYN_TABLE2 │        │
│  │ (视图)     │ │ (视图)     │ │ (同义词)   │ │ (同义词)   │        │
│  └─────┬──────┘ └─────┬──────┘ └────────────┘ └────────────┘        │
│        │              │                                              │
│  ┌─────┴──────────────┴─────┐                                        │
│  │ TRG_TABLE1, TRG_TABLE2   │ (触发器)                               │
│  │ IDX_TABLE1_*, IDX_TABLE2_*│ (索引)                                │
│  │ CHK_*, UK_*, FK_*        │ (约束)                                 │
│  └──────────────────────────┘                                        │
└─────────────────────────────────────────────────────────────────────┘
            │              │
            ▼              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Level 2 依赖对象                              │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐                       │
│  │ V_LEVEL2_A │ │ V_LEVEL2_B │ │ SYN_VIEW1  │                       │
│  │ (视图)     │ │ (视图)     │ │ (同义词)   │                       │
│  └─────┬──────┘ └─────┬──────┘ └────────────┘                       │
└────────┼──────────────┼─────────────────────────────────────────────┘
         │              │
         ▼              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Level 3 依赖对象                              │
│  ┌────────────┐ ┌────────────┐                                      │
│  │ V_LEVEL3_A │ │ V_LEVEL3_B │                                      │
│  └─────┬──────┘ └─────┬──────┘                                      │
└────────┼──────────────┼─────────────────────────────────────────────┘
         │              │
         ▼              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Level 4 依赖对象                              │
│  ┌────────────┐ ┌────────────┐                                      │
│  │ V_LEVEL4_A │ │ V_LEVEL4_B │                                      │
│  └────────────┘ └────────────┘                                      │
└─────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        PL/SQL 依赖对象                               │
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐           │
│  │ PROC_USE_BL    │ │ FUNC_USE_BL    │ │ PKG_USE_BL     │           │
│  │ (存储过程)     │ │ (函数)         │ │ (包)           │           │
│  └────────────────┘ └────────────────┘ └────────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 三、Oracle 测试脚本

### 3.1 创建黑名单表

```sql
-- ============================================================
-- 文件: 01_create_blacklist_tables.sql
-- 说明: 创建真正不支持表 (SPE/DIY) 和类型转换表 (LONG)
-- ============================================================

-- 清理已存在的对象
BEGIN
  FOR r IN (SELECT table_name FROM user_tables 
            WHERE table_name LIKE 'BL_%' OR table_name LIKE 'CV_%' OR table_name LIKE 'REF_%') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE ' || r.table_name || ' CASCADE CONSTRAINTS PURGE';
  END LOOP;
  FOR r IN (SELECT type_name FROM user_types WHERE type_name LIKE 'T_%') LOOP
    EXECUTE IMMEDIATE 'DROP TYPE ' || r.type_name || ' FORCE';
  END LOOP;
END;
/

-- ================================================================
-- 第一类: 真正不支持表 (SPE/DIY) - 依赖对象会被阻断
-- ================================================================

-- 自定义类型 (用于 DIY 黑名单表)
CREATE OR REPLACE TYPE T_ADDRESS AS OBJECT (
    street  VARCHAR2(100),
    city    VARCHAR2(50),
    zipcode VARCHAR2(20)
);
/

-- 黑名单表1: 含 XMLType 列 (BLACK_TYPE=SPE)
-- OceanBase 不支持 XMLType，此表及其依赖对象都应被阻断
CREATE TABLE BL_TABLE_XMLTYPE (
    id          NUMBER(10) PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    xml_data    XMLTYPE,                 -- ★不支持特征: XMLType
    status      VARCHAR2(20) DEFAULT 'ACTIVE',
    created_at  DATE DEFAULT SYSDATE,
    CONSTRAINT chk_blx_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'DELETED'))
);

-- 黑名单表2: 含自定义类型列 (BLACK_TYPE=DIY)
-- OceanBase 不支持用户自定义对象类型作为列类型
CREATE TABLE BL_TABLE_UDT (
    id          NUMBER(10) PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    address     T_ADDRESS,               -- ★不支持特征: 自定义类型
    status      VARCHAR2(20) DEFAULT 'ACTIVE',
    created_at  DATE DEFAULT SYSDATE,
    CONSTRAINT chk_blu_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'DELETED'))
);

-- ================================================================
-- 第二类: 类型转换表 (LONG) - 依赖对象不阻断
-- ================================================================

-- 类型转换表1: 含 LONG 列 (目标端应转为 CLOB)
CREATE TABLE CV_TABLE_LONG (
    id          NUMBER(10) PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    description LONG,                    -- 类型转换: LONG → CLOB
    status      VARCHAR2(20) DEFAULT 'ACTIVE',
    created_at  DATE DEFAULT SYSDATE,
    CONSTRAINT chk_cvl_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'DELETED'))
);

-- 类型转换表2: 含 LONG RAW 列 (目标端应转为 BLOB)
CREATE TABLE CV_TABLE_LONGRAW (
    id          NUMBER(10) PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    binary_data LONG RAW,                -- 类型转换: LONG RAW → BLOB
    file_type   VARCHAR2(50),
    created_at  DATE DEFAULT SYSDATE,
    CONSTRAINT chk_cvr_filetype CHECK (file_type IN ('PDF', 'DOC', 'IMG', 'OTHER'))
);

-- 普通参照表 (非黑名单，用于外键)
CREATE TABLE REF_STATUS (
    status_code VARCHAR2(20) PRIMARY KEY,
    status_name VARCHAR2(100)
);

INSERT INTO REF_STATUS VALUES ('ACTIVE', 'Active');
INSERT INTO REF_STATUS VALUES ('INACTIVE', 'Inactive');
COMMIT;

-- 为各类表添加更多约束
ALTER TABLE BL_TABLE_XMLTYPE ADD CONSTRAINT uk_blx_name UNIQUE (name);
ALTER TABLE BL_TABLE_XMLTYPE ADD CONSTRAINT fk_blx_status 
    FOREIGN KEY (status) REFERENCES REF_STATUS(status_code);

ALTER TABLE BL_TABLE_UDT ADD CONSTRAINT uk_blu_name UNIQUE (name);

ALTER TABLE CV_TABLE_LONG ADD CONSTRAINT uk_cvl_name UNIQUE (name);
ALTER TABLE CV_TABLE_LONGRAW ADD CONSTRAINT uk_cvr_name UNIQUE (name);
```

### 3.2 创建索引

```sql
-- ============================================================
-- 文件: 02_create_indexes.sql
-- 说明: 为各类表创建索引
-- ============================================================

-- ★真正不支持表的索引 (这些索引会被阻断)
CREATE INDEX idx_blx_status ON BL_TABLE_XMLTYPE(status);
CREATE INDEX idx_blx_created ON BL_TABLE_XMLTYPE(created_at);
CREATE INDEX idx_blu_status ON BL_TABLE_UDT(status);
CREATE INDEX idx_blu_created ON BL_TABLE_UDT(created_at);

-- 类型转换表的索引 (这些索引正常校验，不阻断)
CREATE INDEX idx_cvl_status ON CV_TABLE_LONG(status);
CREATE INDEX idx_cvl_created ON CV_TABLE_LONG(created_at);
CREATE INDEX idx_cvr_filetype ON CV_TABLE_LONGRAW(file_type);
CREATE INDEX idx_cvr_created ON CV_TABLE_LONGRAW(created_at);

-- 含 DESC 的索引 (OB 语法不支持，用于测试 INDEX_DESC)
CREATE INDEX idx_cvl_created_desc ON CV_TABLE_LONG(created_at DESC);
```

### 3.3 创建触发器

```sql
-- ============================================================
-- 文件: 03_create_triggers.sql
-- 说明: 为各类表创建触发器
-- ============================================================

-- ★真正不支持表的触发器 (这些触发器会被阻断)
CREATE OR REPLACE TRIGGER trg_blx_before_insert
BEFORE INSERT ON BL_TABLE_XMLTYPE
FOR EACH ROW
BEGIN
    IF :NEW.id IS NULL THEN
        SELECT NVL(MAX(id), 0) + 1 INTO :NEW.id FROM BL_TABLE_XMLTYPE;
    END IF;
    :NEW.created_at := SYSDATE;
END;
/

CREATE OR REPLACE TRIGGER trg_blu_before_insert
BEFORE INSERT ON BL_TABLE_UDT
FOR EACH ROW
BEGIN
    IF :NEW.id IS NULL THEN
        SELECT NVL(MAX(id), 0) + 1 INTO :NEW.id FROM BL_TABLE_UDT;
    END IF;
END;
/

-- 类型转换表的触发器 (这些触发器正常校验，不阻断)
CREATE OR REPLACE TRIGGER trg_cvl_before_insert
BEFORE INSERT ON CV_TABLE_LONG
FOR EACH ROW
BEGIN
    IF :NEW.id IS NULL THEN
        SELECT NVL(MAX(id), 0) + 1 INTO :NEW.id FROM CV_TABLE_LONG;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_cvr_before_insert
BEFORE INSERT ON CV_TABLE_LONGRAW
FOR EACH ROW
BEGIN
    IF :NEW.id IS NULL THEN
        SELECT NVL(MAX(id), 0) + 1 INTO :NEW.id FROM CV_TABLE_LONGRAW;
    END IF;
END;
/
```

### 3.4 创建 Level 1 视图

```sql
-- ============================================================
-- 文件: 04_create_views_level1.sql
-- 说明: 直接依赖表的视图 (Level 1)
-- ============================================================

-- ================================================================
-- ★依赖真正不支持表的视图 (会被阻断)
-- ================================================================

-- 依赖 BL_TABLE_XMLTYPE 的视图 (XMLType 不支持)
CREATE OR REPLACE VIEW V_BL_LEVEL1_A AS
SELECT id, name, status, created_at
FROM BL_TABLE_XMLTYPE
WHERE status = 'ACTIVE';

CREATE OR REPLACE VIEW V_BL_LEVEL1_B AS
SELECT id, name, status
FROM BL_TABLE_XMLTYPE;

-- 依赖 BL_TABLE_UDT 的视图 (自定义类型不支持)
CREATE OR REPLACE VIEW V_BL_LEVEL1_C AS
SELECT id, name, status, created_at
FROM BL_TABLE_UDT;

-- 联合两个不支持表的视图
CREATE OR REPLACE VIEW V_BL_LEVEL1_UNION AS
SELECT 'XMLTYPE' AS source_type, id, name, status, created_at
FROM BL_TABLE_XMLTYPE
UNION ALL
SELECT 'UDT' AS source_type, id, name, status, created_at
FROM BL_TABLE_UDT;

-- ================================================================
-- 类型转换表的视图 (不阻断，正常校验)
-- ================================================================

-- 依赖 CV_TABLE_LONG 的视图
CREATE OR REPLACE VIEW V_CV_LEVEL1_A AS
SELECT id, name, status, created_at
FROM CV_TABLE_LONG
WHERE status = 'ACTIVE';

-- 依赖 CV_TABLE_LONGRAW 的视图
CREATE OR REPLACE VIEW V_CV_LEVEL1_B AS
SELECT id, name, file_type, created_at
FROM CV_TABLE_LONGRAW;
```

### 3.5 创建 Level 2-4 视图

```sql
-- ============================================================
-- 文件: 05_create_views_level2to4.sql
-- 说明: 多级视图依赖链 (Level 2-4) - 仅针对不支持表
-- ============================================================

-- ================================================================
-- ★Level 2 视图 (依赖 Level 1 阻断视图，也会被阻断)
-- ================================================================
CREATE OR REPLACE VIEW V_BL_LEVEL2_A AS
SELECT id, name, status
FROM V_BL_LEVEL1_A
WHERE id > 0;

CREATE OR REPLACE VIEW V_BL_LEVEL2_B AS
SELECT a.id, a.name AS name_a, b.name AS name_b
FROM V_BL_LEVEL1_A a
JOIN V_BL_LEVEL1_C b ON a.id = b.id;

CREATE OR REPLACE VIEW V_BL_LEVEL2_C AS
SELECT * FROM V_BL_LEVEL1_UNION;

-- ================================================================
-- ★Level 3 视图 (依赖 Level 2 阻断视图，也会被阻断)
-- ================================================================
CREATE OR REPLACE VIEW V_BL_LEVEL3_A AS
SELECT id, name, status
FROM V_BL_LEVEL2_A;

CREATE OR REPLACE VIEW V_BL_LEVEL3_B AS
SELECT id, name_a, name_b
FROM V_BL_LEVEL2_B;

-- ================================================================
-- ★Level 4 视图 (依赖 Level 3 阻断视图，也会被阻断)
-- ================================================================
CREATE OR REPLACE VIEW V_BL_LEVEL4_A AS
SELECT * FROM V_BL_LEVEL3_A;

CREATE OR REPLACE VIEW V_BL_LEVEL4_B AS
SELECT l3a.id, l3a.name, l3b.name_b
FROM V_BL_LEVEL3_A l3a
JOIN V_BL_LEVEL3_B l3b ON l3a.id = l3b.id;
```

### 3.6 创建同义词

```sql
-- ============================================================
-- 文件: 06_create_synonyms.sql
-- 说明: 为各类表和视图创建同义词
-- ============================================================

-- ★不支持表的同义词 (会被阻断)
CREATE OR REPLACE SYNONYM SYN_BL_XMLTYPE FOR BL_TABLE_XMLTYPE;
CREATE OR REPLACE SYNONYM SYN_BL_UDT FOR BL_TABLE_UDT;

-- ★阻断视图的同义词 (会被阻断)
CREATE OR REPLACE SYNONYM SYN_V_BL_LEVEL1_A FOR V_BL_LEVEL1_A;
CREATE OR REPLACE SYNONYM SYN_V_BL_LEVEL4_A FOR V_BL_LEVEL4_A;

-- 类型转换表的同义词 (不阻断)
CREATE OR REPLACE SYNONYM SYN_CV_LONG FOR CV_TABLE_LONG;
CREATE OR REPLACE SYNONYM SYN_CV_LONGRAW FOR CV_TABLE_LONGRAW;

-- 类型转换表视图的同义词 (不阻断)
CREATE OR REPLACE SYNONYM SYN_V_CV_LEVEL1_A FOR V_CV_LEVEL1_A;
```

### 3.7 创建 PL/SQL 对象

```sql
-- ============================================================
-- 文件: 07_create_plsql.sql
-- 说明: 依赖各类表的存储过程、函数、包
-- ============================================================

-- ★依赖不支持表的存储过程 (会被阻断)
CREATE OR REPLACE PROCEDURE PROC_USE_BL_XMLTYPE (
    p_id IN NUMBER,
    p_name OUT VARCHAR2
) AS
BEGIN
    SELECT name INTO p_name FROM BL_TABLE_XMLTYPE WHERE id = p_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        p_name := NULL;
END;
/

-- ★依赖不支持表的函数 (会被阻断)
CREATE OR REPLACE FUNCTION FUNC_GET_BL_COUNT RETURN NUMBER AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM BL_TABLE_XMLTYPE;
    RETURN v_count;
END;
/

-- ★依赖阻断视图链的函数 (会被阻断，间接依赖)
CREATE OR REPLACE FUNCTION FUNC_GET_BL_LEVEL4_COUNT RETURN NUMBER AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM V_BL_LEVEL4_A;
    RETURN v_count;
END;
/

-- 依赖黑名单表的包
CREATE OR REPLACE PACKAGE PKG_USE_BL AS
    FUNCTION get_name(p_id NUMBER) RETURN VARCHAR2;
    PROCEDURE update_status(p_id NUMBER, p_status VARCHAR2);
END PKG_USE_BL;
/

CREATE OR REPLACE PACKAGE BODY PKG_USE_BL AS
    FUNCTION get_name(p_id NUMBER) RETURN VARCHAR2 AS
        v_name VARCHAR2(100);
    BEGIN
        SELECT name INTO v_name FROM BL_TABLE_XMLTYPE WHERE id = p_id;
        RETURN v_name;
    END;
    
    PROCEDURE update_status(p_id NUMBER, p_status VARCHAR2) AS
    BEGIN
        UPDATE BL_TABLE_XMLTYPE SET status = p_status WHERE id = p_id;
    END;
END PKG_USE_BL;
/

-- ================================================================
-- 类型转换表的 PL/SQL (不阻断，正常校验)
-- ================================================================

CREATE OR REPLACE PROCEDURE PROC_USE_CV_LONG (
    p_id IN NUMBER,
    p_name OUT VARCHAR2
) AS
BEGIN
    SELECT name INTO p_name FROM CV_TABLE_LONG WHERE id = p_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        p_name := NULL;
END;
/

CREATE OR REPLACE FUNCTION FUNC_GET_CV_COUNT RETURN NUMBER AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM CV_TABLE_LONG;
    RETURN v_count;
END;
/
```

### 3.8 创建对照组（正常对象）

```sql
-- ============================================================
-- 文件: 08_create_normal_objects.sql
-- 说明: 创建正常对象作为对照组，验证正常对象能正确生成 fixup
-- ============================================================

-- 正常表 (无黑名单特征)
CREATE TABLE NORMAL_TABLE (
    id          NUMBER(10) PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    description CLOB,                    -- 使用 CLOB 而非 LONG
    status      VARCHAR2(20) DEFAULT 'ACTIVE',
    created_at  DATE DEFAULT SYSDATE
);

-- 正常表的索引
CREATE INDEX idx_normal_status ON NORMAL_TABLE(status);

-- 正常表的视图
CREATE OR REPLACE VIEW V_NORMAL AS
SELECT id, name, status FROM NORMAL_TABLE;

-- 正常表的同义词
CREATE OR REPLACE SYNONYM SYN_NORMAL FOR NORMAL_TABLE;

-- 正常存储过程
CREATE OR REPLACE PROCEDURE PROC_NORMAL (p_id IN NUMBER) AS
BEGIN
    NULL;
END;
/
```

---

## 四、测试对象清单

### 4.1 对象统计表

| 对象类型 | 总数 | 阻断(SPE/DIY依赖) | 不阻断(LONG依赖) | 正常对象 |
|---------|-----|------------------|-----------------|---------|
| TABLE | 6 | 2 (BL_TABLE_XMLTYPE, BL_TABLE_UDT) | 2 (CV_TABLE_LONG/RAW) | 2 (REF_STATUS, NORMAL) |
| VIEW | 14 | 10 (V_BL_LEVEL1-4) | 2 (V_CV_LEVEL1_A/B) | 1 (V_NORMAL) |
| SYNONYM | 7 | 4 (SYN_BL_*) | 3 (SYN_CV_*) | 1 (SYN_NORMAL) |
| TRIGGER | 4 | 2 (trg_blx_*, trg_blu_*) | 2 (trg_cvl_*, trg_cvr_*) | 0 |
| INDEX | 9 | 4 (idx_blx_*, idx_blu_*) | 5 (idx_cvl_*, idx_cvr_*含DESC) | 1 (idx_normal_*) |
| CONSTRAINT | 10 | 4 (BL表约束) | 4 (CV表约束) | 2 (NORMAL+REF约束) |
| PROCEDURE | 3 | 1 (PROC_USE_BL_XMLTYPE) | 1 (PROC_USE_CV_LONG) | 1 (PROC_NORMAL) |
| FUNCTION | 3 | 2 (FUNC_GET_BL_*, FUNC_GET_BL_LEVEL4_*) | 1 (FUNC_GET_CV_COUNT) | 0 |
| PACKAGE | 1 | 1 (PKG_USE_BL) | 0 | 0 |
| PACKAGE BODY | 1 | 1 (PKG_USE_BL) | 0 | 0 |
| TYPE | 1 | 1 (T_ADDRESS) | 0 | 0 |

### 4.2 预期行为

#### 真正不支持表 (SPE/DIY) - 依赖对象阻断

| 对象 | OceanBase 状态 | 预期分类 | 生成 Fixup | 说明 |
|-----|---------------|---------|-----------|------|
| BL_TABLE_XMLTYPE | 不存在 | UNSUPPORTED | ❌ | XMLType 不支持 |
| BL_TABLE_UDT | 不存在 | UNSUPPORTED | ❌ | 自定义类型不支持 |
| V_BL_LEVEL1_A | 不存在 | BLOCKED | ❌ | 依赖 BL_TABLE_XMLTYPE |
| V_BL_LEVEL2_A | 不存在 | BLOCKED | ❌ | 依赖 V_BL_LEVEL1_A |
| V_BL_LEVEL3_A | 不存在 | BLOCKED | ❌ | 依赖 V_BL_LEVEL2_A |
| V_BL_LEVEL4_A | 不存在 | BLOCKED | ❌ | 依赖 V_BL_LEVEL3_A |
| SYN_BL_XMLTYPE | 不存在 | BLOCKED | ❌ | 依赖不支持表 |
| trg_blx_before_insert | 不存在 | BLOCKED | ❌ | 依赖不支持表 |
| idx_blx_status | 不存在 | BLOCKED | ❌ | 依赖不支持表 |
| PROC_USE_BL_XMLTYPE | 不存在 | BLOCKED | ❌ | 依赖不支持表 |
| PKG_USE_BL | 不存在 | BLOCKED | ❌ | 依赖不支持表 |

#### 类型转换表 (LONG) - 依赖对象不阻断

| 对象 | OceanBase 状态 | 预期分类 | 生成 Fixup | 说明 |
|-----|---------------|---------|-----------|------|
| CV_TABLE_LONG | 已转CLOB | MATCHED | - | LONG→CLOB 已转换 |
| CV_TABLE_LONG | 未转换 | MISSING(需转换) | ❌ | 需先手工转换 |
| V_CV_LEVEL1_A | 不存在 | MISSING | ✅ | 依赖正常，可生成fixup |
| SYN_CV_LONG | 不存在 | MISSING | ✅ | 依赖正常 |
| trg_cvl_before_insert | 不存在 | MISSING | ✅ | 依赖正常 |
| PROC_USE_CV_LONG | 不存在 | MISSING | ✅ | 依赖正常 |

#### 正常对象 - 对照组

| 对象 | OceanBase 状态 | 预期分类 | 生成 Fixup |
|-----|---------------|---------|-----------|
| NORMAL_TABLE | 不存在 | MISSING | ✅ |
| V_NORMAL | 不存在 | MISSING | ✅ |
| SYN_NORMAL | 不存在 | MISSING | ✅ |
| PROC_NORMAL | 不存在 | MISSING | ✅ |

---

## 五、统计公式验证

### 5.1 公式定义

```
Oracle源端数量 = 目标端存在 + 缺失 + 不支持/阻断 - 多余
```

### 5.2 按对象类型验证

以 VIEW 为例：

| 统计项 | 数量 | 说明 |
|-------|-----|------|
| Oracle 源端 | 13 | 所有视图 |
| 目标端存在 | 0 | (假设空库) |
| 缺失 | 1 | V_NORMAL |
| 不支持/阻断 | 12 | V_LEVEL1-4 系列 |
| 多余 | 0 | - |
| **验证** | 0 + 1 + 12 - 0 = **13** ✅ | |

### 5.3 INDEX/CONSTRAINT 特殊处理

**关键问题**: 表不存在时，其 INDEX/CONSTRAINT 如何统计？

**设计决策**:
1. **索引 (INDEX)**: 
   - 若表被标记为 UNSUPPORTED/BLOCKED → 索引计入 `extra_blocked_counts["INDEX"]`
   - 汇总表显示为 "不支持/阻断"
   - 不单独统计为 "缺失"

2. **约束 (CONSTRAINT)**:
   - 同索引处理逻辑
   - PK/UK/FK/CHECK 统一计入 `extra_blocked_counts["CONSTRAINT"]`

3. **触发器 (TRIGGER)**:
   - 若表被标记为 UNSUPPORTED/BLOCKED → 计入 `extra_blocked_counts["TRIGGER"]`

**统计公式对 INDEX/CONSTRAINT/TRIGGER 的适用**:
```
Oracle源端数量 = 目标端存在 + 缺失(真正缺失) + 不支持/阻断(含依赖阻断) - 多余
```

---

## 六、输出文件设计

### 6.1 新增不支持对象分类输出

**需求**: 按对象类型分别输出不支持/阻断对象

**文件命名规范**:
```
unsupported_view_detail_{timestamp}.txt
unsupported_synonym_detail_{timestamp}.txt
unsupported_trigger_detail_{timestamp}.txt
unsupported_procedure_detail_{timestamp}.txt
unsupported_function_detail_{timestamp}.txt
unsupported_package_detail_{timestamp}.txt
unsupported_index_detail_{timestamp}.txt      -- 现有 indexes_unsupported_detail 扩展
unsupported_constraint_detail_{timestamp}.txt -- 现有 constraints_unsupported_detail 扩展
```

### 6.2 文件内容格式

```
# 不支持/阻断 VIEW 明细
# timestamp=2026-01-27_153000
# total=10

SRC_FULL              | TGT_FULL              | STATE   | REASON_CODE           | REASON                            | ROOT_CAUSE
----------------------|-----------------------|---------|----------------------|-----------------------------------|------------------------
SCHEMA.V_BL_LEVEL1_A  | SCHEMA.V_BL_LEVEL1_A  | BLOCKED | DEPENDENCY_UNSUPPORTED| 依赖表 BL_TABLE_XMLTYPE 不支持     | BL_TABLE_XMLTYPE(SPE)
SCHEMA.V_BL_LEVEL2_A  | SCHEMA.V_BL_LEVEL2_A  | BLOCKED | DEPENDENCY_UNSUPPORTED| 依赖视图 V_BL_LEVEL1_A 被阻断      | BL_TABLE_XMLTYPE(SPE)
SCHEMA.V_BL_LEVEL3_A  | SCHEMA.V_BL_LEVEL3_A  | BLOCKED | DEPENDENCY_UNSUPPORTED| 依赖视图 V_BL_LEVEL2_A 被阻断      | BL_TABLE_XMLTYPE(SPE)
SCHEMA.V_BL_LEVEL4_A  | SCHEMA.V_BL_LEVEL4_A  | BLOCKED | DEPENDENCY_UNSUPPORTED| 依赖视图 V_BL_LEVEL3_A 被阻断      | BL_TABLE_XMLTYPE(SPE)
```

### 6.3 字段说明

| 字段 | 说明 |
|-----|------|
| SRC_FULL | 源端对象全名 |
| TGT_FULL | 目标端对象全名 |
| STATE | UNSUPPORTED / BLOCKED |
| REASON_CODE | 原因代码 |
| REASON | 直接原因 |
| ROOT_CAUSE | 根因对象（追溯到源头黑名单表） |

---

## 七、实现方案

### 7.1 代码修改点

#### 7.1.1 新增导出函数

在 `schema_diff_reconciler.py` 中新增：

```python
def export_unsupported_by_type(
    unsupported_rows: List[ObjectSupportReportRow],
    report_dir: Path,
    report_timestamp: str
) -> Dict[str, Path]:
    """
    按对象类型分别输出不支持/阻断对象明细。
    返回: {object_type: output_path} 字典
    """
    # 按类型分组
    by_type: Dict[str, List[ObjectSupportReportRow]] = defaultdict(list)
    for row in unsupported_rows:
        if row.state in ('UNSUPPORTED', 'BLOCKED'):
            by_type[row.obj_type].append(row)
    
    result = {}
    for obj_type, rows in by_type.items():
        type_lower = obj_type.lower().replace(' ', '_')
        output_path = report_dir / f"unsupported_{type_lower}_detail_{report_timestamp}.txt"
        # ... 写入文件
        result[obj_type] = output_path
    
    return result
```

#### 7.1.2 修改 print_final_report

在报告生成阶段调用新函数：

```python
# 现有代码后添加
if emit_detail_files and support_summary.unsupported_rows:
    unsupported_files = export_unsupported_by_type(
        support_summary.unsupported_rows,
        report_dir,
        report_timestamp
    )
    for obj_type, path in unsupported_files.items():
        report_index_rows.append(...)
```

#### 7.1.3 ROOT_CAUSE 追溯

需要在 `classify_missing_objects` 中记录依赖链根因：

```python
@dataclass
class ObjectSupportReportRow:
    # ... 现有字段
    root_cause: str = ""  # 新增: 根因对象标识
```

### 7.2 统计逻辑调整

#### 7.2.1 确保 INDEX/CONSTRAINT/TRIGGER 正确统计

验证 `extra_blocked_counts` 的填充逻辑正确覆盖：
- 表被标记为 UNSUPPORTED → 其 INDEX/CONSTRAINT/TRIGGER 计入 extra_blocked_counts
- 不重复统计到 "缺失" 列

#### 7.2.2 汇总表验证

确保 `build_unsupported_summary_counts()` 正确合并：
- `missing_support_counts` (主对象)
- `extra_blocked_counts` (INDEX/CONSTRAINT/TRIGGER)
- `extra_results["index_unsupported"]` (DESC 索引)
- `extra_results["constraint_unsupported"]` (DEFERRABLE 约束)

---

## 八、验证检查清单

### 8.1 执行前准备

- [ ] 在 Oracle 中执行所有创建脚本 (01-08)
- [ ] 确保 OceanBase 目标 schema 为空或仅有参照表
- [ ] 配置黑名单规则（SPE/DIY 类型的表）
- [ ] 对于类型转换场景，在 OB 创建已转换的 CV_TABLE_LONG (LONG→CLOB)

### 8.2 执行校验

```bash
python schema_diff_reconciler.py \
  --config config.yaml \
  --report_detail_mode split \
  --check_extra_objects index,constraint,trigger \
  --fixup_types all
```

### 8.3 验证项目

#### 场景一：真正不支持表 (SPE/DIY) 验证

| 序号 | 验证项 | 预期结果 | 实际结果 |
|-----|-------|---------|---------|
| 1 | BL_TABLE_XMLTYPE 状态 | UNSUPPORTED (SPE) | |
| 2 | BL_TABLE_UDT 状态 | UNSUPPORTED (DIY) | |
| 3 | V_BL_LEVEL1_A 状态 | BLOCKED | |
| 4 | V_BL_LEVEL4_A 状态 | BLOCKED (4级依赖) | |
| 5 | SYN_BL_XMLTYPE 状态 | BLOCKED | |
| 6 | trg_blx_before_insert 状态 | BLOCKED | |
| 7 | idx_blx_status 状态 | BLOCKED | |
| 8 | PROC_USE_BL_XMLTYPE 状态 | BLOCKED | |
| 9 | PKG_USE_BL 状态 | BLOCKED | |
| 10 | BL_TABLE_XMLTYPE 的 fixup | 不存在 | |
| 11 | V_BL_LEVEL*_* 的 fixup | 不存在 | |

#### 场景二：类型转换表 (LONG) 验证

| 序号 | 验证项 | 预期结果 | 实际结果 |
|-----|-------|---------|---------|
| 12 | CV_TABLE_LONG 状态 (目标已转CLOB) | MATCHED | |
| 13 | CV_TABLE_LONG 状态 (目标未转换) | MISSING (需转换) | |
| 14 | V_CV_LEVEL1_A 状态 | MISSING (不阻断) | |
| 15 | SYN_CV_LONG 状态 | MISSING (不阻断) | |
| 16 | trg_cvl_before_insert 状态 | MISSING (不阻断) | |
| 17 | V_CV_LEVEL1_A 的 fixup | ✅ 存在 | |
| 18 | PROC_USE_CV_LONG 的 fixup | ✅ 存在 | |

#### 场景三：统计公式验证

| 序号 | 验证项 | 预期结果 | 实际结果 |
|-----|-------|---------|---------|
| 19 | VIEW: 缺失+不支持-多余=源端 | 3+10-0=13 ✓ | |
| 20 | SYNONYM: 缺失+不支持-多余=源端 | 4+4-0=8 ✓ | |
| 21 | TRIGGER: 缺失+不支持-多余=源端 | 2+2-0=4 ✓ | |
| 22 | INDEX: 缺失+不支持-多余=源端 | 验证 | |
| 23 | PROCEDURE: 缺失+不支持-多余=源端 | 2+1-0=3 ✓ | |

#### 场景四：输出文件验证

| 序号 | 验证项 | 预期结果 | 实际结果 |
|-----|-------|---------|---------|
| 24 | unsupported_objects_detail | 包含所有阻断对象 | |
| 25 | unsupported_view_detail | 包含10个阻断视图 | |
| 26 | unsupported_synonym_detail | 包含4个阻断同义词 | |
| 27 | unsupported_trigger_detail | 包含2个阻断触发器 | |
| 28 | unsupported_index_detail | 包含4个阻断索引 | |
| 29 | fixup 目录 | 包含 CV_*、NORMAL_* 对象 | |

---

## 九、附录

### 9.1 清理脚本

```sql
-- cleanup.sql
BEGIN
  -- 先删除自定义类型的依赖对象
  FOR r IN (SELECT object_name, object_type FROM user_objects 
            WHERE object_name LIKE 'BL_%' OR object_name LIKE 'CV_%'
               OR object_name LIKE 'V_BL_%' OR object_name LIKE 'V_CV_%'
               OR object_name LIKE 'SYN_%' OR object_name LIKE 'TRG_%'
               OR object_name LIKE 'PROC_%' OR object_name LIKE 'FUNC_%'
               OR object_name LIKE 'PKG_%' OR object_name LIKE 'NORMAL%'
               OR object_name LIKE 'REF_%' OR object_name LIKE 'T_%'
            ORDER BY DECODE(object_type, 'PACKAGE BODY', 1, 'PACKAGE', 2, 
                           'PROCEDURE', 3, 'FUNCTION', 4, 'VIEW', 5, 
                           'SYNONYM', 6, 'TRIGGER', 7, 'TABLE', 8, 'TYPE', 9)) 
  LOOP
    BEGIN
      IF r.object_type = 'TABLE' THEN
        EXECUTE IMMEDIATE 'DROP TABLE ' || r.object_name || ' CASCADE CONSTRAINTS PURGE';
      ELSIF r.object_type = 'TYPE' THEN
        EXECUTE IMMEDIATE 'DROP TYPE ' || r.object_name || ' FORCE';
      ELSE
        EXECUTE IMMEDIATE 'DROP ' || r.object_type || ' ' || r.object_name;
      END IF;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END LOOP;
END;
/
```

### 9.2 黑名单规则配置

**说明**: 真正不支持的表需要通过 `blacklist_rules.json` 或 OMS 黑名单表配置。

```json
// blacklist_rules.json 示例
[
  {
    "id": "xmltype_columns",
    "black_type": "SPE",
    "sql": "SELECT OWNER, TABLE_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS WHERE DATA_TYPE = 'XMLTYPE' AND OWNER IN ({owners_clause})"
  },
  {
    "id": "udt_columns",
    "black_type": "DIY",
    "sql": "SELECT c.OWNER, c.TABLE_NAME, t.TYPE_NAME FROM ALL_TAB_COLUMNS c JOIN ALL_TYPES t ON c.DATA_TYPE = t.TYPE_NAME WHERE t.TYPECODE = 'OBJECT' AND c.OWNER IN ({owners_clause})"
  }
]
```

### 9.3 关键说明

| 类型 | BLACK_TYPE | 依赖对象处理 | fixup 生成 |
|-----|-----------|------------|-----------|
| LONG/LONG RAW | LONG | **不阻断**（若已转换CLOB/BLOB） | 依赖对象正常生成 |
| XMLType | SPE | **阻断** | 不生成 |
| 自定义类型 | DIY | **阻断** | 不生成 |
| 临时表 | TEMP_TABLE | **阻断** | 不生成 |

---

**文档版本**: v2.0  
**最后更新**: 2026-01-27  
**变更说明**: 区分类型转换表(LONG)和真正不支持表(SPE/DIY)的处理逻辑
