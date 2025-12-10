# 索引和约束命名冲突检测与自动解决指南

## 概述

从 v0.8.2 开始，OceanBase Comparator Toolkit 自动检测并解决索引和约束的命名冲突问题。这在以下场景中特别有用：

1. **多对一映射**：多个源schema的表被remap到同一个目标schema
2. **表重命名场景**：表被重命名为 `XXXXX_RENAME_20251118` 后，原表需要重建
3. **目标端已存在对象**：目标schema中已有同名的索引或约束

## 问题场景示例

### 场景1：多对一映射导致的冲突

```
源端：
  SCHEMA_A.USERS (索引: IDX_EMAIL, 约束: UK_USERNAME)
  SCHEMA_B.USERS (索引: IDX_EMAIL, 约束: UK_USERNAME)

Remap规则：
  SCHEMA_A.USERS = TARGET.USERS_A
  SCHEMA_B.USERS = TARGET.USERS_B

问题：
  两个表都有名为 IDX_EMAIL 和 UK_USERNAME 的对象
  如果直接创建，会报错：ORA-00955: 名称已由现有对象使用
```

### 场景2：表重命名后的冲突（增强版）

```
目标端现状：
  TARGET.ORDERS_RENAME_20251118 (表已重命名)
  - 索引: IDX_ORDER_DATE (名称未变！)
  - 约束: PK_ORDERS (名称未变！)
  
  或者：
  TARGET.ORDERS_BACKUP20251118 (无下划线)
  TARGET.ORDERS_OLD_20251118
  TARGET.ORDERS_BAK20251118
  TARGET.ORDERS_HIST_20251118
  TARGET.ORDERS_ARCHIVE_20251118

需要重建：
  TARGET.ORDERS (新表，从源端导出的DDL)
  - 索引: IDX_ORDER_DATE (与旧表索引冲突！)
  - 约束: PK_ORDERS (与旧表约束冲突！)

问题：
  旧表的索引/约束名称未随表重命名而改变
  创建新表时会报名称冲突：ORA-00955

程序检测：
  ✓ 自动识别 ORDERS_RENAME_20251118 是 ORDERS 的重命名版本
  ✓ 检测到其索引/约束与即将创建的 ORDERS 表冲突
  ✓ 为新表的索引/约束自动重命名
```

**支持的重命名模式**：
- `_RENAME_YYYYMMDD` 或 `_RENAMEYYYYMMDD`
- `_BACKUP_YYYYMMDD` 或 `_BACKUPYYYYMMDD`
- `_BAK_YYYYMMDD` 或 `_BAKYYYYMMDD`
- `_OLD_YYYYMMDD` 或 `_OLDYYYYMMDD`
- `_HIST_YYYYMMDD` 或 `_HISTYYYYMMDD`
- `_HISTORY_YYYYMMDD` 或 `_HISTORYYYYYMMDD`
- `_ARCHIVE_YYYYMMDD` 或 `_ARCHIVEYYYYMMDD`
- `_ARC_YYYYMMDD` 或 `_ARCYYYYMMDD`
- `_TMP_YYYYMMDD` 或 `_TMPYYYYMMDD`
- `_TEMP_YYYYMMDD` 或 `_TEMPYYYYMMDD`
- `_YYYYMMDD` (纯日期后缀)
- `YYYYMMDD` (无下划线的日期后缀)

**日期格式支持**：
- 8位: `YYYYMMDD` (20251118)
- 6位: `YYMMDD` (251118)
- 4位: `YYMM` (2511)

### 场景3：目标端已存在同名对象

```
目标端已有：
  TARGET.PRODUCTS (索引: IDX_CATEGORY)

源端迁移：
  SOURCE.ITEMS = TARGET.ITEMS
  - 索引: IDX_CATEGORY (与PRODUCTS表的索引同名！)

问题：
  即使表名不同，索引名在同一schema下也不能重复
```

## 自动解决方案

### 冲突检测

程序在生成修补脚本前会自动：

1. **扫描所有表**：收集每个目标schema下所有表的索引和约束名称
2. **检测冲突**：识别同一schema下出现多次的名称
3. **包含已存在对象**：检查目标端已有的索引/约束
4. **智能识别重命名表**：
   - 检测目标端是否有重命名的表（如 `ORDERS_RENAME_20251118`）
   - 提取原始表名（`ORDERS`）
   - 如果即将创建同名表，标记其索引/约束为潜在冲突
   - 支持多种重命名模式（RENAME/BACKUP/OLD/BAK/HIST等）

### 智能重命名策略

对于检测到的冲突，程序按以下优先级生成新名称：

#### 策略1：提取表名中的日期后缀

如果表名包含 `_RENAME_YYYYMMDD` 模式：

```
原表名: ORDERS_RENAME_20251118
原索引名: IDX_ORDER_DATE
新索引名: IDX_ORDER_DATE_20251118
```

#### 策略2：使用表名后缀

如果表名没有日期模式，使用表名的最后部分：

```
表名: USERS_A
原索引名: IDX_EMAIL
新索引名: IDX_EMAIL_A

表名: CUSTOMER_HISTORY
原约束名: PK_CUSTOMER
新约束名: PK_CUSTOMER_HISTORY
```

#### 策略3：长度限制处理

Oracle/OceanBase 对象名称限制为30字符，程序会自动截断：

```
原索引名: IDX_VERY_LONG_INDEX_NAME_HERE
表后缀: _HISTORY
新索引名: IDX_VERY_LONG_INDEX_NA_HISTORY (截断到30字符)
```

#### 策略4：数字后缀（极端情况）

如果上述策略仍然冲突，添加数字后缀：

```
新索引名: IDX_EMAIL_A_01
新索引名: IDX_EMAIL_A_02
```

## 应用范围

### 自动重命名的对象类型

1. **CREATE TABLE 中的内联约束**
   ```sql
   CREATE TABLE TARGET.USERS_A (
     ID NUMBER,
     EMAIL VARCHAR2(100),
     CONSTRAINT PK_USERS_A PRIMARY KEY (ID),  -- 自动重命名
     CONSTRAINT UK_EMAIL_A UNIQUE (EMAIL)     -- 自动重命名
   );
   ```

2. **独立的 CREATE INDEX 语句**
   ```sql
   CREATE INDEX TARGET.IDX_EMAIL_A ON TARGET.USERS_A (EMAIL);
   -- 原名: IDX_EMAIL, 已重命名避免冲突
   ```

3. **ALTER TABLE ADD CONSTRAINT 语句**
   ```sql
   ALTER TABLE TARGET.USERS_A 
     ADD CONSTRAINT FK_DEPT_A FOREIGN KEY (DEPT_ID) 
     REFERENCES TARGET.DEPARTMENTS (ID);
   -- 原名: FK_DEPT, 已重命名避免冲突
   ```

### 不会重命名的对象

- **PACKAGE 和 PACKAGE BODY**：必须同名，不会重命名
- **主表名称**：只重命名索引和约束，不重命名表本身
- **列名**：列名冲突需要手工处理

## 日志输出示例

### 冲突检测阶段

```
[FIXUP] 检测索引和约束的命名冲突...
[冲突检测] 发现重命名表 TARGET.ORDERS_RENAME_20251118，其索引/约束可能与即将创建的表 TARGET.ORDERS 冲突
[冲突检测] 发现重命名表 TARGET.USERS_BACKUP20251209，其索引/约束可能与即将创建的表 TARGET.USERS 冲突
[FIXUP] 检测到INDEX名称冲突: TARGET.IDX_EMAIL 在表 USERS_A 上，重命名为 IDX_EMAIL_A
[FIXUP] 检测到INDEX名称冲突: TARGET.IDX_EMAIL 在表 USERS_B 上，重命名为 IDX_EMAIL_B
[FIXUP] 检测到INDEX名称冲突: TARGET.IDX_ORDER_DATE 在表 ORDERS 上，重命名为 IDX_ORDER_DATE_RENAME_20251118
[FIXUP] 检测到INDEX名称冲突: TARGET.IDX_ORDER_DATE 在表 ORDERS_RENAME_20251118(已存在,来自重命名表) 上
[FIXUP] 检测到CONSTRAINT名称冲突: TARGET.PK_ORDERS 在表 ORDERS 上，重命名为 PK_ORDERS_RENAME_20251118
[FIXUP] 检测到CONSTRAINT名称冲突: TARGET.PK_ORDERS 在表 ORDERS_RENAME_20251118(已存在,来自重命名表) 上
[FIXUP] 命名冲突检测完成: INDEX=4, CONSTRAINT=2 个对象将被重命名
```

### 脚本生成阶段

```
[FIXUP] 索引重命名以避免冲突: TARGET.IDX_EMAIL -> TARGET.IDX_EMAIL_A (表: TARGET.USERS_A)
[FIXUP][DBCAT] 写入 INDEX 脚本: TARGET.IDX_EMAIL_A.sql
[FIXUP] 约束重命名以避免冲突: TARGET.PK_ORDERS -> TARGET.PK_ORDERS_20251118 (表: TARGET.ORDERS)
[FIXUP][META] 写入 CONSTRAINT 脚本: TARGET.PK_ORDERS_20251118.sql
```

## 生成的脚本示例

### 索引脚本 (TARGET.IDX_EMAIL_A.sql)

```sql
-- ============================================================
-- 修补缺失的 INDEX IDX_EMAIL_A (表: TARGET.USERS_A)
-- [原名: IDX_EMAIL, 已重命名避免冲突]
-- ============================================================

SET SCHEMA TARGET;

CREATE INDEX TARGET.IDX_EMAIL_A ON TARGET.USERS_A (EMAIL);
```

### 约束脚本 (TARGET.PK_ORDERS_20251118.sql)

```sql
-- ============================================================
-- 修补缺失的约束 PK_ORDERS_20251118 (表: TARGET.ORDERS)
-- [原名: PK_ORDERS, 已重命名避免冲突]
-- ============================================================

SET SCHEMA TARGET;

ALTER TABLE TARGET.ORDERS 
  ADD CONSTRAINT PK_ORDERS_20251118 PRIMARY KEY (ORDER_ID);
```

### 表脚本 (TARGET.USERS_A.sql)

```sql
-- ============================================================
-- 修补缺失的 TABLE TARGET.USERS_A (源: SCHEMA_A.USERS)
-- ============================================================

SET SCHEMA TARGET;

CREATE TABLE TARGET.USERS_A (
  ID NUMBER NOT NULL,
  EMAIL VARCHAR2(150),
  USERNAME VARCHAR2(100),
  CONSTRAINT PK_USERS_A PRIMARY KEY (ID),    -- 已自动重命名
  CONSTRAINT UK_EMAIL_A UNIQUE (EMAIL)       -- 已自动重命名
);
```

## 验证和调试

### 检查冲突检测日志

运行程序后，查找以下日志：

```bash
grep "命名冲突" main_reports/report_*.txt
grep "重命名以避免冲突" main_reports/report_*.txt
```

### 检查生成的脚本

```bash
# 查看所有重命名的索引
ls -la fixup_scripts/index/*.sql | grep -v "\.sql$" | head

# 查看所有重命名的约束
ls -la fixup_scripts/constraint/*.sql | grep -v "\.sql$" | head

# 检查脚本内容
cat fixup_scripts/index/TARGET.IDX_EMAIL_A.sql
```

### 手工验证

如果需要手工验证冲突：

```sql
-- 查询目标schema下所有索引名称
SELECT INDEX_NAME, TABLE_NAME, UNIQUENESS
FROM DBA_INDEXES
WHERE OWNER = 'TARGET'
ORDER BY INDEX_NAME;

-- 查询目标schema下所有约束名称
SELECT CONSTRAINT_NAME, TABLE_NAME, CONSTRAINT_TYPE
FROM DBA_CONSTRAINTS
WHERE OWNER = 'TARGET'
ORDER BY CONSTRAINT_NAME;

-- 查找重复的名称
SELECT INDEX_NAME, COUNT(*) 
FROM DBA_INDEXES 
WHERE OWNER = 'TARGET' 
GROUP BY INDEX_NAME 
HAVING COUNT(*) > 1;
```

## 最佳实践

### 1. 提前规划命名规范

在设计remap规则时，考虑使用统一的命名后缀：

```
SCHEMA_A.USERS = TARGET.USERS_A
SCHEMA_B.USERS = TARGET.USERS_B
SCHEMA_C.USERS = TARGET.USERS_C
```

这样自动生成的索引/约束名称会更加规范：
- `IDX_EMAIL_A`, `IDX_EMAIL_B`, `IDX_EMAIL_C`
- `PK_USERS_A`, `PK_USERS_B`, `PK_USERS_C`

### 2. 表重命名时同步重命名约束/索引

如果手工重命名表，建议同时重命名其索引和约束：

```sql
-- 不推荐：只重命名表
ALTER TABLE ORDERS RENAME TO ORDERS_RENAME_20251118;

-- 推荐：同时重命名索引和约束
ALTER TABLE ORDERS RENAME TO ORDERS_RENAME_20251118;
ALTER INDEX IDX_ORDER_DATE RENAME TO IDX_ORDER_DATE_20251118;
ALTER TABLE ORDERS_RENAME_20251118 
  RENAME CONSTRAINT PK_ORDERS TO PK_ORDERS_20251118;
```

### 3. 审核生成的脚本

虽然程序会自动处理冲突，但建议在执行前审核：

```bash
# 查看所有重命名的对象
grep "已重命名避免冲突" fixup_scripts/*/*.sql

# 检查是否有异常长的名称（接近30字符限制）
find fixup_scripts -name "*.sql" -exec basename {} \; | awk '{print length, $0}' | sort -rn | head -20
```

### 4. 分批执行脚本

对于大量冲突的场景，建议分批执行：

```bash
# 先执行表创建
python run_fixup.py --only-dirs table

# 再执行索引创建
python run_fixup.py --only-dirs index

# 最后执行约束创建
python run_fixup.py --only-dirs constraint
```

## 故障排查

### 问题1：仍然报名称冲突

**可能原因**：
- 目标端有程序未检测到的对象
- 手工创建了部分对象

**解决方案**：
```sql
-- 查询实际冲突的对象
SELECT 'INDEX' AS TYPE, INDEX_NAME AS NAME, TABLE_NAME 
FROM DBA_INDEXES 
WHERE OWNER = 'TARGET' AND INDEX_NAME = 'IDX_EMAIL'
UNION ALL
SELECT 'CONSTRAINT', CONSTRAINT_NAME, TABLE_NAME 
FROM DBA_CONSTRAINTS 
WHERE OWNER = 'TARGET' AND CONSTRAINT_NAME = 'PK_USERS';

-- 手工重命名冲突对象
ALTER INDEX TARGET.IDX_EMAIL RENAME TO IDX_EMAIL_OLD;
```

### 问题2：重命名后的名称不符合预期

**可能原因**：
- 表名模式不匹配预期
- 名称过长被截断

**解决方案**：
手工编辑生成的脚本，使用更合适的名称：

```bash
# 编辑脚本
vi fixup_scripts/index/TARGET.IDX_EMAIL_A.sql

# 修改为更合适的名称
CREATE INDEX TARGET.IDX_USERS_A_EMAIL ON TARGET.USERS_A (EMAIL);
```

### 问题3：PACKAGE BODY 报错

**说明**：
PACKAGE 和 PACKAGE BODY 必须同名，程序不会重命名它们。如果出现冲突，需要手工处理：

```sql
-- 方案1：删除旧的PACKAGE
DROP PACKAGE TARGET.PKG_UTILS;

-- 方案2：重命名旧的PACKAGE
-- 注意：Oracle不支持直接重命名PACKAGE，需要重新创建
```

## 技术实现细节

### 冲突检测算法

```python
def detect_naming_conflicts(master_list, oracle_meta, ob_meta):
    # 1. 收集所有源端索引/约束名称
    # 2. 按目标schema分组
    # 3. 检测同一schema下的重复名称
    # 4. 包含目标端已存在的对象
    # 5. 返回冲突列表
```

### 重命名策略

```python
def generate_conflict_free_name(original_name, table_name, obj_type, existing_names):
    # 1. 尝试提取 _RENAME_YYYYMMDD 后缀
    # 2. 否则使用表名后缀
    # 3. 确保不超过30字符
    # 4. 如仍冲突，添加数字后缀
```

### DDL 重写

```python
def rename_embedded_constraints_indexes(ddl, tgt_schema, tgt_table, rename_map):
    # 使用正则表达式替换DDL中的约束/索引名称
    # 支持 CONSTRAINT name 和 INDEX name 模式
```

## 相关文档

- [README.md](README.md) - 主要使用说明
- [REMAP_INFERENCE_GUIDE.md](REMAP_INFERENCE_GUIDE.md) - Schema映射推导指南
- [CHANGELOG.md](CHANGELOG.md) - 版本变更记录
- [AUDIT_REPORT.md](AUDIT_REPORT.md) - 程序一致性审核报告

## 反馈和支持

如果遇到命名冲突相关的问题，请提供：

1. 冲突检测日志（包含 "命名冲突" 的行）
2. 相关的remap规则
3. 目标schema的对象列表（DBA_INDEXES/DBA_CONSTRAINTS查询结果）
4. 生成的脚本内容

这将帮助我们更快地诊断和解决问题。
