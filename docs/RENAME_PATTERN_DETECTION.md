# 表重命名模式检测增强说明

## 问题背景

用户反馈：当表被重命名（如 `ORDERS` → `ORDERS_RENAME_20251118`）后，使用dbcat导出原表DDL重新创建时，会报错：

```
ORA-00955: 名称已由现有对象使用
```

**根本原因**：
- 表名改变了：`ORDERS` → `ORDERS_RENAME_20251118`
- 但索引/约束名称**没有改变**：
  - 索引 `IDX_ORDER_DATE` 仍然叫 `IDX_ORDER_DATE`
  - 约束 `PK_ORDERS` 仍然叫 `PK_ORDERS`
  - 它们现在属于 `ORDERS_RENAME_20251118` 表

当重新创建 `ORDERS` 表时，其DDL中包含同名的索引/约束，导致冲突。

## 解决方案

### 1. 灵活的重命名模式识别

原来只支持 `_RENAME_20251118` 模式，现在支持：

#### 关键词模式（有无下划线）
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

#### 纯日期模式
- `_YYYYMMDD` (如 `ORDERS_20251118`)
- `YYYYMMDD` (如 `ORDERS20251118`，无下划线)

#### 日期格式
- 8位：`YYYYMMDD` (20251118)
- 6位：`YYMMDD` (251118)
- 4位：`YYMM` (2511)

### 2. 智能冲突检测

程序会：

1. **扫描目标端所有表**，包括重命名的表
2. **识别重命名模式**，提取原始表名
   - 例如：`ORDERS_RENAME_20251118` → 原始表名 `ORDERS`
3. **检查是否即将创建同名表**
   - 如果 `ORDERS` 在待创建列表中
4. **标记潜在冲突**
   - 重命名表的索引/约束 vs 即将创建的表的索引/约束

### 3. 自动重命名

对于检测到的冲突，程序会：

1. **提取重命名后缀**
   - `ORDERS_RENAME_20251118` → 后缀 `RENAME_20251118`
2. **应用到索引/约束名称**
   - `IDX_ORDER_DATE` → `IDX_ORDER_DATE_RENAME_20251118`
   - `PK_ORDERS` → `PK_ORDERS_RENAME_20251118`
3. **修改DDL语句**
   - CREATE TABLE 中的内联约束
   - CREATE INDEX 语句
   - ALTER TABLE ADD CONSTRAINT 语句

## 实现细节

### 新增函数：`extract_table_suffix_for_renaming()`

```python
def extract_table_suffix_for_renaming(table_name: str) -> Optional[str]:
    """
    从表名中提取用于重命名的后缀。
    
    示例：
    - ORDERS_RENAME_20251118 → "RENAME_20251118"
    - ORDERS_BACKUP20251209 → "BACKUP_20251209"
    - ORDERS_OLD_2511 → "OLD_2511"
    - ORDERS_20251118 → "20251118"
    - ORDERS20251118 → "20251118"
    """
```

### 增强函数：`detect_naming_conflicts()`

新增逻辑：

```python
# 检查这个表是否是"重命名的表"
if extract_table_suffix_for_renaming(tgt_table_u):
    # 提取原始表名
    base_table_name = extract_base_table_name(tgt_table_u)
    
    # 检查原始表名是否在即将创建的表中
    if base_table_name in tables_to_create:
        is_renamed_table = True
        log.info(
            "[冲突检测] 发现重命名表 %s.%s，其索引/约束可能与即将创建的表 %s.%s 冲突",
            tgt_schema_u, tgt_table_u, tgt_schema_u, base_table_name
        )
```

### 增强函数：`generate_conflict_free_name()`

新增逻辑：

```python
# 尝试提取重命名后缀
suffix = extract_table_suffix_for_renaming(table_u)

if suffix:
    # 使用提取的后缀
    new_name = f"{original_u}_{suffix}"
else:
    # 回退到使用表名后缀
    new_name = f"{original_u}_{table_suffix}"
```

## 示例场景

### 场景1：标准重命名模式

```
目标端现状：
  TARGET.ORDERS_RENAME_20251118
  - 索引: IDX_ORDER_DATE
  - 约束: PK_ORDERS

即将创建：
  TARGET.ORDERS (从源端导出)
  - 索引: IDX_ORDER_DATE (冲突！)
  - 约束: PK_ORDERS (冲突！)

程序处理：
  ✓ 识别 ORDERS_RENAME_20251118 是 ORDERS 的重命名版本
  ✓ 提取后缀: RENAME_20251118
  ✓ 重命名新表的对象:
    - IDX_ORDER_DATE → IDX_ORDER_DATE_RENAME_20251118
    - PK_ORDERS → PK_ORDERS_RENAME_20251118
```

### 场景2：无下划线的重命名

```
目标端现状：
  TARGET.USERS_BACKUP20251209
  - 索引: IDX_EMAIL
  - 约束: UK_USERNAME

即将创建：
  TARGET.USERS
  - 索引: IDX_EMAIL (冲突！)
  - 约束: UK_USERNAME (冲突！)

程序处理：
  ✓ 识别 USERS_BACKUP20251209 是 USERS 的重命名版本
  ✓ 提取后缀: BACKUP_20251209
  ✓ 重命名新表的对象:
    - IDX_EMAIL → IDX_EMAIL_BACKUP_20251209
    - UK_USERNAME → UK_USERNAME_BACKUP_20251209
```

### 场景3：纯日期后缀

```
目标端现状：
  TARGET.PRODUCTS_20251118
  - 索引: IDX_CATEGORY
  - 约束: PK_PRODUCTS

即将创建：
  TARGET.PRODUCTS
  - 索引: IDX_CATEGORY (冲突！)
  - 约束: PK_PRODUCTS (冲突！)

程序处理：
  ✓ 识别 PRODUCTS_20251118 是 PRODUCTS 的重命名版本
  ✓ 提取后缀: 20251118
  ✓ 重命名新表的对象:
    - IDX_CATEGORY → IDX_CATEGORY_20251118
    - PK_PRODUCTS → PK_PRODUCTS_20251118
```

## 日志输出

### 冲突检测阶段

```
[FIXUP] 检测索引和约束的命名冲突...
[冲突检测] 发现重命名表 TARGET.ORDERS_RENAME_20251118，其索引/约束可能与即将创建的表 TARGET.ORDERS 冲突
[冲突检测] 发现重命名表 TARGET.USERS_BACKUP20251209，其索引/约束可能与即将创建的表 TARGET.USERS 冲突
[FIXUP] 检测到INDEX名称冲突: TARGET.IDX_ORDER_DATE 在表 ORDERS 上，重命名为 IDX_ORDER_DATE_RENAME_20251118
[FIXUP] 检测到INDEX名称冲突: TARGET.IDX_ORDER_DATE 在表 ORDERS_RENAME_20251118(已存在,来自重命名表) 上
[FIXUP] 检测到CONSTRAINT名称冲突: TARGET.PK_ORDERS 在表 ORDERS 上，重命名为 PK_ORDERS_RENAME_20251118
[FIXUP] 检测到CONSTRAINT名称冲突: TARGET.PK_ORDERS 在表 ORDERS_RENAME_20251118(已存在,来自重命名表) 上
[FIXUP] 命名冲突检测完成: INDEX=2, CONSTRAINT=2 个对象将被重命名
```

### 脚本生成阶段

```
[FIXUP] (5/9) 正在生成 INDEX 脚本...
[FIXUP] 索引重命名以避免冲突: TARGET.IDX_ORDER_DATE -> TARGET.IDX_ORDER_DATE_RENAME_20251118 (表: TARGET.ORDERS)
[FIXUP][DBCAT] 写入 INDEX 脚本: TARGET.IDX_ORDER_DATE_RENAME_20251118.sql

[FIXUP] (6/9) 正在生成 CONSTRAINT 脚本...
[FIXUP] 约束重命名以避免冲突: TARGET.PK_ORDERS -> TARGET.PK_ORDERS_RENAME_20251118 (表: TARGET.ORDERS)
[FIXUP][META] 写入 CONSTRAINT 脚本: TARGET.PK_ORDERS_RENAME_20251118.sql
```

## 生成的脚本

### 索引脚本

```sql
-- ============================================================
-- 修补缺失的 INDEX IDX_ORDER_DATE_RENAME_20251118 (表: TARGET.ORDERS)
-- [原名: IDX_ORDER_DATE, 已重命名避免冲突]
-- ============================================================

SET SCHEMA TARGET;

CREATE INDEX TARGET.IDX_ORDER_DATE_RENAME_20251118 
  ON TARGET.ORDERS (ORDER_DATE);
```

### 约束脚本

```sql
-- ============================================================
-- 修补缺失的约束 PK_ORDERS_RENAME_20251118 (表: TARGET.ORDERS)
-- [原名: PK_ORDERS, 已重命名避免冲突]
-- ============================================================

SET SCHEMA TARGET;

ALTER TABLE TARGET.ORDERS 
  ADD CONSTRAINT PK_ORDERS_RENAME_20251118 PRIMARY KEY (ORDER_ID);
```

### 表脚本（内联约束已重命名）

```sql
-- ============================================================
-- 修补缺失的 TABLE TARGET.ORDERS (源: SOURCE.ORDERS)
-- ============================================================

SET SCHEMA TARGET;

CREATE TABLE TARGET.ORDERS (
  ORDER_ID NUMBER NOT NULL,
  ORDER_DATE DATE,
  CUSTOMER_ID NUMBER,
  CONSTRAINT PK_ORDERS_RENAME_20251118 PRIMARY KEY (ORDER_ID),  -- 已自动重命名
  CONSTRAINT FK_CUSTOMER_RENAME_20251118 FOREIGN KEY (CUSTOMER_ID) 
    REFERENCES TARGET.CUSTOMERS (CUSTOMER_ID)  -- 已自动重命名
);
```

## 测试验证

### 手工验证步骤

1. **查询目标端的重命名表**：
```sql
SELECT TABLE_NAME 
FROM DBA_TABLES 
WHERE OWNER = 'TARGET' 
  AND (TABLE_NAME LIKE '%_RENAME_%' 
    OR TABLE_NAME LIKE '%_BACKUP%'
    OR TABLE_NAME LIKE '%_OLD_%'
    OR TABLE_NAME LIKE '%_BAK%'
    OR TABLE_NAME LIKE '%_HIST%'
    OR TABLE_NAME LIKE '%_ARCHIVE%'
    OR TABLE_NAME LIKE '%_TMP%'
    OR TABLE_NAME LIKE '%_TEMP%');
```

2. **查询重命名表的索引/约束**：
```sql
-- 索引
SELECT INDEX_NAME, TABLE_NAME 
FROM DBA_INDEXES 
WHERE OWNER = 'TARGET' 
  AND TABLE_NAME = 'ORDERS_RENAME_20251118';

-- 约束
SELECT CONSTRAINT_NAME, TABLE_NAME, CONSTRAINT_TYPE 
FROM DBA_CONSTRAINTS 
WHERE OWNER = 'TARGET' 
  AND TABLE_NAME = 'ORDERS_RENAME_20251118';
```

3. **检查是否有同名冲突**：
```sql
-- 查找可能冲突的索引
SELECT INDEX_NAME, COUNT(*) AS CNT
FROM DBA_INDEXES
WHERE OWNER = 'TARGET'
  AND INDEX_NAME IN (
    SELECT INDEX_NAME FROM DBA_INDEXES 
    WHERE OWNER = 'TARGET' AND TABLE_NAME = 'ORDERS_RENAME_20251118'
  )
GROUP BY INDEX_NAME
HAVING COUNT(*) > 1;
```

### 自动化测试

运行程序后检查日志：

```bash
# 查找重命名表检测日志
grep "发现重命名表" main_reports/report_*.txt

# 查找冲突检测日志
grep "来自重命名表" main_reports/report_*.txt

# 查看生成的脚本
ls -la fixup_scripts/index/*.sql
ls -la fixup_scripts/constraint/*.sql
```

## 兼容性

### 向后兼容
- 如果没有重命名表，行为与之前版本完全一致
- 不影响现有的remap规则和配置
- 不需要修改用户工作流程

### 数据库兼容性
- Oracle 11g+
- OceanBase 3.x/4.x (Oracle模式)
- 遵守30字符对象名称限制

## 限制和注意事项

### 已知限制

1. **复杂的重命名模式**：
   - 如果表名不符合识别的模式，可能无法自动检测
   - 例如：`ORDERS_V2`, `ORDERS_COPY` 等

2. **30字符限制**：
   - 如果原始名称+后缀超过30字符，会被截断
   - 可能需要人工审核

3. **多次重命名**：
   - 如果表被多次重命名（如 `ORDERS_RENAME_20251118_BACKUP_20251209`）
   - 只会提取最后一个匹配的模式

### 建议

1. **统一重命名规范**：
   - 建议使用标准的重命名模式（如 `_RENAME_YYYYMMDD`）
   - 避免使用非标准的命名方式

2. **同步重命名索引/约束**：
   - 在重命名表时，同时重命名其索引和约束
   - 避免依赖程序的自动检测

3. **审核生成的脚本**：
   - 在执行前检查重命名是否合理
   - 特别注意接近30字符限制的名称

## 相关文档

- [NAMING_CONFLICT_GUIDE.md](NAMING_CONFLICT_GUIDE.md) - 完整的命名冲突指南
- [CHANGELOG.md](CHANGELOG.md) - 版本变更记录
- [README.md](README.md) - 主要使用说明

## 总结

v0.8.2 版本显著增强了表重命名场景的处理能力：

✓ **灵活的模式识别**：支持10+种重命名关键词和多种日期格式
✓ **智能冲突检测**：自动识别重命名表与即将创建的表的冲突
✓ **自动重命名**：为冲突的索引/约束生成有意义的新名称
✓ **详细日志**：清晰标注冲突来源和处理结果
✓ **向后兼容**：不影响现有功能和工作流程

这个增强解决了用户反馈的核心问题，使得表重命名场景下的迁移更加顺畅。
