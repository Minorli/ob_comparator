# VARCHAR/VARCHAR2 长度校验规则说明

## 概述

程序现在能够区分 VARCHAR/VARCHAR2 列的 BYTE 和 CHAR 语义，并应用不同的校验规则。

## 校验规则

### BYTE 语义（默认）
- **识别**：`CHAR_USED` 字段为空或不等于 'C'
- **示例**：`VARCHAR2(386) BYTE` 或 `VARCHAR2(386)`（未显式指定时默认为BYTE）
- **校验规则**：目标端长度需在 `[ceil(源长度 * 1.5), ceil(源长度 * 2.5)]` 区间
- **修补策略**：
  - 长度不足：放大到 `ceil(源长度 * 1.5)`
  - 长度过大：仅警告，需人工评估
- **重要**：生成的 DDL 不会包含 BYTE 关键字（OceanBase 默认就是 BYTE 语义）

### CHAR 语义
- **识别**：`CHAR_USED` 字段为 'C'
- **示例**：`VARCHAR2(100) CHAR`
- **校验规则**：目标端长度必须与源端完全一致
- **修补策略**：生成 ALTER 语句将目标端长度修改为源端长度

## 实现细节

### 1. 列长度校验（第2883-2921行）
```python
src_char_used = (src_info.get("char_used") or "").strip().upper()

if src_char_used == 'C':
    # CHAR语义：要求长度完全一致
    if tgt_len_int != src_len_int:
        length_mismatches.append(
            ColumnLengthIssue(col_name, src_len_int, tgt_len_int, src_len_int, 'char_mismatch')
        )
else:
    # BYTE语义：需要放大1.5倍
    expected_min_len = int(math.ceil(src_len_int * VARCHAR_LEN_MIN_MULTIPLIER))
    ...
```

### 2. DDL 放大逻辑（第5243-5267行）
```python
# 只对BYTE语义的列进行放大，CHAR语义保持原样
char_used = (info.get("char_used") or "").strip().upper()
if char_used == 'C':
    continue  # 跳过CHAR语义的列
```

### 3. ALTER 脚本生成（第5317-5346行）
```python
if dtype in ("VARCHAR2", "VARCHAR"):
    char_used = (info.get("char_used") or "").strip().upper()
    if char_used != 'C':  # 只对BYTE语义放大
        override_len = int(math.ceil(src_len_int * VARCHAR_LEN_MIN_MULTIPLIER))
```

### 4. 报告输出（第6880-6900行）
```python
if issue_type == 'char_mismatch':
    details.append(f"    - {col}: 源={src_len} CHAR, 目标={tgt_len}, 要求一致\n")
elif issue_type == 'short':
    details.append(f"    - {col}: 源={src_len} BYTE, 目标={tgt_len}, 期望下限={limit_len}\n")
```

## 影响范围

1. **主对象校验**：区分 BYTE/CHAR 语义进行长度校验
2. **修补脚本生成**：
   - `table/` 目录：CREATE TABLE 时只对 BYTE 语义列放大
   - `table_alter/` 目录：ALTER TABLE 时根据语义生成不同的 MODIFY 语句
3. **报告输出**：明确标注列的语义类型（BYTE 或 CHAR）

## 示例

### BYTE 语义列
```sql
-- 源端
CREATE TABLE test (col1 VARCHAR2(100) BYTE);

-- 目标端期望
col1 VARCHAR(150)  -- 至少 ceil(100*1.5)=150，无 BYTE 关键字

-- 修补脚本（注意：不包含 BYTE 关键字）
ALTER TABLE schema.test MODIFY (col1 VARCHAR(150)); 
-- BYTE语义，源长度: 100, 目标长度: 80, 期望下限: 150
```

### CHAR 语义列
```sql
-- 源端
CREATE TABLE test (col2 VARCHAR2(50) CHAR);

-- 目标端期望
col2 VARCHAR(50) CHAR  -- 必须完全一致，保留 CHAR 关键字

-- 修补脚本
ALTER TABLE schema.test MODIFY (col2 VARCHAR(50) CHAR); 
-- CHAR语义，源长度: 50, 目标长度: 40, 要求一致
```

## 注意事项

1. Oracle 默认的字符串长度语义由 `NLS_LENGTH_SEMANTICS` 参数控制（默认为 BYTE）
2. 程序通过 `DBA_TAB_COLUMNS.CHAR_USED` 字段判断实际语义
3. CHAR 语义通常用于多字节字符集环境，确保字符数量而非字节数量
4. BYTE 语义需要放大是因为字符集转换可能导致字节数增加
5. **重要**：生成的 DDL 中，BYTE 语义不会显式包含 BYTE 关键字，因为 OceanBase 默认就是 BYTE 语义；只有 CHAR 语义才会显式添加 CHAR 关键字
