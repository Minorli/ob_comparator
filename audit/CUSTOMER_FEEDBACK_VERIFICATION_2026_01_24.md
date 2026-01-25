# 客户反馈问题验证报告

**验证日期**: 2026-01-24  
**代码版本**: commit 8f6ac30 (最新)  
**验证人**: Cascade AI  
**复核人**: _______________  

---

## 1. 背景

客户（杨云风）在使用 ob_comparator 程序后反馈了以下问题：

1. NUMBER 字段类型误判：`NUMBER(*,0)` 与 `NUMBER(38,0)` 应等价但被误报
2. 索引/约束误报缺失：实际存在的 UNIQUE INDEX/CONSTRAINT 被报告为缺失
3. NOT NULL 约束大量误报：可能与大小写敏感有关

---

## 2. 问题 1：NUMBER(*,0) vs NUMBER(38,0) 等价性

### 2.1 客户描述

> 源端是 `"PROC_COUNT" NUMBER(*,0)`，`*` 表示最大精度 38，跟 OB 的 `"PROC_COUNT" NUMBER(38,0)` 是等价的，但是程序检测之后提示要改成 `NUMBER`。

### 2.2 代码验证

**关键函数**: `normalize_number_signature()` (第 1272-1282 行)

```python
def normalize_number_signature(
    src_prec: Optional[int],
    src_scale: Optional[int],
    *,
    star_precision: int = NUMBER_STAR_PRECISION  # 38
) -> Tuple[Optional[int], Optional[int], bool]:
    if src_prec is None and src_scale is None:
        return None, None, True  # unbounded NUMBER
    if src_prec is None:
        return star_precision, src_scale, False  # NUMBER(*,scale) -> NUMBER(38,scale)
    return src_prec, src_scale, False
```

**比较函数**: `is_number_equivalent()` (第 1285-1301 行)

```python
def is_number_equivalent(src_prec, src_scale, tgt_prec, tgt_scale) -> bool:
    src_prec_n, src_scale_n, src_unbounded = normalize_number_signature(src_prec, src_scale)
    tgt_prec_n, tgt_scale_n, tgt_unbounded = normalize_number_signature(tgt_prec, tgt_scale)
    # ... 比较逻辑
    return tgt_prec_n >= src_prec_n
```

**逻辑验证**:

| 场景 | 源端 (Oracle) | 目标端 (OB) | 标准化后 | 结果 |
|------|--------------|-------------|----------|------|
| NUMBER(*,0) vs NUMBER(38,0) | prec=None, scale=0 | prec=38, scale=0 | (38,0) vs (38,0) | ✅ 等价 |
| NUMBER(*,0) vs NUMBER(37,0) | prec=None, scale=0 | prec=37, scale=0 | (38,0) vs (37,0) | ❌ 不等价 |

### 2.3 单元测试

**测试用例**: `test_check_primary_objects_number_star_zero_equivalence` (第 404-435 行)

```python
def test_check_primary_objects_number_star_zero_equivalence(self):
    # Oracle: NUMBER(*,0) -> prec=None, scale=0
    oracle_meta = {"C1": {"data_precision": None, "data_scale": 0}}
    # OB: NUMBER(38,0) -> prec=38, scale=0
    ob_meta = {"C1": {"data_precision": 38, "data_scale": 0}}
    # 预期：无 mismatch
    self.assertEqual(len(results["mismatched"]), 0)
    self.assertEqual(len(results["ok"]), 1)
```

**测试执行结果**: ✅ PASS

### 2.4 结论

| 项目 | 状态 |
|------|------|
| 代码逻辑 | ✅ 正确处理 NUMBER(*,0) = NUMBER(38,0) |
| 单元测试 | ✅ 通过 |
| 修复状态 | ✅ 已修复 |

**⚠️ 注意**: 如客户仍遇到问题，可能是以下原因：
- Oracle 元数据返回 `DATA_PRECISION=0` 而非 `NULL`（不同 Oracle 版本行为可能不同）
- 需要客户提供具体的 `DBA_TAB_COLUMNS` 查询结果确认

---

## 3. 问题 2：UNIQUE INDEX 与 UNIQUE CONSTRAINT 误判

### 3.1 客户描述

> 程序生成的 DDL:
> ```sql
> CREATE UNIQUE INDEX "FINDATA"."IX_LAS_RL_TRAN_BUSI_UNIQUE_ID" ON "FINDATA"."LAS_REAL_TIME_BANK_TRAN" (DECODE(...));
> ```
> 实际 `show create table` 看表结构里的约束是:
> ```sql
> CONSTRAINT "IX_LAS_RL_TRAN_BUSI_UNIQUE_ID" UNIQUE (decode(...))
> ```
> 是存在的。

### 3.2 代码验证

**关键逻辑**: 索引比对时会检查目标端约束列集

```python
# schema_diff_reconciler.py:10720-10725
constraint_index_cols: Set[Tuple[str, ...]] = {
    normalize_column_sequence(cons.get("columns"))
    for cons in tgt_constraints.values()
    if (cons.get("type") or "").upper() in ("P", "U")
}
```

```python
# schema_diff_reconciler.py:10943-10946
for cols in missing_cols:
    if cols in constraint_index_cols:
        continue  # 跳过已被约束覆盖的索引
    filtered_missing_cols.add(cols)
```

**函数表达式标准化**: `normalize_index_columns()` (第 2021-2039 行)

```python
def normalize_index_columns(columns, expr_map=None):
    for idx, col in enumerate(columns, start=1):
        expr = expr_map.get(idx)
        # 函数表达式使用大小写标准化
        token = normalize_sql_expression_casefold(expr) if expr else col.upper()
```

### 3.3 单元测试

**测试用例**: `test_normalize_index_expression_casefold` (第 3808-3814 行)

```python
def test_normalize_index_expression_casefold(self):
    expr_upper = 'DECODE("CMS_RESULT",\'PBB00\',"BUSINESS_UNIQUE_ID",NULL,"BUSINESS_UNIQUE_ID")'
    expr_lower = 'decode("CMS_RESULT",\'PBB00\',"BUSINESS_UNIQUE_ID",null,"BUSINESS_UNIQUE_ID")'
    norm_upper = sdr.normalize_index_columns(cols, {1: expr_upper})
    norm_lower = sdr.normalize_index_columns(cols, {1: expr_lower})
    self.assertEqual(norm_upper, norm_lower)
```

**测试执行结果**: ✅ PASS

### 3.4 目标端元数据验证

**验证查询**:
```sql
SELECT 1
FROM DBA_TAB_COLUMNS
WHERE OWNER = 'SYS'
  AND TABLE_NAME = 'DBA_CONSTRAINTS'
  AND COLUMN_NAME = 'INDEX_NAME'
  AND ROWNUM = 1;
```

**验证结果**: 返回 `1`，说明目标端 `DBA_CONSTRAINTS` 提供 `INDEX_NAME` 字段，可用于约束→索引表达式关联。

### 3.5 潜在问题分析

| 场景 | 问题 | 状态 |
|------|------|------|
| 索引函数表达式大小写 | 已通过 casefold 标准化 | ✅ 已修复 |
| 约束列与索引列匹配 | `constraint_index_cols` 使用 `normalize_column_sequence` | ⚠️ 见下文 |

**⚠️ 潜在风险**:

`constraint_index_cols` 使用 `normalize_column_sequence()` 提取约束列，该函数**不处理函数表达式**。
而索引列使用 `normalize_index_columns()` 提取，该函数**处理函数表达式**。

这可能导致：
- 约束: `UNIQUE (decode(...))` → 列集 `('SYS_NC0004$',)` 或类似系统列名
- 索引: `CREATE INDEX ... (DECODE(...))` → 列集 `('DECODE(CMS_RESULT,...)',)`

两者不匹配，导致误报。

### 3.6 结论

| 项目 | 状态 |
|------|------|
| 表达式大小写标准化 | ✅ 已修复 |
| 约束 vs 索引列集匹配 | ⚠️ 可能存在不一致 |
| 建议 | 需要客户提供具体元数据验证 |

---

## 4. 问题 3：NOT NULL 约束大小写敏感

### 4.1 客户描述

> 很多 `is not null` 约束，不知道是不是区分了大小写
> - SQL: `CHECK ("VOUCHER_STATUS" IS NOT NULL)`
> - 数据库查询: `("VOUCHER_STATUS" is not null)`

### 4.2 代码验证

**关键函数**: `normalize_check_constraint_expression()` (第 1186-1196 行)

```python
def normalize_check_constraint_expression(expr, cons_name) -> str:
    expr_norm = normalize_sql_expression_casefold(expr)  # 大小写标准化
    expr_norm = strip_redundant_predicate_parentheses(expr_norm)
    expr_norm = uppercase_outside_single_quotes(normalize_sql_expression(expr_norm))
    # ...
```

**大小写标准化**: `normalize_sql_expression_casefold()` (第 1144-1145 行)

```python
def normalize_sql_expression_casefold(expr):
    return uppercase_outside_single_quotes(normalize_sql_expression(expr))
```

**逻辑验证**:

| 输入 | 标准化后 |
|------|----------|
| `"VOUCHER_STATUS" IS NOT NULL` | `VOUCHER_STATUS IS NOT NULL` |
| `"VOUCHER_STATUS" is not null` | `VOUCHER_STATUS IS NOT NULL` |
| `("VOUCHER_STATUS" is not null)` | `VOUCHER_STATUS IS NOT NULL` |

### 4.3 单元测试

**测试用例**: `test_normalize_check_constraint_expression_casefold` (第 3787-3792 行)

```python
def test_normalize_check_constraint_expression_casefold(self):
    expr_upper = '"VOUCHER_STATUS" IS NOT NULL'
    expr_lower = '"VOUCHER_STATUS" is not null'
    norm_upper = sdr.normalize_check_constraint_expression(expr_upper, "NN1")
    norm_lower = sdr.normalize_check_constraint_expression(expr_lower, "NN1")
    self.assertEqual(norm_upper, norm_lower)  # 相等
```

**测试执行结果**: ✅ PASS

### 4.4 NOT NULL 约束过滤

系统生成的 NOT NULL 约束应被过滤：

```python
# schema_diff_reconciler.py:1148-1158
def is_system_notnull_check(cons_name, search_condition) -> bool:
    if not cons_name.upper().startswith("SYS_"):
        return False
    cond_u = normalize_sql_expression(search_condition).upper()
    return bool(re.match(r"^[A-Z0-9_#$]+\s+IS\s+NOT\s+NULL$", cond_u))
```

**⚠️ 注意**: 只有 `SYS_` 开头的约束名才会被过滤。用户自定义的 NOT NULL 约束（如 `NN_VALUEVOU_REC_VOU_STATUS`）不会被过滤。

### 4.5 结论

| 项目 | 状态 |
|------|------|
| 大小写标准化 | ✅ 已修复 |
| 单元测试 | ✅ 通过 |
| 系统约束过滤 | ✅ 仅过滤 SYS_ 开头 |
| 用户自定义 NOT NULL | ⚠️ 不会被过滤，按正常约束比对 |

---

## 5. 测试执行汇总

```
test_check_primary_objects_number_star_zero_equivalence ... ok
test_normalize_check_constraint_expression_casefold ... ok
test_normalize_index_expression_casefold ... ok
```

**总计**: 3 项关键测试全部通过

---

## 6. 综合结论

| 问题 | 代码修复 | 测试覆盖 | 风险评估 |
|------|----------|----------|----------|
| NUMBER(*,0) vs NUMBER(38,0) | ✅ | ✅ | 低 |
| CHECK 约束大小写 | ✅ | ✅ | 低 |
| 索引函数表达式大小写 | ✅ | ✅ | 低 |
| 约束 vs 索引列集匹配 | ⚠️ | ❌ | 中 |

**补充验证**: 目标端 `DBA_CONSTRAINTS.INDEX_NAME` 可用，可用于修复表达式索引的列集匹配问题。

---

## 7. 建议后续行动

### 7.1 如客户问题仍存在

请客户提供以下信息用于进一步定位：

1. **NUMBER 字段问题**:
   ```sql
   SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE 
   FROM DBA_TAB_COLUMNS 
   WHERE TABLE_NAME = 'RENEWAL_BATCH_PROC_RECORD' AND COLUMN_NAME = 'PROC_COUNT';
   ```

2. **索引/约束问题**:
   ```sql
   -- Oracle 侧
   SELECT INDEX_NAME, COLUMN_NAME, COLUMN_EXPRESSION 
   FROM DBA_IND_COLUMNS JOIN DBA_IND_EXPRESSIONS USING (INDEX_NAME)
   WHERE TABLE_NAME = 'LAS_REAL_TIME_BANK_TRAN';
   
   -- OB 侧
   SHOW CREATE TABLE FINDATA.LAS_REAL_TIME_BANK_TRAN;
   ```

3. **NOT NULL 约束问题**:
   ```sql
   SELECT CONSTRAINT_NAME, SEARCH_CONDITION 
   FROM DBA_CONSTRAINTS 
   WHERE TABLE_NAME = 'VALUE_VOUCHER_RECORD' AND CONSTRAINT_TYPE = 'C';
   ```

### 7.2 代码改进建议

1. **约束列集标准化**: 使用 `INDEX_NAME` 关联索引表达式并通过 `normalize_index_columns()` 生成列集
2. **日志增强**: 在比对逻辑中增加 DEBUG 级别日志，便于问题定位

---

## 8. 复核确认

| 复核项 | 复核人签字 | 日期 |
|--------|-----------|------|
| 代码逻辑正确性 | | |
| 测试覆盖充分性 | | |
| 风险评估合理性 | | |
| 建议可行性 | | |

**复核意见**:

_______________________________________________________________________________

_______________________________________________________________________________

---

*报告生成时间: 2026-01-24 20:54*  
*代码版本: ae8ca5d*
