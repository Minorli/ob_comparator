# 报告生成逻辑审查报告

**审查日期**: 2026-01-27  
**审查范围**: `schema_diff_reconciler.py` 中所有报告文件的生成逻辑  
**触发问题**: 用户反馈索引不支持数显示61个，但 `indexes_unsupported_detail` 文件未生成

---

## 0. 报告文件精确规格

### 0.1 主报告

#### report_{timestamp}.txt
- **说明**: 主校验报告，Rich 格式转纯文本
- **生成条件**: 无条件生成
- **数据来源**: 所有校验结果的汇总展示
- **内容**: 检查汇总表、缺失对象、差异对象、扩展对象校验、依赖分析等全部内容

---

### 0.2 明细报告 (DETAIL)

#### missing_objects_detail_{timestamp}.txt
- **说明**: 缺失对象支持性明细
- **生成条件**: `emit_detail_files=True` (report_detail_mode=split) 且数据非空
- **数据来源**: `support_summary.missing_detail_rows` (来自 classify_missing_objects)
- **字段**:
  | 字段 | 说明 |
  |-----|------|
  | SRC_FULL | 源端对象全名 (SCHEMA.NAME) |
  | TYPE | 对象类型 |
  | TGT_FULL | 目标端对象全名 |
  | STATE | 支持状态 (SUPPORTED/UNSUPPORTED/BLOCKED) |
  | REASON_CODE | 原因代码 |
  | REASON | 原因说明 |
  | DEPENDENCY | 依赖对象 |
  | ACTION | 建议操作 |
  | DETAIL | 详细信息 |
- **包含对象类型**: TABLE, VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, SYNONYM, TYPE, TYPE BODY 等主对象

#### unsupported_objects_detail_{timestamp}.txt
- **说明**: 不支持/阻断对象明细
- **生成条件**: `emit_detail_files=True` 且数据非空
- **数据来源**: `support_summary.unsupported_rows`
- **字段**: 同 missing_objects_detail
- **重要**: **包含因依赖不支持表而被阻断的 INDEX/CONSTRAINT/TRIGGER**
- **包含的 REASON_CODE**:
  - `DEPENDENCY_UNSUPPORTED`: 依赖不支持表
  - `VIEW_COMPAT_*`: VIEW 兼容性问题
  - `DBLINK_*`: DBLINK 相关问题
  - 等

#### indexes_unsupported_detail_{timestamp}.txt
- **说明**: **仅 DESC 列索引**的不支持明细
- **生成条件**: `extra_results["index_unsupported"]` 非空（不受 emit_detail_files 控制）
- **数据来源**: `classify_unsupported_indexes()` 返回的 `IndexUnsupportedDetail` 列表
- **字段**:
  | 字段 | 说明 |
  |-----|------|
  | TABLE | 表全名 |
  | INDEX_NAME | 索引名 |
  | COLUMNS | 索引列 |
  | REASON_CODE | 固定为 INDEX_DESC |
  | REASON | "索引包含 DESC 列，OceanBase 不支持" |
  | OB_ERROR_HINT | ORA-00900 |
- **注意**: **不包含**因依赖不支持表而被阻断的索引（那些在 unsupported_objects_detail）

#### constraints_unsupported_detail_{timestamp}.txt
- **说明**: **仅 DEFERRABLE 等语法不支持**的约束明细
- **生成条件**: `extra_results["constraint_unsupported"]` 非空（不受 emit_detail_files 控制）
- **数据来源**: `classify_unsupported_check_constraints()` 返回的 `ConstraintUnsupportedDetail` 列表
- **字段**:
  | 字段 | 说明 |
  |-----|------|
  | TABLE | 表全名 |
  | CONSTRAINT_NAME | 约束名 |
  | SEARCH_CONDITION | CHECK 约束表达式 |
  | REASON_CODE | DEFERRABLE_CONSTRAINT 等 |
  | REASON | 原因说明 |
  | OB_ERROR_HINT | OB 错误提示 |
- **注意**: **不包含**因依赖不支持表而被阻断的约束（那些在 unsupported_objects_detail）

#### extra_targets_detail_{timestamp}.txt
- **说明**: 目标端多余对象明细
- **生成条件**: `emit_detail_files=True` 且数据非空
- **数据来源**: `tv_results["extra_targets"]` (来自 check_primary_objects)
- **字段**:
  | 字段 | 说明 |
  |-----|------|
  | TYPE | 对象类型 |
  | TARGET_FULL | 目标端对象全名 |
- **包含对象类型**: **仅 PRIMARY_OBJECT_TYPES** (TABLE, VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, SYNONYM, JOB, SCHEDULE, TYPE, TYPE BODY)
- **注意**: **不包含 SEQUENCE/INDEX/CONSTRAINT/TRIGGER**（它们的多余在 extra_mismatch_detail）

#### skipped_objects_detail_{timestamp}.txt
- **说明**: 仅打印未校验对象明细
- **生成条件**: `emit_detail_files=True` 且数据非空
- **数据来源**: `tv_results["skipped"]`
- **字段**: TYPE | SRC_FULL | TGT_FULL | REASON
- **适用**: MATERIALIZED VIEW 等配置为 print_only 的类型

#### mismatched_tables_detail_{timestamp}.txt
- **说明**: 表列不匹配明细
- **生成条件**: `emit_detail_files=True` 且数据非空
- **数据来源**: `tv_results["mismatched"]` 中 TYPE=TABLE 的项
- **字段**:
  | 字段 | 说明 |
  |-----|------|
  | TABLE | 表全名 |
  | MISSING_COLS | 缺失列（逗号分隔） |
  | EXTRA_COLS | 多余列（逗号分隔） |
  | LENGTH_MISMATCHES | 长度差异 (COL:SRC_LEN->TGT_LEN) |
  | TYPE_MISMATCHES | 类型差异 (COL:SRC_TYPE->TGT_TYPE) |

#### column_order_mismatch_detail_{timestamp}.txt
- **说明**: 列顺序差异明细
- **生成条件**: `emit_detail_files=True` 且数据非空
- **数据来源**: `tv_results["column_order_mismatched"]`
- **字段**: TABLE | SRC_ORDER | TGT_ORDER

#### comment_mismatch_detail_{timestamp}.txt
- **说明**: 注释差异明细
- **生成条件**: `emit_detail_files=True` 且数据非空
- **数据来源**: `comment_results["mismatched"]`
- **字段**: TABLE | TABLE_COMMENT_DIFF | MISSING_COLS | EXTRA_COLS | COLUMN_COMMENT_DIFFS

#### extra_mismatch_detail_{timestamp}.txt
- **说明**: 扩展对象（INDEX/CONSTRAINT/SEQUENCE/TRIGGER）差异明细
- **生成条件**: `emit_detail_files=True` 且数据非空
- **数据来源**: `extra_results` 中的 index_mismatched, constraint_mismatched, sequence_mismatched, trigger_mismatched
- **字段**:
  | 字段 | 说明 |
  |-----|------|
  | TYPE | INDEX/CONSTRAINT/SEQUENCE/TRIGGER |
  | OBJECT | 表全名 或 SCHEMA映射 |
  | MISSING | 缺失项（逗号分隔） |
  | EXTRA | **多余项（逗号分隔）** |
  | DETAIL | 详细信息 |
- **重要**: **SEQUENCE 的多余数据在此报告**，不在 extra_targets_detail

#### dependency_detail_{timestamp}.txt
- **说明**: 依赖关系差异明细
- **生成条件**: `emit_detail_files=True` 且数据非空
- **数据来源**: `dependency_report` 中的 missing, unexpected, skipped
- **字段**: CATEGORY | DEPENDENT | DEPENDENT_TYPE | REFERENCED | REFERENCED_TYPE | REASON
- **CATEGORY 值**: MISSING（缺失依赖）, EXTRA（多余依赖）, SKIPPED（跳过）

#### noise_suppressed_detail_{timestamp}.txt
- **说明**: 降噪明细（被自动过滤的系统生成对象）
- **生成条件**: `emit_detail_files=True` 且数据非空
- **数据来源**: `noise_suppressed_details`
- **字段**: TYPE | SCOPE | REASON | IDENTIFIERS | DETAIL

#### package_compare_{timestamp}.txt
- **说明**: PACKAGE/PACKAGE BODY 对比明细
- **生成条件**: PACKAGE 或 PACKAGE BODY 在校验范围且有结果
- **数据来源**: `package_results["rows"]`
- **字段**: SRC_FULL | TYPE | SRC_STATUS | TGT_FULL | TGT_STATUS | RESULT | ERROR_COUNT | FIRST_ERROR

---

### 0.3 辅助报告 (AUX)

#### report_index_{timestamp}.txt
- **说明**: 报告索引，列出所有生成的报告文件
- **生成条件**: 无条件生成
- **字段**: CATEGORY | PATH | ROWS | DESCRIPTION

#### object_mapping_{timestamp}.txt
- **说明**: 全量对象映射（源端->目标端）
- **生成条件**: fixup 生成时
- **格式**: `SRC_FULL<TAB>OBJECT_TYPE<TAB>TGT_FULL`（每行一个映射）

#### remap_conflicts_{timestamp}.txt
- **说明**: 无法自动推导的对象（需手动配置 remap_rules.txt）
- **生成条件**: 存在冲突时
- **格式**: `SRC_FULL<TAB>OBJECT_TYPE<TAB>REASON`

#### dependency_chains_{timestamp}.txt
- **说明**: 对象依赖链（下探到 TABLE/MVIEW）
- **生成条件**: 存在依赖关系时
- **内容**: 
  - [SOURCE - ORACLE] 源端依赖链
  - [TARGET - REMAPPED] 目标端依赖链
  - 依赖环检测

#### VIEWs_chain_{timestamp}.txt
- **说明**: VIEW 依赖链（用于 fixup 执行排序）
- **生成条件**: 存在 VIEW 缺失时
- **内容**: VIEW 的拓扑排序，确保依赖先创建

#### blacklist_tables.txt
- **说明**: 黑名单表清单
- **生成条件**: 存在黑名单配置时
- **字段**: TABLE_FULL | BLACK_TYPE | DATA_TYPE | STATUS | DETAIL | REASON
- **特殊功能**: LONG/LONG RAW 会校验目标端是否已转换为 CLOB/BLOB

#### trigger_status_report.txt
- **说明**: 触发器状态/清单报告
- **生成条件**: 使用 trigger_list 或存在触发器状态差异时
- **内容**:
  - [Section] trigger_list 清单筛选: ENTRY | STATUS | DETAIL
  - [Section] 触发器状态差异: TRIGGER | SRC_EVENT | TGT_EVENT | SRC_ENABLED | TGT_ENABLED | SRC_VALID | TGT_VALID | DETAIL

#### filtered_grants.txt
- **说明**: 过滤掉的不兼容 GRANT 权限
- **生成条件**: 存在过滤权限时
- **字段**: CATEGORY | GRANTEE | PRIVILEGE | OBJECT | REASON

#### fixup_skip_summary_{timestamp}.txt
- **说明**: Fixup 跳过原因汇总
- **生成条件**: fixup 生成时
- **内容**: 按对象类型分组，显示 missing_total, task_total, generated, 以及各 skip 原因的数量

#### ddl_format_report_{timestamp}.txt
- **说明**: DDL 格式化报告（SQLcl 格式化统计）
- **生成条件**: `ddl_format_enable=true`
- **内容**: 各对象类型的 formatted/skipped/failed 数量

#### ddl_punct_clean_{timestamp}.txt
- **说明**: 全角标点清洗报告
- **生成条件**: 存在全角标点替换时
- **字段**: TYPE | OBJECT | REPLACED | SAMPLES

#### ddl_hint_clean_{timestamp}.txt
- **说明**: DDL hint 清洗报告
- **生成条件**: 存在 hint 处理时
- **字段**: TYPE | OBJECT | POLICY | TOKENS | KEPT | REMOVED | UNKNOWN | KEPT_SAMPLES | REMOVED_SAMPLES | UNKNOWN_SAMPLES

---

### 0.4 OMS 迁移辅助

#### missed_tables_views_for_OMS/{SCHEMA}_T.txt
- **说明**: 缺失的 TABLE 列表（供 OMS 消费）
- **生成条件**: 存在缺失 TABLE 且非黑名单/非阻断
- **格式**: 每行一个表名

#### missed_tables_views_for_OMS/{SCHEMA}_V.txt
- **说明**: 缺失的 VIEW 列表
- **生成条件**: 存在缺失 VIEW 且非阻断
- **格式**: 每行一个视图名

---

### 0.5 run_fixup.py 执行报告

#### fixup_scripts/errors/fixup_errors_{timestamp}.txt
- **说明**: Fixup 执行错误报告
- **生成条件**: 执行 fixup 时有错误
- **字段**: FILE | STMT_INDEX | ERROR_CODE | OBJECT | MESSAGE

---

## 1. 问题根因分析

### 1.1 indexes_unsupported_detail 未生成的原因

**问题现象**: 检查汇总显示 INDEX 不支持/阻断 = 61，但 `indexes_unsupported_detail_{timestamp}.txt` 文件未生成。

**根本原因**: **汇总数与明细数据来源不一致**

汇总表 "不支持/阻断" 列的数据来自 `build_unsupported_summary_counts()` 函数：

```python
# schema_diff_reconciler.py:22256-22273
def build_unsupported_summary_counts(...) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    if support_summary:
        # 来源1: 主对象的 unsupported/blocked
        for obj_type, data in (support_summary.missing_support_counts or {}).items():
            counts[obj_type] += int(data.get("unsupported", 0) or 0)
            counts[obj_type] += int(data.get("blocked", 0) or 0)
        # 来源2: extra_blocked_counts（扩展对象因依赖不支持表而被阻断）
        for obj_type, blocked in (support_summary.extra_blocked_counts or {}).items():
            counts[obj_type] += int(blocked or 0)  # ← 这里统计了61个
    if extra_results:
        # 来源3: index_unsupported（仅DESC索引）
        counts["INDEX"] += len(extra_results.get("index_unsupported", []) or [])
```

而 `indexes_unsupported_detail` 文件仅导出 **来源3**：

```python
# schema_diff_reconciler.py:23594-23598
index_unsupported_path = export_indexes_unsupported_detail(
    extra_results.get("index_unsupported", []) or [],  # ← 仅包含 DESC 索引
    report_path.parent,
    report_ts
)
```

**数据来源对比**:

| 来源 | 统计位置 | 内容 | 输出到哪个报告 |
|-----|---------|------|--------------|
| `extra_blocked_counts["INDEX"]` | classify_missing_objects:4379 | 依赖不支持表的索引 | `unsupported_objects_detail` |
| `extra_results["index_unsupported"]` | classify_unsupported_indexes | DESC 索引 | `indexes_unsupported_detail` |

**结论**: 用户看到的61个不支持索引来自 `extra_blocked_counts`（因依赖表不支持），这些数据输出到 `unsupported_objects_detail` 而非 `indexes_unsupported_detail`。只有包含 DESC 列的索引才会出现在 `indexes_unsupported_detail`。

---

## 2. 报告生成条件审查

### 2.1 report_detail_mode 对报告生成的影响

| 模式 | 主报告内容 | 明细文件生成 |
|-----|----------|------------|
| `full` | 包含完整明细 | 不生成 |
| `split` | 仅概要 | 生成所有明细文件 |
| `summary` | 仅概要 | 不生成 |

**潜在问题**: 用户如果设置了 `report_detail_mode=full` 或 `summary`，所有明细文件都不会生成。

### 2.2 各明细报告生成条件

#### 2.2.1 无条件生成（不受 emit_detail_files 控制）

| 报告 | 生成条件 | 代码位置 |
|-----|---------|---------|
| `indexes_unsupported_detail` | `extra_results["index_unsupported"]` 非空 | 23594-23598 |
| `constraints_unsupported_detail` | `extra_results["constraint_unsupported"]` 非空 | 23605-23609 |

**问题**: 这两个报告不受 `emit_detail_files` 控制，但数据来源可能为空（如本案例）。

#### 2.2.2 需要 emit_detail_files=True（report_detail_mode=split）

| 报告 | 额外条件 | 代码位置 |
|-----|---------|---------|
| `missing_objects_detail` | `missing_detail_rows` 非空 | 23572-23576 |
| `unsupported_objects_detail` | `unsupported_rows` 非空 | 23577-23581 |
| `extra_targets_detail` | `tv_results["extra_targets"]` 非空 | 23625-23629 |
| `skipped_objects_detail` | `tv_results["skipped"]` 非空 | 23630-23634 |
| `mismatched_tables_detail` | `tv_results["mismatched"]` 非空 | 23635-23639 |
| `column_order_mismatch_detail` | `tv_results["column_order_mismatched"]` 非空 | 23640-23644 |
| `comment_mismatch_detail` | `comment_results["mismatched"]` 非空 | 23645-23649 |
| `extra_mismatch_detail` | 任一扩展对象有差异 | 23650-23654 |
| `dependency_detail` | 依赖有差异 | 23655-23659 |
| `noise_suppressed_detail` | `noise_suppressed_details` 非空 | 23660-23664 |

---

## 3. 发现的问题清单

### 3.1 P0 - 数据源与汇总不一致（高优先级）

| 问题 | 影响 | 位置 |
|-----|------|-----|
| INDEX 不支持汇总数 ≠ indexes_unsupported_detail 行数 | 用户困惑 | 22271 vs 23594 |
| CONSTRAINT 不支持汇总数 ≠ constraints_unsupported_detail 行数 | 用户困惑 | 22272 vs 23605 |

**修复建议**:
1. 方案A: 将 `extra_blocked_counts` 中的 INDEX/CONSTRAINT 也导出到对应的 `*_unsupported_detail` 文件
2. 方案B: 在汇总表中区分 "语法不支持" 和 "依赖阻断"，明确两类数据的输出位置

### 3.2 P1 - 报告命名与内容不符

| 报告 | 实际内容 | 建议改名或说明 |
|-----|---------|--------------|
| `indexes_unsupported_detail` | 仅 DESC 索引 | 改为 `indexes_desc_unsupported_detail` 或添加说明 |
| `constraints_unsupported_detail` | 仅 DEFERRABLE/特定语法约束 | 添加说明 |

### 3.3 P2 - extra_targets_detail 不包含 SEQUENCE

| 问题 | 影响 | 说明 |
|-----|------|-----|
| SEQUENCE 多余数在汇总表显示，但不在 `extra_targets_detail` | 用户找不到多余序列 | SEQUENCE 属于扩展对象，多余信息在 `extra_mismatch_detail` |

**状态**: 设计如此，但需在文档中说明。

### 3.4 P3 - 辅助报告生成依赖文件存在性检查

```python
# schema_diff_reconciler.py:23720-23741
if report_ts:
    mapping_path = report_path.parent / f"object_mapping_{report_ts}.txt"
    if mapping_path.exists():  # ← 依赖文件是否已存在
        _add_index_entry("AUX", mapping_path, None, "全量对象映射")
```

**问题**: 辅助报告添加到索引的逻辑依赖文件已存在，但如果生成顺序有问题，索引可能不完整。

---

## 4. 各报告生成逻辑详细审查

### 4.1 主报告 (report_{timestamp}.txt)

| 检查项 | 状态 | 说明 |
|-------|------|-----|
| 生成条件 | ✅ | 无条件生成 |
| 数据完整性 | ✅ | 汇总所有校验结果 |

### 4.2 明细报告

#### missing_objects_detail

| 检查项 | 状态 | 说明 |
|-------|------|-----|
| 生成条件 | ⚠️ | 需要 `emit_detail_files=True` |
| 数据来源 | ✅ | `support_summary.missing_detail_rows` |
| 与汇总一致性 | ✅ | 一致 |

#### unsupported_objects_detail

| 检查项 | 状态 | 说明 |
|-------|------|-----|
| 生成条件 | ⚠️ | 需要 `emit_detail_files=True` |
| 数据来源 | ✅ | `support_summary.unsupported_rows`（含 INDEX/CONSTRAINT/TRIGGER 因依赖阻断） |
| 与汇总一致性 | ⚠️ | 包含部分 INDEX/CONSTRAINT 数据，与专项报告有交叉 |

#### indexes_unsupported_detail

| 检查项 | 状态 | 说明 |
|-------|------|-----|
| 生成条件 | ⚠️ | 不受 `emit_detail_files` 控制，但数据可能为空 |
| 数据来源 | ❌ | 仅 `extra_results["index_unsupported"]`（DESC索引） |
| 与汇总一致性 | ❌ | **不一致** - 汇总包含 `extra_blocked_counts["INDEX"]` |

#### constraints_unsupported_detail

| 检查项 | 状态 | 说明 |
|-------|------|-----|
| 生成条件 | ⚠️ | 不受 `emit_detail_files` 控制，但数据可能为空 |
| 数据来源 | ❌ | 仅 `extra_results["constraint_unsupported"]`（DEFERRABLE等） |
| 与汇总一致性 | ❌ | **不一致** - 汇总包含 `extra_blocked_counts["CONSTRAINT"]` |

#### extra_targets_detail

| 检查项 | 状态 | 说明 |
|-------|------|-----|
| 生成条件 | ⚠️ | 需要 `emit_detail_files=True` |
| 数据来源 | ⚠️ | 仅 PRIMARY_OBJECT_TYPES，不含 SEQUENCE |
| 与汇总一致性 | ⚠️ | SEQUENCE 多余数不在此报告 |

#### extra_mismatch_detail

| 检查项 | 状态 | 说明 |
|-------|------|-----|
| 生成条件 | ⚠️ | 需要 `emit_detail_files=True` |
| 数据来源 | ✅ | INDEX/CONSTRAINT/SEQUENCE/TRIGGER 差异 |
| 与汇总一致性 | ✅ | 一致 |

### 4.3 辅助报告

| 报告 | 生成条件 | 状态 |
|-----|---------|------|
| `report_index` | 无条件 | ✅ |
| `object_mapping` | fixup 生成时 | ✅ |
| `remap_conflicts` | 有冲突时 | ✅ |
| `dependency_chains` | 有依赖时 | ✅ |
| `VIEWs_chain` | 有 VIEW 缺失时 | ✅ |
| `blacklist_tables` | 有黑名单时 | ✅ |
| `trigger_status_report` | 有触发器差异或 trigger_list 时 | ✅ |
| `filtered_grants` | 有过滤权限时 | ✅ |
| `fixup_skip_summary` | fixup 生成时 | ✅ |
| `ddl_format_report` | ddl_format_enable=true | ✅ |
| `ddl_punct_clean` | 有全角清洗时 | ✅ |
| `ddl_hint_clean` | 有 hint 清洗时 | ✅ |

---

## 5. 修复建议

### 5.1 P0 修复（立即）

**问题**: indexes_unsupported_detail / constraints_unsupported_detail 与汇总数不一致

**修复方案**:

```python
# 方案A: 合并数据源
# 在 export_indexes_unsupported_detail 之前，将 unsupported_rows 中 obj_type="INDEX" 的行
# 转换为 IndexUnsupportedDetail 并合并到 extra_results["index_unsupported"]

# 方案B: 修改汇总显示
# 在汇总表中区分显示：
# - "语法不支持": 来自 index_unsupported / constraint_unsupported
# - "依赖阻断": 来自 extra_blocked_counts
# 分别标注对应的明细报告位置
```

### 5.2 P1 修复（高优先级）

1. **更新报告目录文档** (`REPORTS_CATALOG.txt`):
   - 明确 `indexes_unsupported_detail` 仅包含 DESC 索引
   - 明确 `constraints_unsupported_detail` 仅包含 DEFERRABLE 等语法不支持约束
   - 说明因依赖阻断的 INDEX/CONSTRAINT 在 `unsupported_objects_detail`

2. **在主报告中添加提示**:
   当有 `extra_blocked_counts` 时，提示用户查看 `unsupported_objects_detail`

### 5.3 P2 修复（建议）

1. 在 `report_index` 中添加更详细的内容说明
2. 考虑将 SEQUENCE 多余也输出到 `extra_targets_detail`（需权衡设计）

---

## 6. 用户当前问题的解答

**Q**: 索引不支持显示61个，但 indexes_unsupported_detail 文件未生成？

**A**: 
1. 这61个索引来自"依赖不支持表"的阻断（`extra_blocked_counts["INDEX"]`）
2. 这类索引的明细在 `unsupported_objects_detail_{timestamp}.txt` 中，TYPE 列为 INDEX
3. `indexes_unsupported_detail` 仅包含"DESC列索引"这一特定不支持类型
4. 如果没有 DESC 索引，该文件不会生成

**建议用户操作**:
```bash
# 在 unsupported_objects_detail 中查找 INDEX 类型的行
grep "INDEX" unsupported_objects_detail_*.txt
```

---

## 7. 附录：相关代码位置

| 功能 | 文件 | 行号 |
|-----|------|-----|
| 汇总数计算 | schema_diff_reconciler.py | 22256-22273 |
| extra_blocked_counts 填充 | schema_diff_reconciler.py | 4366-4379 |
| classify_unsupported_indexes | schema_diff_reconciler.py | 6408-6500 |
| classify_unsupported_check_constraints | schema_diff_reconciler.py | 6320-6405 |
| export_missing_objects_detail | schema_diff_reconciler.py | 21244-21281 |
| export_unsupported_objects_detail | schema_diff_reconciler.py | 21284-21321 |
| export_indexes_unsupported_detail | schema_diff_reconciler.py | 21358-21389 |
| export_constraints_unsupported_detail | schema_diff_reconciler.py | 21324-21355 |
| export_extra_targets_detail | schema_diff_reconciler.py | 21392-21402 |
| export_extra_mismatch_detail | schema_diff_reconciler.py | 21497-21556 |
| export_dependency_detail | schema_diff_reconciler.py | 21581-21599 |
| export_blacklist_tables | schema_diff_reconciler.py | 21945-22028 |
| export_trigger_status_report | schema_diff_reconciler.py | 22032-22134 |
| export_filtered_grants | schema_diff_reconciler.py | 22137-22190 |
| export_fixup_skip_summary | schema_diff_reconciler.py | 22193-22228 |
| 报告生成入口 | schema_diff_reconciler.py | 23565-23800 |

---

## 8. 汇总表与明细报告数据来源对照

| 汇总表列 | 对象类型 | 数据来源 | 对应明细报告 |
|---------|---------|---------|-------------|
| 缺失 | 所有主对象 | missing_support_counts | missing_objects_detail |
| 不支持/阻断 | TABLE/VIEW等 | missing_support_counts[unsupported+blocked] | unsupported_objects_detail |
| 不支持/阻断 | INDEX | extra_blocked_counts["INDEX"] + index_unsupported | unsupported_objects_detail (依赖阻断) + indexes_unsupported_detail (DESC) |
| 不支持/阻断 | CONSTRAINT | extra_blocked_counts["CONSTRAINT"] + constraint_unsupported | unsupported_objects_detail (依赖阻断) + constraints_unsupported_detail (DEFERRABLE) |
| 不支持/阻断 | TRIGGER | extra_blocked_counts["TRIGGER"] | unsupported_objects_detail |
| 多余 | PRIMARY_OBJECT_TYPES | extra_targets | extra_targets_detail |
| 多余 | SEQUENCE | extra_results["sequence_mismatched"].extra_sequences | extra_mismatch_detail |
| 多余 | INDEX | extra_results["index_mismatched"].extra_indexes | extra_mismatch_detail |

---

**审查完成**
