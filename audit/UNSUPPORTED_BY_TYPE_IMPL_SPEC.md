# 对象分类输出与报告优化实现规格

**设计日期**: 2026-01-27  
**关联文档**: BLACKLIST_DEPENDENCY_TEST_CASE.md  
**版本**: v2.0

---

## 一、需求概述

### 1.1 当前问题

1. **混合输出**: 现有 `unsupported_objects_detail` 和 `missing_objects_detail` 将所有对象混合输出，用户难以按类型查看
2. **报告重复**: 多个子报告内容重叠，造成信息冗余
3. **序列噪声**: 序列比较检查属性差异（INCREMENT_BY, CACHE_SIZE等），产生大量误报

### 1.2 目标

1. **按类型分类输出**: UNSUPPORTED 和 MISSING 对象都按类型分别输出到独立文件
2. **去重子报告**: 识别并移除内容重复的子报告
3. **简化序列检查**: 仅检查序列存在性，不比较属性
4. 增加 ROOT_CAUSE 字段追溯到源头黑名单表
5. 确保统计公式正确: `缺失 + 不支持/阻断 - 多余 = Oracle源端`

---

## 二、输出文件规格

### 2.1 文件命名

#### UNSUPPORTED/BLOCKED 对象

| 对象类型 | 文件名 |
|---------|--------|
| TABLE | `unsupported_table_detail_{timestamp}.txt` |
| VIEW | `unsupported_view_detail_{timestamp}.txt` |
| SYNONYM | `unsupported_synonym_detail_{timestamp}.txt` |
| TRIGGER | `unsupported_trigger_detail_{timestamp}.txt` |
| INDEX | `unsupported_index_detail_{timestamp}.txt` |
| CONSTRAINT | `unsupported_constraint_detail_{timestamp}.txt` |
| PROCEDURE | `unsupported_procedure_detail_{timestamp}.txt` |
| FUNCTION | `unsupported_function_detail_{timestamp}.txt` |
| PACKAGE | `unsupported_package_detail_{timestamp}.txt` |
| PACKAGE BODY | `unsupported_package_body_detail_{timestamp}.txt` |
| TYPE | `unsupported_type_detail_{timestamp}.txt` |
| TYPE BODY | `unsupported_type_body_detail_{timestamp}.txt` |

#### MISSING 对象 (新增)

| 对象类型 | 文件名 |
|---------|--------|
| TABLE | `missing_table_detail_{timestamp}.txt` |
| VIEW | `missing_view_detail_{timestamp}.txt` |
| SYNONYM | `missing_synonym_detail_{timestamp}.txt` |
| TRIGGER | `missing_trigger_detail_{timestamp}.txt` |
| INDEX | `missing_index_detail_{timestamp}.txt` |
| CONSTRAINT | `missing_constraint_detail_{timestamp}.txt` |
| SEQUENCE | `missing_sequence_detail_{timestamp}.txt` |
| PROCEDURE | `missing_procedure_detail_{timestamp}.txt` |
| FUNCTION | `missing_function_detail_{timestamp}.txt` |
| PACKAGE | `missing_package_detail_{timestamp}.txt` |
| PACKAGE BODY | `missing_package_body_detail_{timestamp}.txt` |
| TYPE | `missing_type_detail_{timestamp}.txt` |
| TYPE BODY | `missing_type_body_detail_{timestamp}.txt` |

### 2.2 文件格式

```
# 不支持/阻断 {OBJECT_TYPE} 明细
# timestamp={timestamp}
# total={count}
# 分隔符: |
# 字段说明: SRC_FULL|TGT_FULL|STATE|REASON_CODE|REASON|DEPENDENCY|ROOT_CAUSE

SRC_FULL|TGT_FULL|STATE|REASON_CODE|REASON|DEPENDENCY|ROOT_CAUSE
SCHEMA.OBJ1|SCHEMA.OBJ1|BLOCKED|DEPENDENCY_UNSUPPORTED|依赖表 X 不支持|X|X(LONG)
...
```

### 2.3 字段定义

| 字段 | 类型 | 说明 |
|-----|------|------|
| SRC_FULL | STRING | 源端对象全名 SCHEMA.NAME |
| TGT_FULL | STRING | 目标端对象全名 |
| STATE | ENUM | UNSUPPORTED / BLOCKED |
| REASON_CODE | STRING | 原因代码 |
| REASON | STRING | 直接原因描述 |
| DEPENDENCY | STRING | 直接依赖对象 |
| ROOT_CAUSE | STRING | 根因对象及原因，格式: `OBJECT(REASON)` |

---

## 三、数据结构修改

### 3.1 ObjectSupportReportRow 扩展

```python
# 文件: schema_diff_reconciler.py
# 位置: ObjectSupportReportRow 定义处

@dataclass
class ObjectSupportReportRow:
    src_full: str
    obj_type: str
    tgt_full: str
    state: str           # SUPPORTED / UNSUPPORTED / BLOCKED
    reason_code: str
    reason: str
    dependency: str
    action: str
    detail: str
    root_cause: str = "" # 新增: 根因追溯，格式 "TABLE_NAME(REASON)"
```

### 3.2 根因追溯逻辑

在 `classify_missing_objects` 中，当标记对象为 BLOCKED 时，记录根因：

```python
def _trace_root_cause(
    obj_full: str,
    obj_type: str,
    dependency_graph: Dict[str, Set[str]],
    unsupported_set: Set[str]
) -> str:
    """
    追溯依赖链，找到最终的不支持根因。
    返回格式: "ROOT_OBJ(REASON)"
    """
    visited = set()
    current = obj_full
    
    while current and current not in visited:
        visited.add(current)
        if current in unsupported_set:
            # 找到根因
            reason = unsupported_reasons.get(current, "UNSUPPORTED")
            return f"{current}({reason})"
        # 继续追溯依赖
        deps = dependency_graph.get(current, set())
        blocked_dep = next((d for d in deps if d in unsupported_set or d in blocked_set), None)
        if blocked_dep:
            current = blocked_dep
        else:
            break
    
    return ""
```

---

## 四、新增导出函数

### 4.1 函数签名

```python
def export_unsupported_by_type(
    unsupported_rows: List[ObjectSupportReportRow],
    report_dir: Path,
    report_timestamp: str
) -> Dict[str, Optional[Path]]:
    """
    按对象类型分别输出不支持/阻断对象明细。
    
    Args:
        unsupported_rows: 不支持/阻断对象行列表
        report_dir: 报告输出目录
        report_timestamp: 报告时间戳
    
    Returns:
        字典 {object_type: output_path}，若某类型无数据则 path 为 None
    """
```

### 4.2 实现代码

```python
def export_unsupported_by_type(
    unsupported_rows: List[ObjectSupportReportRow],
    report_dir: Path,
    report_timestamp: str
) -> Dict[str, Optional[Path]]:
    """按对象类型分别输出不支持/阻断对象明细。"""
    if not unsupported_rows or not report_dir:
        return {}
    
    # 按类型分组
    by_type: Dict[str, List[ObjectSupportReportRow]] = defaultdict(list)
    for row in unsupported_rows:
        by_type[row.obj_type].append(row)
    
    result: Dict[str, Optional[Path]] = {}
    
    for obj_type, rows in sorted(by_type.items()):
        if not rows:
            result[obj_type] = None
            continue
        
        # 生成文件名
        type_lower = obj_type.lower().replace(' ', '_')
        output_path = report_dir / f"unsupported_{type_lower}_detail_{report_timestamp}.txt"
        
        # 构建文件内容
        lines: List[str] = [
            f"# 不支持/阻断 {obj_type} 明细",
            f"# timestamp={report_timestamp}",
            f"# total={len(rows)}",
            "# 分隔符: |",
            "# 字段说明: SRC_FULL|TGT_FULL|STATE|REASON_CODE|REASON|DEPENDENCY|ROOT_CAUSE",
            "SRC_FULL|TGT_FULL|STATE|REASON_CODE|REASON|DEPENDENCY|ROOT_CAUSE"
        ]
        
        # 排序并写入数据行
        sorted_rows = sorted(rows, key=lambda r: (r.state, r.src_full))
        for row in sorted_rows:
            line = "|".join([
                sanitize_pipe_field(row.src_full),
                sanitize_pipe_field(row.tgt_full),
                sanitize_pipe_field(row.state),
                sanitize_pipe_field(row.reason_code),
                sanitize_pipe_field(row.reason),
                sanitize_pipe_field(row.dependency),
                sanitize_pipe_field(row.root_cause)
            ])
            lines.append(line)
        
        # 写入文件
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            result[obj_type] = output_path
            log.info("输出不支持 %s 明细: %s (%d 条)", obj_type, output_path, len(rows))
        except OSError as exc:
            log.warning("写入 unsupported_%s_detail 失败: %s", type_lower, exc)
            result[obj_type] = None
    
    return result
```

---

## 五、调用位置修改

### 5.1 print_final_report 修改

```python
# 文件: schema_diff_reconciler.py
# 函数: print_final_report
# 位置: 在现有 export_unsupported_objects_detail 调用后

# 现有代码
if emit_detail_files:
    if support_summary.unsupported_rows:
        unsupported_path = export_unsupported_objects_detail(
            support_summary.unsupported_rows,
            report_dir,
            report_timestamp
        )
        if unsupported_path:
            report_index_rows.append(
                ReportIndexRow("detail", str(unsupported_path.name), 
                              len(support_summary.unsupported_rows),
                              "不支持/阻断对象明细(汇总)")
            )

# 新增代码: 按类型分别输出
        unsupported_by_type_paths = export_unsupported_by_type(
            support_summary.unsupported_rows,
            report_dir,
            report_timestamp
        )
        for obj_type, path in unsupported_by_type_paths.items():
            if path:
                type_rows = [r for r in support_summary.unsupported_rows 
                            if r.obj_type == obj_type]
                report_index_rows.append(
                    ReportIndexRow("detail", str(path.name),
                                  len(type_rows),
                                  f"不支持/阻断 {obj_type} 明细")
                )
```

---

## 六、统计逻辑验证

### 6.1 INDEX/CONSTRAINT/TRIGGER 统计规则

**当前实现** (需验证):

```python
# classify_missing_objects 中
for item in extra_results.get("index_mismatched", []):
    table_key = (item.schema, item.table_name)
    if table_key in unsupported_table_keys:
        # 索引所属表不支持 → 索引计入 extra_blocked_counts
        extra_blocked_counts["INDEX"] = extra_blocked_counts.get("INDEX", 0) + len(item.missing_indexes)
        # 同时添加到 unsupported_rows (用于明细输出)
        for idx_name in item.missing_indexes:
            unsupported_rows.append(ObjectSupportReportRow(
                src_full=f"{item.schema}.{idx_name}",
                obj_type="INDEX",
                tgt_full=f"{tgt_schema}.{idx_name}",
                state="BLOCKED",
                reason_code="DEPENDENCY_UNSUPPORTED",
                reason=f"依赖表 {item.schema}.{item.table_name} 不支持",
                dependency=f"{item.schema}.{item.table_name}",
                action="SKIP",
                detail="",
                root_cause=f"{item.schema}.{item.table_name}(LONG/LONG_RAW)"
            ))
```

### 6.2 汇总表数据来源

确保 `build_unsupported_summary_counts` 正确合并:

```python
def build_unsupported_summary_counts(
    support_summary: SupportSummary,
    extra_results: Dict[str, Any]
) -> Dict[str, int]:
    """构建不支持/阻断汇总数。"""
    counts = {}
    
    # 1. 主对象 (TABLE, VIEW, PROCEDURE, etc.)
    for obj_type, count in support_summary.missing_support_counts.items():
        unsupported = count.get("unsupported", 0) + count.get("blocked", 0)
        if unsupported > 0:
            counts[obj_type] = counts.get(obj_type, 0) + unsupported
    
    # 2. 扩展对象因依赖阻断 (INDEX, CONSTRAINT, TRIGGER)
    for obj_type, count in support_summary.extra_blocked_counts.items():
        if count > 0:
            counts[obj_type] = counts.get(obj_type, 0) + count
    
    # 3. 语法不支持 (DESC索引, DEFERRABLE约束)
    index_unsupported = extra_results.get("index_unsupported", [])
    if index_unsupported:
        counts["INDEX"] = counts.get("INDEX", 0) + len(index_unsupported)
    
    constraint_unsupported = extra_results.get("constraint_unsupported", [])
    if constraint_unsupported:
        counts["CONSTRAINT"] = counts.get("CONSTRAINT", 0) + len(constraint_unsupported)
    
    return counts
```

---

## 七、report_index 更新

### 7.1 _infer_report_index_meta 扩展

```python
def _infer_report_index_meta(filename: str) -> Tuple[str, str]:
    """根据文件名推断报告分类和描述。"""
    # ... 现有映射
    
    # 新增映射
    if filename.startswith("unsupported_") and "_detail_" in filename:
        # 解析对象类型
        match = re.match(r"unsupported_(.+)_detail_", filename)
        if match:
            obj_type = match.group(1).replace('_', ' ').upper()
            return ("detail", f"不支持/阻断 {obj_type} 明细")
    
    # ... 其他映射
```

---

## 八、测试验证

### 8.1 单元测试

```python
def test_export_unsupported_by_type():
    """测试按类型导出不支持对象。"""
    rows = [
        ObjectSupportReportRow("S.V1", "VIEW", "T.V1", "BLOCKED", 
                              "DEPENDENCY_UNSUPPORTED", "依赖表不支持", 
                              "S.T1", "SKIP", "", "S.T1(LONG)"),
        ObjectSupportReportRow("S.V2", "VIEW", "T.V2", "BLOCKED",
                              "DEPENDENCY_UNSUPPORTED", "依赖表不支持",
                              "S.T1", "SKIP", "", "S.T1(LONG)"),
        ObjectSupportReportRow("S.SYN1", "SYNONYM", "T.SYN1", "BLOCKED",
                              "DEPENDENCY_UNSUPPORTED", "依赖表不支持",
                              "S.T1", "SKIP", "", "S.T1(LONG)"),
    ]
    
    with tempfile.TemporaryDirectory() as tmpdir:
        result = export_unsupported_by_type(rows, Path(tmpdir), "20260127")
        
        assert "VIEW" in result
        assert "SYNONYM" in result
        assert result["VIEW"].exists()
        assert result["SYNONYM"].exists()
        
        # 验证文件内容
        view_content = result["VIEW"].read_text()
        assert "total=2" in view_content
        assert "S.V1" in view_content
        assert "S.V2" in view_content
```

### 8.2 集成测试

使用 BLACKLIST_DEPENDENCY_TEST_CASE.md 中的测试数据执行完整校验。

---

## 九、实施计划

### 9.1 修改清单

| 序号 | 文件 | 修改内容 | 优先级 |
|-----|------|---------|-------|
| 1 | schema_diff_reconciler.py | 扩展 ObjectSupportReportRow 增加 root_cause | P0 |
| 2 | schema_diff_reconciler.py | 新增 export_unsupported_by_type 函数 | P0 |
| 3 | schema_diff_reconciler.py | 修改 classify_missing_objects 填充 root_cause | P1 |
| 4 | schema_diff_reconciler.py | 修改 print_final_report 调用新函数 | P0 |
| 5 | schema_diff_reconciler.py | 扩展 _infer_report_index_meta | P2 |
| 6 | REPORTS_CATALOG.txt | 更新文档 | P2 |

### 9.2 风险评估

| 风险 | 影响 | 缓解措施 |
|-----|------|---------|
| root_cause 追溯性能 | 大量对象时可能慢 | 使用缓存避免重复计算 |
| 文件数量增多 | 输出目录文件变多 | 可配置是否启用分类输出 |
| 向后兼容 | 新字段可能影响解析 | root_cause 默认空字符串 |

---

## 十、MISSING 对象分类输出 (新增)

### 10.1 函数签名

```python
def export_missing_by_type(
    missing_rows: List[ObjectSupportReportRow],
    report_dir: Path,
    report_timestamp: str
) -> Dict[str, Optional[Path]]:
    """
    按对象类型分别输出 MISSING 对象明细。
    逻辑与 export_unsupported_by_type 类似。
    """
```

### 10.2 文件格式

```
# 缺失 {OBJECT_TYPE} 明细
# timestamp={timestamp}
# total={count}
# 分隔符: |
# 字段说明: SRC_FULL|TGT_FULL|STATE|ACTION|FIXUP_GENERATED

SRC_FULL|TGT_FULL|STATE|ACTION|FIXUP_GENERATED
SCHEMA.OBJ1|SCHEMA.OBJ1|MISSING|CREATE|YES
SCHEMA.OBJ2|SCHEMA.OBJ2|MISSING|CREATE|YES
```

### 10.3 调用位置

在 `print_final_report` 中，与 unsupported 类似：

```python
# 按类型分别输出 MISSING 对象
if support_summary.missing_rows:
    missing_by_type_paths = export_missing_by_type(
        support_summary.missing_rows,
        report_dir,
        report_timestamp
    )
    for obj_type, path in missing_by_type_paths.items():
        if path:
            # 添加到报告索引
            ...
```

---

## 十一、重复子报告识别与移除

### 11.1 当前重复报告分析

| 报告文件 | 内容来源 | 是否与其他重复 | 处理建议 |
|---------|---------|--------------|---------|
| `missing_objects_detail` | 缺失对象汇总 | 与 `missing_{type}_detail` 重复 | **保留汇总，按需启用分类** |
| `unsupported_objects_detail` | 不支持对象汇总 | 与 `unsupported_{type}_detail` 重复 | **保留汇总，按需启用分类** |
| `indexes_unsupported_detail` | DESC 索引 | 与 `unsupported_index_detail` 部分重复 | **合并到 unsupported_index_detail** |
| `constraints_unsupported_detail` | DEFERRABLE 约束 | 与 `unsupported_constraint_detail` 部分重复 | **合并到 unsupported_constraint_detail** |
| `extra_mismatch_detail` | 汇总所有 mismatch | 与各类型 mismatch 报告重复 | **考虑移除或设为可选** |

### 11.2 处理方案

1. **默认行为**: 仅生成分类明细文件 (`missing_{type}_detail`, `unsupported_{type}_detail`)
2. **汇总文件**: 通过配置开关控制是否额外生成汇总文件
3. **废弃报告**: 
   - `indexes_unsupported_detail` → 合并到 `unsupported_index_detail`
   - `constraints_unsupported_detail` → 合并到 `unsupported_constraint_detail`

### 11.3 配置开关

```yaml
report:
  by_type_detail: true          # 按类型分类输出
  summary_detail: false         # 是否额外生成汇总文件
  legacy_unsupported: false     # 是否生成旧版 indexes_unsupported_detail 等
```

---

## 十二、序列检查简化

### 12.1 当前问题

`compare_sequences_for_schema` 函数检查以下属性差异：
- `INCREMENT_BY`
- `MIN_VALUE`
- `MAX_VALUE`
- `CYCLE_FLAG`
- `ORDER_FLAG`
- `CACHE_SIZE`

这些属性差异在迁移场景中通常不重要，产生大量噪声。

### 12.2 修改方案

**仅检查存在性**，移除属性比较：

```python
def compare_sequences_for_schema(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    tgt_schema: str
) -> Tuple[bool, Optional[SequenceMismatch]]:
    """简化版：仅检查序列存在性。"""
    src_seqs = oracle_meta.sequences.get(src_schema.upper())
    if src_seqs is None:
        # ... 处理无元数据情况 (保持现有逻辑)
        return True, None
    
    tgt_seqs = ob_meta.sequences.get(tgt_schema.upper(), set())
    
    missing = src_seqs - tgt_seqs
    extra = tgt_seqs - src_seqs
    
    # ★移除属性比较逻辑★
    # 不再调用 _compare_attrs
    
    if not missing and not extra:
        return True, None
    
    return False, SequenceMismatch(
        src_schema=src_schema,
        tgt_schema=tgt_schema,
        missing_sequences=missing,
        extra_sequences=extra,
        note=None,
        missing_mappings=None,
        detail_mismatch=None  # ★不再填充属性差异★
    )
```

### 12.3 SequenceMismatch 清理

`detail_mismatch` 字段将不再使用，可考虑：
1. 保留字段但始终为 None（向后兼容）
2. 在未来版本中废弃

### 12.4 报告输出调整

序列报告将只显示：
- 缺失序列列表
- 多余序列列表

不再显示属性差异详情。

---

## 十三、配置开关汇总

```yaml
# config.yaml
report:
  by_type_detail: true          # 按类型分类输出 MISSING/UNSUPPORTED
  summary_detail: false         # 是否额外生成汇总文件
  legacy_unsupported: false     # 是否生成旧版细分报告

sequence:
  check_attrs: false            # 是否检查序列属性 (默认false=仅检查存在性)
```

---

## 十四、实施计划更新

### 14.1 修改清单

| 序号 | 文件 | 修改内容 | 优先级 |
|-----|------|---------|-------|
| 1 | schema_diff_reconciler.py | 新增 export_missing_by_type 函数 | P0 |
| 2 | schema_diff_reconciler.py | 新增 export_unsupported_by_type 函数 | P0 |
| 3 | schema_diff_reconciler.py | 简化 compare_sequences_for_schema | P0 |
| 4 | schema_diff_reconciler.py | 移除/合并重复报告生成逻辑 | P1 |
| 5 | schema_diff_reconciler.py | 修改 print_final_report 调用新函数 | P0 |
| 6 | schema_diff_reconciler.py | 添加配置开关支持 | P2 |
| 7 | REPORTS_CATALOG.md | 更新文档 | P2 |

### 14.2 风险评估

| 风险 | 影响 | 缓解措施 |
|-----|------|---------|
| 序列属性不再检查 | 可能遗漏需要关注的差异 | 提供 check_attrs 配置开关 |
| 报告格式变化 | 下游工具可能依赖旧格式 | 提供 legacy 模式 |
| 文件数量变化 | 用户习惯改变 | 渐进式推广，文档说明 |

---

**文档版本**: v2.0  
**最后更新**: 2026-01-27  
**变更说明**: 新增 MISSING 分类输出、重复报告识别、序列检查简化
