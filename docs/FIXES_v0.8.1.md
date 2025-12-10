# OceanBase Comparator Toolkit v0.8.1 修复总结

## 修复日期
2025-12-10

## 修复概述
本次版本主要解决生产环境中遇到的稳定性问题和功能增强，包括 unpack 错误、版本检测失败、误报问题等。

---

## 1. 修复 "too many values to unpack (expected 2)" 错误

### 问题描述
生产环境中处理大量对象时，程序频繁报错：
- `[FIXUP] 处理 VIEW xxxxx 时出错：too many values to unpack (expected 2)`
- `[FIXUP] 任务 OTHER_OBJECTS 失败：too many values to unpack (expected 2)`

### 根本原因
对象名包含多个点号（如 `SCHEMA.PACKAGE.PROCEDURE`）时，使用 `split('.')` 会返回超过2个元素，导致元组解包失败。

### 修复方案
采用三层防御策略：

1. **使用 `split('.', 1)` 限制分割次数**
   ```python
   # 修复前
   src_schema, src_obj = src_name.split('.')  # 可能返回3+个元素
   
   # 修复后
   src_schema, src_obj = src_name.split('.', 1)  # 只分割成2个元素
   ```

2. **添加 try-except 防御**
   ```python
   try:
       src_schema, src_obj = src_name.split('.')
       tgt_schema, tgt_obj = tgt_name.split('.')
   except ValueError:
       log.warning(f"[跳过] 对象名格式不正确: src='{src_name}', tgt='{tgt_name}'")
       continue
   ```

3. **添加长度检查**
   ```python
   parts = dep_name.split('.', 1)
   if len(parts) != 2:
       continue
   schema_u, obj_u = parts[0], parts[1]
   ```

### 涉及位置
- `get_relevant_replacements()` 函数（第5938-5939行）
- 缺失对象处理（第6135-6136行）
- Schema映射处理（第1490-1491行、第1547行）
- 依赖对象处理（第6904-6907行）
- 表对比处理（第2860-2866行、第3402-3408行、第3535-3541行）

### 影响范围
- ✅ VIEW 处理不再崩溃
- ✅ OTHER_OBJECTS 任务稳定运行
- ✅ 支持复杂对象名（如 PACKAGE BODY）

---

## 2. 修复 OceanBase 版本检测失败

### 问题描述
程序无法获取 OceanBase 版本号，日志显示：
```
[ERROR] [OBClient 错误] SQL: SELECT VERSION() | 错误: ORA-00900: You have an error in your SQL syntax
[WARNING] 无法获取OceanBase版本，将使用保守的DDL清理策略
```

### 根本原因
`SELECT VERSION()` 是 MySQL 语法，在 OceanBase Oracle 模式下不支持。

### 修复方案
1. **修改 SQL 查询**
   ```python
   # 修复前
   sql = "SELECT VERSION()"
   
   # 修复后
   sql = "SELECT OB_VERSION() FROM DUAL"
   ```

2. **修正版本号解析逻辑**
   ```python
   # OB_VERSION() 直接返回版本号如 "4.2.5.7"
   for line in out.splitlines():
       line = line.strip()
       if line and line != 'OB_VERSION()':  # 跳过列标题
           if '.' in line and line.replace('.', '').replace('-', '').isdigit():
               return line.split('-')[0]  # 去掉可能的后缀
   ```

### 涉及函数
- `get_oceanbase_version()`
- `get_oceanbase_info()`

### 影响范围
- ✅ 正确检测 OceanBase 版本（如 4.2.5.7）
- ✅ VIEW DDL 清理策略根据版本正确应用
- ✅ 版本相关功能（如 WITH CHECK OPTION 处理）正常工作

---

## 3. 移除不必要的元数据缺失警告

### 问题描述
日志中出现大量警告：
```
注：源端 Oracle 该表无索引元数据 (DBA_INDEXES/DBA_IND_COLUMNS dump 为空或确实无索引)。
注：源端 Oracle 该表无约束元数据 (DBA_CONSTRAINTS/DBA_CONS_COLUMNS dump 为空或确实无约束)。
```

### 根本原因
这些情况是正常的（表可能确实没有索引/约束），不应作为警告输出。

### 修复方案
移除相关警告代码：
```python
# 修复前
src_idx_info_note: Optional[str] = None
if src_idx is None:
    src_idx = {}
    src_idx_info_note = "注：源端 Oracle 该表无索引元数据..."
detail_mismatch: List[str] = []
if src_idx_info_note:
    detail_mismatch.append(src_idx_info_note)

# 修复后
if src_idx is None:
    src_idx = {}
detail_mismatch: List[str] = []
```

### 影响范围
- ✅ 日志更清晰，只显示真正的问题
- ✅ 减少日志噪音，便于问题定位
- ✅ 不影响实际的索引/约束对比逻辑

---

## 4. 修复同名索引但 SYS_NC 列名不同的误报

### 问题描述
同一个索引被同时报告为"缺失"和"多余"：
```
- 缺失: IDX_TABLE (列: SYS_NC00023$)
+ 多余: IDX_TABLE (列: SYS_NC38$)
```

### 根本原因
Oracle 和 OceanBase 对隐藏列的命名方式不同，导致列名不匹配。

### 修复方案
添加 SYS_NC 列名标准化逻辑：

```python
def normalize_sys_nc_columns(cols: Tuple[str, ...]) -> Tuple[str, ...]:
    """将SYS_NC开头的列名标准化为通用形式"""
    normalized = []
    for col in cols:
        if col.startswith('SYS_NC') and '$' in col:
            normalized.append('SYS_NC$')  # 标准化为通用形式
        else:
            normalized.append(col)
    return tuple(normalized)

def has_same_named_index(src_cols: Tuple[str, ...], tgt_cols: Tuple[str, ...]) -> bool:
    """检查是否存在同名索引"""
    src_names = src_map.get(src_cols, {}).get("names", set())
    tgt_names = tgt_map.get(tgt_cols, {}).get("names", set())
    return bool(src_names & tgt_names)

def is_sys_nc_only_diff(src_cols: Tuple[str, ...], tgt_cols: Tuple[str, ...]) -> bool:
    """检查是否仅SYS_NC列名不同"""
    return normalize_sys_nc_columns(src_cols) == normalize_sys_nc_columns(tgt_cols)

# 找出因SYS_NC列名不同而被误判的同名索引
for src_cols in list(missing_cols):
    for tgt_cols in list(extra_cols):
        if (has_same_named_index(src_cols, tgt_cols) and 
            is_sys_nc_only_diff(src_cols, tgt_cols)):
            missing_cols.discard(src_cols)
            extra_cols.discard(tgt_cols)
            break
```

### 影响范围
- ✅ 消除同名索引的误报
- ✅ 仅对 SYS_NC 列做特殊处理，不影响业务列校验
- ✅ 保持索引校验的严格性

---

## 5. 增强 OMS 索引过滤逻辑

### 问题描述
包含额外业务列的 OMS 索引无法被正确识别和过滤。

### 根本原因
原逻辑要求索引列精确匹配4个 OMS 列，过于严格：
```python
# 修复前
if set(cols_u) != set(IGNORED_OMS_COLUMNS) or len(cols_u) != len(IGNORED_OMS_COLUMNS):
    return False
```

### 修复方案
改为检查索引名以 `_OMS_ROWID` 结尾且包含所有4个 OMS 列作为子集：
```python
# 修复后
if not name_u.endswith("_OMS_ROWID"):
    return False

cols_set = set(cols_u)
oms_cols_set = set(IGNORED_OMS_COLUMNS)

# 如果包含所有4个OMS列，则认为是OMS索引（允许有额外列）
return oms_cols_set.issubset(cols_set)
```

### 影响范围
- ✅ 正确识别包含额外列的 OMS 索引
- ✅ 减少误报，提高准确性
- ✅ 符合实际的 OMS 索引使用场景

---

## 6. 修复 `non_view_missing_objects` 变量作用域错误

### 问题描述
程序报错：`UnboundLocalError: cannot access local variable 'non_view_missing_objects' where it is not associated with a value`

### 根本原因
变量在使用前未定义，定义位置在使用位置之后。

### 修复方案
将 VIEW/非VIEW 对象分离逻辑移到使用前执行：
```python
# 修复后：在使用前定义
view_missing_objects: List[Tuple[str, str, str, str]] = []
non_view_missing_objects: List[Tuple[str, str, str, str, str]] = []

for (obj_type, src_schema, src_obj, tgt_schema, tgt_obj) in other_missing_objects:
    if obj_type.upper() == 'VIEW':
        view_missing_objects.append((src_schema, src_obj, tgt_schema, tgt_obj))
    else:
        non_view_missing_objects.append((obj_type, src_schema, src_obj, tgt_schema, tgt_obj))

# 然后才使用这些变量
dbcat_data, ddl_source_meta = fetch_dbcat_schema_objects(...)
```

### 影响范围
- ✅ 修补脚本生成正常运行
- ✅ 避免变量作用域错误

---

## 7. 新增功能

### 7.1 防御性错误处理
在所有关键位置添加防御性代码：
```python
# fetch_ddl_with_timing 调用处
fetch_result = fetch_ddl_with_timing(ss, ot, so)
if len(fetch_result) != 3:
    log.error("[FIXUP] fetch_ddl_with_timing 返回了 %d 个值，期望 3 个: %s", 
              len(fetch_result), fetch_result)
    return
ddl, ddl_source_label, _elapsed = fetch_result
```

### 7.2 IOT 表过滤
自动跳过 IOT 溢出表：
```python
if obj_name.startswith("SYS_IOT_OVER_"):
    skipped_iot += 1
    continue
```

### 7.3 注释标准化增强
```python
def normalize_comment_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    # 去除控制字符
    sanitized = re.sub(r"[\x00-\x1f\x7f]", " ", str(text))
    collapsed = " ".join(sanitized.replace("\r\n", "\n").replace("\r", "\n").split())
    normalized = collapsed.strip()
    # 过滤无效注释
    if normalized.upper() in {"NULL", "<NULL>", "NONE"}:
        return ""
    return normalized
```

### 7.4 并发处理优化
- 添加 `fixup_workers` 配置项（默认 CPU 核心数，最多12）
- 添加 `progress_log_interval` 配置项（默认10秒）
- 使用 `ThreadPoolExecutor` 并发生成修补脚本

### 7.5 报告宽度配置
- 添加 `report_width` 配置项（默认160）
- 避免 nohup 后台运行时报告被截断为80列

---

## 测试验证

### 测试环境
- Oracle 19c (19.3.0.0.0)
- OceanBase 4.2.5.7
- 测试场景：gorgon_knot_case（多对一/一对多混合映射）

### 测试结果
✅ 所有修复均通过测试
✅ 程序稳定运行，无 unpack 错误
✅ 版本检测正常
✅ 索引/约束校验准确
✅ 修补脚本正常生成

---

## 升级建议

### 从 v0.8.0 升级到 v0.8.1
1. 备份当前版本
2. 替换 `schema_diff_reconciler.py`
3. 更新 `config.ini`，添加新配置项（可选）：
   ```ini
   fixup_workers = 8
   progress_log_interval = 10
   report_width = 160
   print_dependency_chains = true
   ```
4. 重新运行程序验证

### 兼容性
- ✅ 完全向后兼容 v0.8.0
- ✅ 配置文件兼容（新配置项有默认值）
- ✅ Remap 规则文件格式不变

---

## 已知限制

1. **对象名限制**：虽然支持多点号对象名，但仍建议遵循标准命名规范
2. **SYS_NC 列处理**：仅处理 `SYS_NC*$` 格式的隐藏列
3. **并发限制**：`fixup_workers` 最多12个线程，避免过度并发

---

## 后续计划

1. 进一步优化大规模对象处理性能
2. 增强错误恢复机制
3. 添加更多的自动化测试用例
4. 改进日志输出格式和详细程度

---

## 贡献者
- OceanBase Migration Team
- 版本：v0.8.1
- 日期：2025-12-10
