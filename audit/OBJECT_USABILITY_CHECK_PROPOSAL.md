# 对象可用性校验功能提案

**提案日期**: 2026-02-02  
**提案状态**: 待评审  
**影响范围**: VIEW, SYNONYM 对象

---

## 一、需求背景

### 1.1 问题描述

当前程序主要校验对象**是否存在**，但无法验证对象**是否真实可用**。以下场景会导致对象存在但不可用：

| 场景 | 说明 |
|-----|------|
| FORCE 创建的视图 | `CREATE FORCE VIEW` 可在依赖对象不存在时创建视图 |
| 状态为 VALID 但实际不可用 | 某些情况下对象状态显示 VALID，但查询时报错 |
| 依赖链断裂 | 视图/同义词依赖的底层对象被删除或失效 |
| 权限问题 | 创建成功但缺少查询权限 |
| 循环依赖 | 同义词或视图形成循环引用 |

### 1.2 用户诉求

1. 验证目标端对象**实际可用性**（不仅仅是存在性）
2. 快速返回结果，不影响程序响应时间
3. 对比源端可用性，判断目标端不可用是否属于"预期行为"
4. 输出详细的可用性报告

---

## 二、功能设计

### 2.1 校验范围

**仅限以下对象类型**：

| 对象类型 | 校验方法 | 说明 |
|---------|---------|------|
| VIEW | `SELECT * FROM {schema}.{view} WHERE 1=2` | 快速验证视图可查询 |
| SYNONYM | `SELECT * FROM {schema}.{synonym} WHERE 1=2` | 验证同义词指向有效对象 |

**不包含的对象类型**：
- TABLE（通常不存在"创建成功但不可用"的问题）
- PROCEDURE/FUNCTION/PACKAGE（需要执行才能验证，风险高）
- TRIGGER（依附于表，表可用则触发器可用）
- INDEX/CONSTRAINT（DDL 级别的对象）

### 2.2 校验方法

使用 `WHERE 1=2` 子句快速返回：

```sql
-- 视图可用性校验
SELECT * FROM {schema}.{view_name} WHERE 1=2;

-- 同义词可用性校验
SELECT * FROM {schema}.{synonym_name} WHERE 1=2;
```

**优点**：
- 不返回实际数据，避免大表扫描
- 仅解析和验证对象引用，秒级返回
- 能捕获大部分"存在但不可用"的问题

**超时保护**：
- 设置查询超时（建议 5-10 秒）
- 超时视为"不确定"状态，不判定为不可用

### 2.3 双端校验逻辑

```
源端可用 + 目标端可用 → ✅ 正常
源端可用 + 目标端不可用 → ❌ 异常（需修复）
源端不可用 + 目标端不可用 → ⚠️ 预期行为（源端问题）
源端不可用 + 目标端可用 → ℹ️ 意外可用（可能迁移时修复了依赖）
```

### 2.4 配置开关

```yaml
# 配置文件中新增
validation:
  # 是否启用对象可用性校验（默认关闭）
  check_object_usability: false
  
  # 校验超时时间（秒）
  usability_check_timeout: 10
  
  # 是否同时校验源端可用性
  check_source_usability: true
  
  # 是否在发现不可用时中断（默认否，继续校验其他对象）
  fail_fast_on_unusable: false
```

**命令行参数**（可选）：
```bash
--check-usability        # 启用可用性校验
--usability-timeout=10   # 超时秒数
--skip-source-check      # 跳过源端校验
```

---

## 三、数据结构设计

### 3.1 可用性校验结果

```python
@dataclass
class UsabilityCheckResult:
    """单个对象的可用性校验结果"""
    schema: str                      # 对象所属 schema
    object_name: str                 # 对象名
    object_type: str                 # VIEW / SYNONYM
    
    # 源端信息
    source_exists: bool              # 源端是否存在
    source_usable: bool              # 源端是否可用
    source_error: Optional[str]      # 源端错误信息（如有）
    source_check_time_ms: int        # 源端校验耗时（毫秒）
    
    # 目标端信息
    target_exists: bool              # 目标端是否存在
    target_usable: bool              # 目标端是否可用
    target_error: Optional[str]      # 目标端错误信息（如有）
    target_check_time_ms: int        # 目标端校验耗时（毫秒）
    
    # 综合判定
    status: str                      # OK / UNUSABLE / EXPECTED_UNUSABLE / UNEXPECTED_USABLE / TIMEOUT
    root_cause: Optional[str]        # 根因分析
    recommendation: Optional[str]    # 修复建议
```

### 3.2 汇总结构

```python
@dataclass
class UsabilitySummary:
    """可用性校验汇总"""
    total_checked: int               # 总校验数
    total_usable: int                # 可用数
    total_unusable: int              # 不可用数
    total_expected_unusable: int     # 预期不可用数（源端也不可用）
    total_timeout: int               # 超时数
    total_skipped: int               # 跳过数（对象不存在）
    
    view_results: List[UsabilityCheckResult]
    synonym_results: List[UsabilityCheckResult]
    
    check_duration_seconds: float    # 总校验耗时
```

---

## 四、报告输出设计

### 4.1 报告文件命名

```
usability_check_detail_{timestamp}.txt
usability_check_view_detail_{timestamp}.txt      # 可选：按类型拆分
usability_check_synonym_detail_{timestamp}.txt   # 可选：按类型拆分
```

### 4.2 报告格式

```
# 对象可用性校验报告
# timestamp={timestamp}
# total_checked={n}
# total_usable={n}
# total_unusable={n}
# total_expected_unusable={n}
# total_timeout={n}
# check_duration_seconds={n.nn}
# 分隔符: |
# 字段说明: SCHEMA|OBJECT_NAME|OBJECT_TYPE|SRC_USABLE|TGT_USABLE|STATUS|ROOT_CAUSE|RECOMMENDATION

SCHEMA|OBJECT_NAME|OBJECT_TYPE|SRC_USABLE|TGT_USABLE|STATUS|ROOT_CAUSE|RECOMMENDATION
LIFEDATA|V_POL_INFO|VIEW|YES|YES|OK||
LIFEDATA|V_CLAIM_DETAIL|VIEW|YES|NO|UNUSABLE|ORA-00942: table or view does not exist|检查依赖表 CLAIMDATA.CLAIM_MAIN 是否存在
LIFEDATA|SYN_POLICY|SYNONYM|NO|NO|EXPECTED_UNUSABLE|源端同义词指向不存在的对象|无需处理，源端问题
UWSDATA|V_AGENT_INFO|VIEW|YES|TIMEOUT|TIMEOUT|查询超时（>10s）|手动验证或增加超时时间
```

### 4.3 详细错误报告（可选）

对于不可用的对象，可输出更详细的诊断信息：

```
usability_errors_detail_{timestamp}.txt
```

```
# 不可用对象详细诊断
# timestamp={timestamp}
# total_errors={n}

================================================================================
[1/5] LIFEDATA.V_CLAIM_DETAIL (VIEW)
================================================================================
源端状态: 可用
目标端状态: 不可用
错误代码: ORA-00942
错误信息: table or view does not exist
校验 SQL: SELECT * FROM LIFEDATA.V_CLAIM_DETAIL WHERE 1=2
根因分析: 视图依赖的表 CLAIMDATA.CLAIM_MAIN 在目标端不存在
修复建议: 
  1. 检查 CLAIMDATA.CLAIM_MAIN 是否已迁移
  2. 执行 missing_table_fixup 脚本
  3. 重新创建视图

================================================================================
[2/5] UWSDATA.V_AGENT_INFO (VIEW)
================================================================================
...
```

---

## 五、执行流程

### 5.1 校验时机

```
主程序执行流程:
1. 元数据采集
2. 对象存在性比较
3. DDL 差异比较
4. ★ 对象可用性校验（新增，可选）★
5. 报告生成
```

### 5.2 校验流程

```
for each (schema, object) in target_objects:
    if object_type not in (VIEW, SYNONYM):
        skip
    
    # 1. 校验目标端
    target_result = check_usability(target_conn, schema, object)
    
    # 2. 校验源端（如果启用）
    if check_source_usability:
        source_result = check_usability(source_conn, schema, object)
    
    # 3. 综合判定
    status = determine_status(source_result, target_result)
    
    # 4. 分析根因
    if status != OK:
        root_cause = analyze_root_cause(target_result.error)
    
    # 5. 记录结果
    results.append(...)
```

### 5.3 并行优化

考虑到对象数量可能较多，建议支持并行校验：

```python
# 使用线程池并行校验
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(check_single_object, obj)
        for obj in objects_to_check
    ]
    results = [f.result() for f in futures]
```

---

## 六、错误码与根因映射

### 6.1 常见错误码

| 错误码 | 错误描述 | 根因分析 | 修复建议 |
|-------|---------|---------|---------|
| ORA-00942 | table or view does not exist | 依赖对象不存在 | 检查并创建依赖对象 |
| ORA-00980 | synonym translation is no longer valid | 同义词指向无效 | 重建同义词或创建目标对象 |
| ORA-01775 | looping chain of synonyms | 同义词循环引用 | 检查并修复同义词链 |
| ORA-00904 | invalid identifier | 列不存在 | 检查依赖表结构 |
| ORA-01031 | insufficient privileges | 权限不足 | 授予查询权限 |
| ORA-04063 | view has errors | 视图编译错误 | 重新编译视图 |
| TIMEOUT | 查询超时 | 复杂查询或锁等待 | 手动验证 |

### 6.2 根因分析函数

```python
def analyze_root_cause(error_msg: str, object_type: str) -> Tuple[str, str]:
    """
    分析错误根因，返回 (root_cause, recommendation)
    """
    if 'ORA-00942' in error_msg:
        # 提取缺失对象名
        missing_obj = extract_missing_object(error_msg)
        return (
            f"依赖对象 {missing_obj} 不存在",
            f"检查 {missing_obj} 是否已迁移，执行相应 fixup 脚本"
        )
    elif 'ORA-00980' in error_msg:
        return (
            "同义词指向的对象不存在或已失效",
            "重建同义词或创建目标对象"
        )
    # ... 其他错误码
```

---

## 七、性能考量

### 7.1 预估耗时

| 对象数量 | 串行耗时（估算） | 并行耗时（10线程） |
|---------|----------------|-------------------|
| 100 | ~100秒 | ~10秒 |
| 1000 | ~1000秒 | ~100秒 |
| 5000 | ~5000秒 | ~500秒 |

*假设每个对象校验 1 秒*

### 7.2 优化策略

1. **并行校验**：使用线程池
2. **超时控制**：单对象超时 10 秒
3. **批量采样**：对于大量对象，可选择采样校验
4. **增量校验**：仅校验本次变更的对象

### 7.3 配置建议

```yaml
validation:
  # 并行线程数
  usability_check_workers: 10
  
  # 单对象超时
  usability_check_timeout: 10
  
  # 最大校验对象数（超过则采样）
  max_objects_to_check: 1000
  
  # 采样比例（当超过 max_objects_to_check 时）
  sample_ratio: 0.1
```

---

## 八、与现有功能的集成

### 8.1 与存在性检查的关系

```
存在性检查 → 对象是否存在于目标端
可用性检查 → 对象是否能实际查询（仅针对存在的对象）
```

### 8.2 报告整合

可用性报告作为**独立子报告**输出，与以下报告并列：

```
report_dir/
├── summary_{timestamp}.txt
├── missing_objects_detail_{timestamp}.txt
├── unsupported_objects_detail_{timestamp}.txt
├── usability_check_detail_{timestamp}.txt      ← 新增
├── usability_errors_detail_{timestamp}.txt     ← 新增（可选）
└── ...
```

### 8.3 统计整合

在最终汇总报告中增加可用性统计：

```
========== 对象可用性统计 ==========
校验对象总数: 1234
  - 视图: 1000
  - 同义词: 234
可用: 1200 (97.2%)
不可用: 30 (2.4%)
  - 预期不可用（源端也不可用）: 25
  - 异常不可用: 5
超时: 4 (0.3%)
```

---

## 九、风险与限制

### 9.1 已知限制

| 限制 | 说明 | 缓解措施 |
|-----|------|---------|
| 无法检测运行时错误 | `WHERE 1=2` 不执行实际逻辑 | 仅作为基本可用性校验 |
| 权限依赖 | 需要 SELECT 权限 | 使用有足够权限的账号 |
| 性能影响 | 大量对象时耗时较长 | 并行 + 超时 + 采样 |
| 锁竞争 | 可能受 DDL 锁影响 | 设置超时 |

### 9.2 不覆盖的场景

- 存储过程/函数的运行时错误
- 数据级别的业务逻辑问题
- 特定条件下才触发的错误

---

## 十、实施计划

### 10.1 开发阶段

| 阶段 | 任务 | 预估工时 |
|-----|------|---------|
| 1 | 核心校验逻辑实现 | 2天 |
| 2 | 配置开关和命令行参数 | 0.5天 |
| 3 | 报告生成和输出 | 1天 |
| 4 | 根因分析和建议生成 | 1天 |
| 5 | 并行优化和超时控制 | 0.5天 |
| 6 | 单元测试和集成测试 | 1天 |
| **总计** | | **6天** |

### 10.2 测试验证

1. **单元测试**：
   - 正常视图校验
   - 不可用视图校验
   - 同义词校验
   - 超时处理
   - 错误码解析

2. **集成测试**：
   - 与主流程集成
   - 报告格式验证
   - 配置开关验证

---

## 十一、决策点

请评审以下决策点：

| # | 决策点 | 选项 | 建议 |
|---|-------|------|------|
| 1 | 默认开关状态 | 开启/关闭 | **关闭**（避免影响现有流程） |
| 2 | 是否按类型拆分报告 | 是/否 | **否**（统一报告更易查看） |
| 3 | 超时时间 | 5/10/30秒 | **10秒** |
| 4 | 是否支持采样 | 是/否 | **是**（大规模场景需要） |
| 5 | 并行线程数 | 5/10/20 | **10**（平衡速度和资源） |

---

**提案人**: Cascade AI  
**待评审人**: [待指定]  
**预计评审日期**: [待定]
