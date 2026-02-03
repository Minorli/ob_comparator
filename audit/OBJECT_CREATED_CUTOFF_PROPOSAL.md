# 基于对象创建时间的增量校验功能提案

**提案日期**: 2024年  
**功能类型**: 新增功能  
**影响范围**: 对象过滤与校验范围控制  

---

## 一、功能概述

### 1.1 需求背景

在实际迁移场景中，存在以下诉求：
- **存量校验**: 只关注某个时间点之前创建的对象是否已成功迁移
- **增量隔离**: 新创建的对象可能尚在迁移流水线中，暂不纳入校验
- **变更窗口**: 在某个变更窗口期间创建的对象需要单独处理
- **双向感知**: 即使对象不在校验范围内，如果 OB 端已存在，也需要感知

### 1.2 功能目标

| 目标 | 说明 |
|-----|------|
| **时间截止过滤** | 只校验 Oracle `DBA_OBJECTS.CREATED` 在指定时间之前的对象 |
| **默认当前时间** | 不配置时默认使用程序运行时间，等同于当前行为 |
| **新对象感知** | 截止时间之后的对象如果在 OB 存在，标记为 `EXTRA_RECENT` |
| **保留原功能** | 不影响现有校验逻辑，仅作为前置过滤器 |

### 1.3 使用场景示例

```
场景1: 迁移割接校验
  - 割接时间点: 2024-01-15 00:00:00
  - 配置 object_created_cutoff = 2024-01-15 00:00:00
  - 只校验割接前创建的对象，割接后新建的对象排除

场景2: 增量迁移验证
  - 上次校验时间: 2024-02-01 10:00:00  
  - 配置 object_created_cutoff = 2024-02-01 10:00:00
  - 本次只校验上次之前的存量，新增对象下次再验

场景3: 默认全量校验
  - 不配置或配置为空
  - 默认使用当前时间，所有对象都在校验范围内
```

---

## 二、配置设计

### 2.1 新增配置项

```ini
[SETTINGS]
# ... 现有配置 ...

# ============ 对象创建时间截止过滤 ============
# 只校验 Oracle DBA_OBJECTS.CREATED 在此时间之前创建的对象
# 格式: YYYY-MM-DD HH24:MI:SS 或 YYYY-MM-DD (默认 00:00:00)
# 留空或 now 表示使用程序运行时间（即不过滤）
# 示例: 2024-01-15 00:00:00
object_created_cutoff = 

# 对于截止时间之后创建但在 OB 端已存在的对象，处理策略:
#   report   - 作为 EXTRA_RECENT 类型记录到报告（默认）
#   ignore   - 完全忽略，不出现在任何报告中
#   include  - 纳入正常校验流程（等同于不启用此功能）
object_recent_policy = report
```

### 2.2 配置解析逻辑

```python
from datetime import datetime
from typing import Optional

def parse_created_cutoff(value: str) -> Optional[datetime]:
    """
    解析对象创建时间截止配置。
    
    Args:
        value: 配置值，支持以下格式:
            - 空字符串或 'now': 返回 None（使用当前时间）
            - 'YYYY-MM-DD': 解析为当天 00:00:00
            - 'YYYY-MM-DD HH:MI:SS': 精确时间
    
    Returns:
        datetime 对象，或 None 表示使用当前时间
    """
    if not value or value.strip().lower() == 'now':
        return None
    
    value = value.strip()
    
    # 尝试完整格式
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"无法解析时间格式: {value}，期望格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MI:SS")


# 在 load_config() 中新增
cutoff_raw = config.get('SETTINGS', 'object_created_cutoff', fallback='').strip()
try:
    settings['object_created_cutoff'] = parse_created_cutoff(cutoff_raw)
except ValueError as e:
    log.error("配置错误: %s", e)
    sys.exit(1)

settings['object_recent_policy'] = config.get(
    'SETTINGS', 'object_recent_policy', fallback='report'
).strip().lower()

if settings['object_recent_policy'] not in ('report', 'ignore', 'include'):
    log.warning("object_recent_policy 值无效 '%s'，使用默认值 'report'", 
                settings['object_recent_policy'])
    settings['object_recent_policy'] = 'report'
```

---

## 三、数据结构设计

### 3.1 新增 NamedTuple

```python
class RecentObjectInfo(NamedTuple):
    """截止时间之后创建的对象信息"""
    owner: str
    object_name: str
    object_type: str
    created: datetime
    ob_exists: bool          # OB 端是否存在
    ob_status: Optional[str] # OB 端状态 (VALID/INVALID/None)


class CreatedCutoffSummary(NamedTuple):
    """创建时间过滤摘要"""
    cutoff_time: datetime           # 实际使用的截止时间
    cutoff_source: str              # 来源: 'config' 或 'runtime'
    total_source_objects: int       # 源端总对象数
    included_objects: int           # 纳入校验的对象数
    excluded_objects: int           # 排除的对象数（截止时间之后）
    recent_in_ob: int               # 排除对象中已在 OB 存在的数量
    recent_not_in_ob: int           # 排除对象中不在 OB 的数量
```

### 3.2 扩展现有结构

在 `ReportResults` 中新增字段：

```python
# 新增到 tv_results
"recent_objects": List[RecentObjectInfo]  # 截止时间之后的对象列表
"created_cutoff_summary": Optional[CreatedCutoffSummary]  # 过滤摘要
```

---

## 四、核心实现

### 4.1 修改 get_source_objects() 函数

**当前逻辑** (约 4937-5080 行):
```python
def get_source_objects(...) -> Tuple[...]:
    # 从 DBA_OBJECTS 查询对象列表
    sql = """
    SELECT OWNER, OBJECT_NAME, OBJECT_TYPE, STATUS
    FROM DBA_OBJECTS
    WHERE OWNER IN (...)
      AND OBJECT_TYPE IN (...)
    """
```

**修改后**:
```python
def get_source_objects(
    ora_cfg: OraConfig,
    schemas: List[str],
    object_types: Set[str],
    created_cutoff: Optional[datetime] = None,  # 新增参数
    recent_policy: str = 'report'               # 新增参数
) -> Tuple[
    List[Tuple[str, str, str, str]],  # 纳入校验的对象
    List[RecentObjectInfo],            # 截止时间之后的对象（新增返回值）
    ...
]:
    """
    获取源端对象列表，支持按创建时间过滤。
    """
    # 确定实际截止时间
    effective_cutoff = created_cutoff or datetime.now()
    cutoff_str = effective_cutoff.strftime('%Y-%m-%d %H:%M:%S')
    
    # 主查询：截止时间之前的对象
    sql_main = f"""
    SELECT OWNER, OBJECT_NAME, OBJECT_TYPE, STATUS, CREATED
    FROM DBA_OBJECTS
    WHERE OWNER IN ({schemas_in})
      AND OBJECT_TYPE IN ({types_in})
      AND CREATED <= TO_DATE('{cutoff_str}', 'YYYY-MM-DD HH24:MI:SS')
    """
    
    # 如果策略不是 ignore，还需要查询截止时间之后的对象
    recent_objects = []
    if recent_policy != 'ignore':
        sql_recent = f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE, STATUS, CREATED
        FROM DBA_OBJECTS
        WHERE OWNER IN ({schemas_in})
          AND OBJECT_TYPE IN ({types_in})
          AND CREATED > TO_DATE('{cutoff_str}', 'YYYY-MM-DD HH24:MI:SS')
        """
        # 执行查询并收集 recent_objects
    
    return main_objects, recent_objects, ...
```

### 4.2 新增 check_recent_objects_in_ob() 函数

```python
def check_recent_objects_in_ob(
    recent_objects: List[Tuple[str, str, str, str, datetime]],
    ob_meta: ObMetadata,
    full_object_mapping: FullObjectMapping
) -> List[RecentObjectInfo]:
    """
    检查截止时间之后创建的对象在 OB 端是否存在。
    
    Args:
        recent_objects: 源端截止时间之后的对象列表
        ob_meta: OB 元数据
        full_object_mapping: 对象映射关系
    
    Returns:
        带有 OB 存在状态的对象信息列表
    """
    result = []
    
    for owner, obj_name, obj_type, status, created in recent_objects:
        src_full = f"{owner}.{obj_name}"
        
        # 通过映射查找目标对象
        target_full = find_mapped_target_any_type(
            src_full, obj_type, full_object_mapping
        )
        
        if target_full:
            tgt_schema, tgt_name = target_full.split('.', 1)
        else:
            # 无映射规则时，使用默认推导
            tgt_schema = owner  # 或根据 schema mapping 推导
            tgt_name = obj_name
        
        # 检查 OB 端是否存在
        ob_exists = False
        ob_status = None
        
        if obj_type == 'TABLE':
            ob_exists = (tgt_schema, tgt_name) in ob_meta.tables
        elif obj_type == 'VIEW':
            ob_exists = (tgt_schema, tgt_name) in ob_meta.views
            if ob_exists:
                ob_status = ob_meta.views.get((tgt_schema, tgt_name), {}).get('status')
        elif obj_type == 'INDEX':
            ob_exists = (tgt_schema, tgt_name) in ob_meta.indexes
        elif obj_type == 'SEQUENCE':
            ob_exists = (tgt_schema, tgt_name) in ob_meta.sequences
        elif obj_type == 'SYNONYM':
            ob_exists = (tgt_schema, tgt_name) in ob_meta.synonyms
        # ... 其他类型
        
        result.append(RecentObjectInfo(
            owner=owner,
            object_name=obj_name,
            object_type=obj_type,
            created=created,
            ob_exists=ob_exists,
            ob_status=ob_status
        ))
    
    return result
```

### 4.3 新增 build_created_cutoff_summary() 函数

```python
def build_created_cutoff_summary(
    cutoff_time: datetime,
    cutoff_source: str,
    total_source: int,
    included: int,
    recent_objects: List[RecentObjectInfo]
) -> CreatedCutoffSummary:
    """
    构建创建时间过滤摘要。
    """
    excluded = len(recent_objects)
    recent_in_ob = sum(1 for obj in recent_objects if obj.ob_exists)
    recent_not_in_ob = excluded - recent_in_ob
    
    return CreatedCutoffSummary(
        cutoff_time=cutoff_time,
        cutoff_source=cutoff_source,
        total_source_objects=total_source,
        included_objects=included,
        excluded_objects=excluded,
        recent_in_ob=recent_in_ob,
        recent_not_in_ob=recent_not_in_ob
    )
```

### 4.4 修改主流程调用

在 `main()` 或 `run_comparison()` 中：

```python
# 解析截止时间
created_cutoff = settings.get('object_created_cutoff')
recent_policy = settings.get('object_recent_policy', 'report')

# 确定实际截止时间和来源
if created_cutoff is None:
    effective_cutoff = datetime.now()
    cutoff_source = 'runtime'
else:
    effective_cutoff = created_cutoff
    cutoff_source = 'config'

log.info(
    "对象创建时间截止过滤: %s (来源: %s, 策略: %s)",
    effective_cutoff.strftime('%Y-%m-%d %H:%M:%S'),
    cutoff_source,
    recent_policy
)

# 获取源端对象（带时间过滤）
source_objects, recent_source_objects, ... = get_source_objects(
    ora_cfg,
    schemas,
    object_types,
    created_cutoff=effective_cutoff,
    recent_policy=recent_policy
)

# 如果策略是 include，将 recent 对象合并到主列表
if recent_policy == 'include':
    source_objects.extend(recent_source_objects)
    recent_source_objects = []

# ... 继续现有的校验流程 ...

# 在 OB 元数据加载后，检查 recent 对象的 OB 状态
recent_objects_info = []
if recent_source_objects and recent_policy == 'report':
    recent_objects_info = check_recent_objects_in_ob(
        recent_source_objects,
        ob_meta,
        full_object_mapping
    )

# 构建摘要
cutoff_summary = build_created_cutoff_summary(
    effective_cutoff,
    cutoff_source,
    len(source_objects) + len(recent_source_objects),
    len(source_objects),
    recent_objects_info
)

# 将结果加入报告
tv_results['recent_objects'] = recent_objects_info
tv_results['created_cutoff_summary'] = cutoff_summary
```

---

## 五、报告输出

### 5.1 控制台摘要

在报告摘要中新增一个区块：

```
┌─────────────────────────────────────────────────────────────────┐
│                    对象创建时间过滤摘要                          │
├─────────────────────────────────────────────────────────────────┤
│  截止时间      : 2024-01-15 00:00:00                            │
│  配置来源      : config (显式配置)                               │
│  源端总对象    : 5,234                                          │
│  纳入校验      : 5,100                                          │
│  排除 (新创建) : 134                                            │
│    - 已在 OB   : 120 (EXTRA_RECENT)                             │
│    - 未在 OB   : 14  (待迁移)                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 详细报告文件

新增文件 `recent_objects_{timestamp}.txt`:

```
# 截止时间之后创建的对象 (共 134 个)
# 截止时间: 2024-01-15 00:00:00
# 生成时间: 2024-02-03 14:00:00

## 已在 OB 端存在 (120 个) - EXTRA_RECENT
OWNER          OBJECT_NAME              TYPE       CREATED              OB_STATUS
-------------- ------------------------ ---------- -------------------- ----------
APPUSER        NEW_CONFIG_TABLE         TABLE      2024-01-20 10:30:00  -
APPUSER        V_NEW_SUMMARY            VIEW       2024-01-22 14:15:00  VALID
APPUSER        IDX_NEW_CONFIG_01        INDEX      2024-01-20 10:31:00  -
...

## 未在 OB 端存在 (14 个) - 待迁移
OWNER          OBJECT_NAME              TYPE       CREATED
-------------- ------------------------ ---------- --------------------
APPUSER        TEMP_MIGRATION_TAB       TABLE      2024-02-01 09:00:00
APPUSER        V_TEMP_REPORT            VIEW       2024-02-02 11:30:00
...
```

### 5.3 JSON 格式（用于自动化）

如果启用了报告存库或 JSON 导出：

```json
{
  "created_cutoff_summary": {
    "cutoff_time": "2024-01-15T00:00:00",
    "cutoff_source": "config",
    "total_source_objects": 5234,
    "included_objects": 5100,
    "excluded_objects": 134,
    "recent_in_ob": 120,
    "recent_not_in_ob": 14
  },
  "recent_objects": [
    {
      "owner": "APPUSER",
      "object_name": "NEW_CONFIG_TABLE",
      "object_type": "TABLE",
      "created": "2024-01-20T10:30:00",
      "ob_exists": true,
      "ob_status": null
    },
    ...
  ]
}
```

---

## 六、全局影响分析 (关键章节)

> ⚠️ **重要**: cutoff 功能会影响程序的多个核心模块，需要仔细评估每个影响点。

### 6.1 影响矩阵总览

| 模块 | 影响程度 | 处理策略 | 备注 |
|-----|---------|---------|------|
| master_list 构建 | **高** | 前置过滤 | 核心变更点 |
| full_object_mapping | **高** | 保留映射 | recent 对象仍需映射 |
| 依赖图 (DBA_DEPENDENCIES) | **高** | 边界处理 | 跨 cutoff 依赖 |
| 拓扑排序 | **高** | 特殊处理 | 视图依赖链 |
| 扩展对象 (INDEX等) | **中** | 级联过滤 | 跟随主对象 |
| DDL 获取 | **中** | 跳过 recent | 减少开销 |
| 黑名单传播 | **中** | 独立处理 | 互不影响 |
| 权限处理 | **中** | 条件生成 | 已存在才生成 |
| 注释比对 | **低** | 跟随主对象 | 自动排除 |
| 可用性检查 | **低** | 跳过 recent | 不校验 |

---

### 6.2 master_list 构建影响

**当前流程**:
```
get_source_objects() → source_objects
        ↓
build_master_list() → master_list
        ↓
后续所有校验基于 master_list
```

**修改后流程**:
```
get_source_objects(cutoff) → included_objects + recent_objects
        ↓
build_master_list(included_objects) → master_list  (仅包含 cutoff 前对象)
        ↓
后续校验基于过滤后的 master_list
        ↓
recent_objects 单独处理，检查 OB 存在性
```

**关键点**:
- cutoff 过滤必须在 master_list 构建**之前**完成
- master_list 构建逻辑本身不需要修改
- recent_objects 需要单独维护，不进入主流程

---

### 6.3 full_object_mapping 影响

**问题**: remap 文件中可能包含 recent 对象的映射规则

**场景分析**:
```
remap.txt 内容:
ORACLE_SCHEMA.NEW_TABLE -> OB_SCHEMA.NEW_TABLE

如果 NEW_TABLE 是 recent 对象:
- 该映射规则是否需要加载？
- 如果 recent 对象已在 OB 存在，如何确定目标？
```

**处理策略**:
```python
# 策略: 映射规则照常加载，不受 cutoff 影响
# 原因: 
#   1. recent 对象检查 OB 存在性时需要映射
#   2. 依赖解析时需要知道 recent 对象的目标
#   3. 如果 recent_policy=include，需要完整映射

full_object_mapping = build_object_mapping(
    remap_rules,
    source_objects + recent_objects  # 完整源对象列表
)
```

---

### 6.4 依赖图影响 (DBA_DEPENDENCIES)

**问题**: 依赖关系可能跨越 cutoff 边界

**场景分析**:
```
场景1: 校验对象依赖 recent 对象
  VIEW_A (cutoff前) → TABLE_B (cutoff后)
  
场景2: recent 对象依赖校验对象  
  VIEW_X (cutoff后) → TABLE_Y (cutoff前)
  
场景3: 链式依赖
  VIEW_A (前) → VIEW_B (后) → TABLE_C (前)
```

**处理策略**:
```python
# 新增依赖分类
class DependencyClassification(NamedTuple):
    normal: List[Tuple]          # 两端都在校验范围内
    depends_on_recent: List[Tuple]  # 校验对象依赖 recent 对象
    recent_depends: List[Tuple]     # recent 对象依赖校验对象 (仅记录)

def classify_dependencies(
    dependencies: Set[Tuple],
    included_objects: Set[str],
    recent_objects: Set[str]
) -> DependencyClassification:
    normal = []
    depends_on_recent = []
    recent_depends = []
    
    for dep_obj, dep_type, ref_obj, ref_type in dependencies:
        dep_is_included = dep_obj in included_objects
        ref_is_recent = ref_obj in recent_objects
        
        if dep_is_included and not ref_is_recent:
            normal.append((dep_obj, dep_type, ref_obj, ref_type))
        elif dep_is_included and ref_is_recent:
            depends_on_recent.append((dep_obj, dep_type, ref_obj, ref_type))
        elif dep_obj in recent_objects:
            recent_depends.append((dep_obj, dep_type, ref_obj, ref_type))
    
    return DependencyClassification(normal, depends_on_recent, recent_depends)
```

**报告输出**:
```
依赖分析:
  正常依赖: 1,234 对
  依赖 recent 对象: 15 对 ⚠️
    - APPUSER.V_SUMMARY 依赖 APPUSER.NEW_CONFIG (recent)
    - ...
```

---

### 6.5 拓扑排序影响

**问题**: 视图拓扑排序基于依赖关系，recent 对象会打断依赖链

**场景**:
```
正常情况:
  V1 → V2 → V3 → T1
  排序: T1, V3, V2, V1

cutoff 打断:
  V1 (前) → V2 (后) → V3 (前) → T1 (前)
  V2 是 recent，不在排序范围内
  
问题: V1 依赖 V2，但 V2 不生成 DDL，V1 会创建失败
```

**处理策略**:
```python
def build_view_dependency_graph_with_cutoff(
    view_dependencies: Dict,
    included_views: Set[str],
    recent_views: Set[str]
) -> Tuple[Dict, List[str]]:
    """
    构建视图依赖图，处理 cutoff 边界。
    
    Returns:
        (filtered_graph, blocked_views)
        - filtered_graph: 可用的依赖图
        - blocked_views: 因依赖 recent view 而无法创建的视图
    """
    blocked_views = []
    
    for view in included_views:
        deps = view_dependencies.get(view, set())
        recent_deps = deps & recent_views
        
        if recent_deps:
            # 检查 recent 依赖是否在 OB 已存在
            unresolved = [d for d in recent_deps if not ob_exists(d)]
            if unresolved:
                blocked_views.append((view, unresolved))
    
    return filtered_graph, blocked_views
```

**报告输出**:
```
视图创建受阻 (依赖 recent 对象且 OB 不存在): 3 个
  APPUSER.V_REPORT 依赖:
    - APPUSER.V_NEW_DATA (recent, OB不存在) ❌
  建议: 等待 V_NEW_DATA 迁移完成后重新运行
```

---

### 6.6 扩展对象 (INDEX/CONSTRAINT/TRIGGER/SEQUENCE) 影响

**问题**: 扩展对象是否也按 CREATED 时间过滤？

**分析**:
```
选项1: 扩展对象独立过滤
  - INDEX 有自己的 CREATED 时间
  - 可能出现: TABLE (前) 的 INDEX (后)
  - 复杂度高

选项2: 扩展对象跟随主对象 (推荐)
  - TABLE 在校验范围内 → 其所有 INDEX 都校验
  - TABLE 是 recent → 其所有 INDEX 都排除
  - 逻辑简单，符合直觉
```

**处理策略**:
```python
# 扩展对象过滤基于主对象
def filter_extra_objects_by_master(
    extra_results: ExtraCheckResults,
    included_tables: Set[Tuple[str, str]],
    recent_tables: Set[Tuple[str, str]]
) -> Tuple[ExtraCheckResults, ExtraCheckResults]:
    """
    将扩展对象按主对象的 cutoff 状态分类。
    """
    included_extra = filter_by_table(extra_results, included_tables)
    recent_extra = filter_by_table(extra_results, recent_tables)
    return included_extra, recent_extra
```

**注意**: SEQUENCE 是独立对象，需要单独按 CREATED 过滤

---

### 6.7 DDL 获取影响

**问题**: 是否需要获取 recent 对象的 DDL？

**分析**:
```
不需要获取:
  - 不生成 fixup 脚本
  - 减少 dbcat/DBMS_METADATA 调用
  - 提升性能

需要获取:
  - 如果 recent_policy = include
  - 如果需要分析 recent 对象的依赖详情
```

**处理策略**:
```python
# DDL 获取跳过 recent 对象
def get_ddl_batch(objects, recent_objects, recent_policy):
    if recent_policy == 'include':
        return fetch_ddl(objects + recent_objects)
    else:
        return fetch_ddl(objects)  # 仅获取 included 对象
```

---

### 6.8 黑名单传播影响

**场景**:
```
TABLE_A (黑名单) ← VIEW_B (recent) ← VIEW_C (included)

问题: VIEW_C 依赖 VIEW_B，VIEW_B 依赖黑名单表
      但 VIEW_B 是 recent，黑名单传播逻辑是否能正确处理？
```

**处理策略**:
```
1. 黑名单传播在 cutoff 过滤之前执行
2. recent 对象也参与黑名单传播计算
3. 如果 included 对象因 recent 对象而间接依赖黑名单，需要记录

流程:
  load_blacklist() 
  → propagate_blacklist(all_objects)  # 包含 recent
  → apply_cutoff_filter()
  → 检查 included 对象的黑名单状态
```

---

### 6.9 权限处理影响

**问题**: recent 对象的权限如何处理？

**场景**:
```
recent_policy = report 时:
  - recent TABLE 在 OB 存在
  - 源端有 GRANT SELECT ON recent_table TO user1
  - 是否生成授权语句？
```

**处理策略**:
```python
# 新增配置项
# grant_recent_existing = true  # 是否为已存在的 recent 对象生成授权

def build_grant_plan_with_cutoff(
    grant_plan: GrantPlan,
    recent_objects: List[RecentObjectInfo],
    grant_recent_existing: bool
) -> GrantPlan:
    if not grant_recent_existing:
        return filter_out_recent(grant_plan, recent_objects)
    
    # 仅保留 OB 已存在的 recent 对象授权
    recent_existing = [o for o in recent_objects if o.ob_exists]
    return filter_to_existing(grant_plan, recent_existing)
```

---

### 6.10 注释比对影响

**处理**: 自动跟随主对象，recent 表的注释不比对

```python
# check_comments() 基于 master_list
# master_list 已排除 recent 对象
# 无需额外修改
```

---

### 6.11 对象可用性检查影响

**处理**: recent VIEW/SYNONYM 不进行可用性检查

```python
# check_object_usability() 基于 master_list
# master_list 已排除 recent 对象
# 无需额外修改
```

---

### 6.12 PUBLIC SYNONYM 影响

**场景**:
```
recent VIEW 引用了 PUBLIC SYNONYM
PUBLIC SYNONYM 指向 included TABLE

问题: PUBLIC SYNONYM 的依赖解析是否受影响？
```

**处理**: PUBLIC SYNONYM 本身也有 CREATED 时间
- recent PUBLIC SYNONYM 不纳入校验
- included 对象引用的 PUBLIC SYNONYM 正常解析

---

### 6.13 与现有功能交互总结

#### 与黑名单的交互

```
处理顺序:
1. 加载黑名单 (全量)
2. 传播黑名单 (全量对象参与)
3. 应用 cutoff 过滤
4. 校验时使用已传播的黑名单状态

关键: 黑名单传播在 cutoff 过滤之前完成
```

#### 与 fixup 脚本生成的交互

```
规则:
- recent 对象不生成 fixup 脚本
- 依赖 recent 对象的 included 对象，标记为 blocked
- blocked 对象生成带注释的 fixup 脚本，说明依赖未就绪
```

#### 与依赖检查的交互

```
规则:
- 依赖检查分类: normal / depends_on_recent / recent_depends
- depends_on_recent 记录为警告，不计入 missing
```

#### 与授权生成的交互

```
规则:
- 默认不生成 recent 对象的授权
- 可配置: grant_recent_existing = true 时生成已存在对象的授权
```

---

## 6.14 处理流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CUTOFF 过滤全局处理流程                                │
└─────────────────────────────────────────────────────────────────────────────┘

1. 配置解析阶段
   ┌──────────────────┐
   │ parse_created_   │
   │ cutoff()         │──→ effective_cutoff (datetime)
   └──────────────────┘

2. 源端对象采集阶段
   ┌──────────────────┐     ┌─────────────────────────────────┐
   │ get_source_      │     │  DBA_OBJECTS                    │
   │ objects()        │────→│  WHERE CREATED <= cutoff        │
   └──────────────────┘     │  ────────────────────────────── │
          │                 │  WHERE CREATED > cutoff (recent)│
          │                 └─────────────────────────────────┘
          ↓
   ┌──────────────────┬──────────────────┐
   │ included_objects │ recent_objects   │
   │ (纳入校验)        │ (排除校验)        │
   └──────────────────┴──────────────────┘

3. 映射构建阶段 (不受 cutoff 影响)
   ┌──────────────────┐
   │ build_object_    │────→ full_object_mapping
   │ mapping()        │      (包含 included + recent)
   └──────────────────┘

4. 黑名单处理阶段 (cutoff 前执行)
   ┌──────────────────┐
   │ propagate_       │────→ blacklist_status
   │ blacklist()      │      (全量对象参与传播)
   └──────────────────┘

5. master_list 构建阶段
   ┌──────────────────┐
   │ build_master_    │────→ master_list
   │ list()           │      (仅 included_objects)
   └──────────────────┘

6. 依赖分析阶段
   ┌──────────────────┐     ┌─────────────────────────────────┐
   │ classify_        │     │ normal        → 正常依赖检查    │
   │ dependencies()   │────→│ depends_on_recent → 警告        │
   └──────────────────┘     │ recent_depends → 仅记录         │
                            └─────────────────────────────────┘

7. 拓扑排序阶段
   ┌──────────────────┐     ┌─────────────────────────────────┐
   │ build_view_      │     │ sorted_views  → 正常排序        │
   │ dependency_graph │────→│ blocked_views → 依赖 recent 且  │
   │ _with_cutoff()   │     │               OB 不存在的视图   │
   └──────────────────┘     └─────────────────────────────────┘

8. 主对象校验阶段
   ┌──────────────────┐
   │ check_primary_   │────→ tv_results (基于 master_list)
   │ objects()        │
   └──────────────────┘

9. 扩展对象校验阶段
   ┌──────────────────┐
   │ check_extra_     │────→ extra_results
   │ objects()        │      (仅 included 表的扩展对象)
   └──────────────────┘

10. DDL 获取阶段
    ┌──────────────────┐
    │ fetch_ddl()      │────→ DDL (仅 included 对象)
    │                  │      recent 对象跳过
    └──────────────────┘

11. recent 对象处理阶段
    ┌──────────────────┐     ┌─────────────────────────────────┐
    │ check_recent_    │     │ recent_in_ob    → EXTRA_RECENT  │
    │ objects_in_ob()  │────→│ recent_not_in_ob → 待迁移        │
    └──────────────────┘     └─────────────────────────────────┘

12. 报告生成阶段
    ┌──────────────────┐
    │ generate_report()│────→ 包含 cutoff_summary 和 recent_objects
    └──────────────────┘
```

---

## 6.15 关键代码修改点清单

| 序号 | 函数/模块 | 修改类型 | 说明 |
|-----|---------|---------|------|
| 1 | `load_config()` | 新增 | 解析 cutoff 配置 |
| 2 | `get_source_objects()` | 修改 | 增加 cutoff 参数，返回 recent 列表 |
| 3 | `build_object_mapping()` | 无修改 | 传入完整对象列表即可 |
| 4 | `propagate_blacklist()` | 无修改 | 传入完整对象列表即可 |
| 5 | `build_master_list()` | 无修改 | 仅传入 included 对象 |
| 6 | `load_oracle_dependencies()` | 新增 | `classify_dependencies()` |
| 7 | `build_view_dependency_graph()` | 修改 | 处理 blocked_views |
| 8 | `check_extra_objects()` | 修改 | 按 included 表过滤 |
| 9 | `generate_fixup_scripts()` | 修改 | 跳过 recent，标记 blocked |
| 10 | `build_grant_plan()` | 修改 | 支持 `grant_recent_existing` |
| 11 | `print_final_report()` | 修改 | 输出 cutoff_summary |
| 12 | 新增 | `check_recent_objects_in_ob()` |
| 13 | 新增 | `build_created_cutoff_summary()` |
| 14 | 新增 | `export_recent_objects_report()` |

---

## 七、边界条件处理

### 7.1 时区问题

```python
# Oracle CREATED 字段使用数据库时区
# 需要确保比较时时区一致

# 建议: 在查询中使用 Oracle 的 SYSTIMESTAMP 作为基准
sql = f"""
SELECT ... 
WHERE CREATED <= TO_TIMESTAMP('{cutoff_str}', 'YYYY-MM-DD HH24:MI:SS')
"""

# 或者在 Python 端转换为 UTC
```

### 7.2 跨天边界

```python
# 如果只配置日期，默认使用当天 00:00:00
# 即 2024-01-15 表示 2024-01-15 00:00:00
# 2024-01-15 当天创建的对象会被排除

# 如果需要包含当天对象，应配置为:
# object_created_cutoff = 2024-01-15 23:59:59
# 或
# object_created_cutoff = 2024-01-16 00:00:00
```

### 7.3 对象创建时间为空

```python
# DBA_OBJECTS.CREATED 通常不为空
# 但为保险起见，将 NULL 视为"无限早"，纳入校验
sql = """
WHERE (CREATED <= TO_DATE(...) OR CREATED IS NULL)
"""
```

### 7.4 分区对象

```python
# 分区表的 CREATED 时间是表创建时间，不是分区创建时间
# 这符合预期：按表级别过滤，不按分区级别
```

---

## 八、性能影响分析

### 8.1 查询影响

| 场景 | 影响 |
|-----|------|
| 无过滤配置 | 无额外开销 |
| 配置截止时间 | +1 次 DBA_OBJECTS 查询（recent 对象） |
| recent_policy=ignore | 无 recent 查询 |

### 8.2 内存影响

```
recent_objects 列表通常较小（最近创建的对象数量有限）
预估: 1000 个 recent 对象约占用 < 1MB 内存
```

### 8.3 优化建议

```sql
-- 确保 DBA_OBJECTS 的 CREATED 列有索引（Oracle 默认有）
-- 如果 recent 对象特别多，考虑分页查询
```

---

## 九、测试计划

### 9.1 单元测试

| 测试项 | 测试内容 |
|-------|---------|
| `test_parse_cutoff_formats` | 各种时间格式解析 |
| `test_parse_cutoff_empty` | 空值和 'now' 处理 |
| `test_parse_cutoff_invalid` | 无效格式报错 |
| `test_filter_by_created` | 按时间过滤逻辑 |
| `test_recent_policy_report` | report 策略 |
| `test_recent_policy_ignore` | ignore 策略 |
| `test_recent_policy_include` | include 策略 |

### 9.2 集成测试

| 测试场景 | 验证点 |
|---------|-------|
| 默认配置 | 所有对象纳入校验 |
| 历史时间点 | 仅历史对象校验 |
| recent 在 OB 存在 | 正确标记 EXTRA_RECENT |
| recent 不在 OB | 正确标记待迁移 |
| 依赖 recent 对象 | 依赖检查正确处理 |

---

## 十、实施计划

### 阶段一：基础实现 (2 天)

1. 添加配置项解析
2. 修改 `get_source_objects()` 支持时间过滤
3. 实现 `check_recent_objects_in_ob()`
4. 实现 `build_created_cutoff_summary()`

### 阶段二：报告集成 (1 天)

1. 修改控制台报告输出
2. 新增 recent_objects 详细报告文件
3. 更新 JSON/数据库报告格式

### 阶段三：边界处理 (1 天)

1. 依赖检查交互处理
2. fixup 脚本生成交互处理
3. 授权生成交互处理

### 阶段四：测试验证 (1 天)

1. 单元测试
2. 集成测试
3. 边界条件测试

---

## 十一、配置示例

### 示例 1: 存量校验（割接场景）

```ini
[SETTINGS]
# 只校验 2024-01-15 00:00:00 之前创建的对象
object_created_cutoff = 2024-01-15 00:00:00
# 新对象如果在 OB 存在，记录到报告
object_recent_policy = report
```

### 示例 2: 全量校验（默认行为）

```ini
[SETTINGS]
# 留空或不配置，等同于当前时间，校验所有对象
object_created_cutoff = 
```

### 示例 3: 严格存量校验（忽略新对象）

```ini
[SETTINGS]
object_created_cutoff = 2024-01-15
# 完全忽略新对象，不出现在报告中
object_recent_policy = ignore
```

### 示例 4: 宽松校验（新对象也纳入）

```ini
[SETTINGS]
object_created_cutoff = 2024-01-15
# 新对象也纳入正常校验流程
object_recent_policy = include
```

---

## 十二、FAQ

**Q1: 为什么默认使用当前时间而不是无限大？**

A: 使用当前时间作为默认值确保：
- 与现有行为 100% 兼容（所有对象都在截止时间之前）
- 无需特殊处理"无过滤"的逻辑分支
- 报告中可以清晰显示使用的截止时间

**Q2: 如果源端和目标端时区不同怎么办？**

A: 程序使用 Oracle 的 `CREATED` 字段进行比较，时区以 Oracle 数据库为准。建议用户在配置截止时间时明确使用源端数据库的时区。

**Q3: recent 对象会影响缺失/存在统计吗？**

A: 不会。recent 对象有独立的统计区块，不计入 missing/ok 等主要统计。

**Q4: 这个功能会影响 SEQUENCE 等无 CREATED 属性的对象吗？**

A: SEQUENCE 在 `DBA_OBJECTS` 中也有 `CREATED` 字段，会正常过滤。但需要注意 SEQUENCE 的 `LAST_NUMBER` 等运行时属性不受创建时间影响。

---

**提案状态**: 待评审  
**预计工作量**: 5 人天
