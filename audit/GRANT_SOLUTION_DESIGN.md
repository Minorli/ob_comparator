# 跨 Schema 授权完整性方案设计

**项目**: OceanBase Comparator Toolkit  
**版本**: V0.9.8  
**设计日期**: 2026-01-20  
**设计目标**: 解决跨 Schema 授权的完整性、可追溯性和正确性问题

---

## 📋 现状分析

### 当前实现（基于代码审查）

#### 1. 数据结构设计 ✅
```python
# lines 523-537
class ObjectGrantEntry(NamedTuple):
    privilege: str
    object_full: str
    grantable: bool  # ✅ 支持 WITH GRANT OPTION

class SystemGrantEntry(NamedTuple):
    privilege: str
    admin_option: bool  # ✅ 支持 WITH ADMIN OPTION

class RoleGrantEntry(NamedTuple):
    role: str
    admin_option: bool  # ✅ 支持 WITH ADMIN OPTION
```

**优点**：
- ✅ 支持 `WITH GRANT OPTION`（对象权限）
- ✅ 支持 `WITH ADMIN OPTION`（系统权限和角色）
- ✅ 区分三类权限（对象/系统/角色）

**缺陷**：
- ❌ 缺少授权者（GRANTOR）信息
- ❌ 无法追溯授权链路
- ❌ 无法区分直接授权和角色继承授权

---

#### 2. 角色递归展开 ✅
```python
# lines 6280-6321
def load_oracle_role_privileges(ora_conn, base_grantees):
    """
    读取 DBA_ROLE_PRIVS，并递归展开角色授予链路。
    ✅ 支持角色嵌套
    """
    while pending:
        # 递归查询角色授予关系
        # 发现新角色继续展开
```

**优点**：
- ✅ 正确处理角色嵌套（A → B → C）
- ✅ 避免循环引用

**缺陷**：
- ❌ 未记录授权层级
- ❌ 无法区分哪些权限来自哪个角色
- ❌ 生成的 GRANT 语句可能重复授予已通过角色获得的权限

---

#### 3. 权限过滤机制 ✅
```python
# lines 8259-8338
def filter_missing_grant_entries(
    object_grants_by_grantee,
    sys_privs_by_grantee,
    role_privs_by_grantee,
    ob_catalog
):
    """
    基于 OB 权限目录过滤已存在的授权
    ✅ 支持 grantable/admin_option 区分
    """
```

**优点**：
- ✅ 区分基本权限和可授权权限
- ✅ 避免重复授权

**缺陷**：
- ❌ 未检查 OB 端授权的完整性（是否包含 WITH GRANT OPTION）
- ❌ 如果 OB 端有 `GRANT SELECT` 但无 `WITH GRANT OPTION`，而源端需要，会被误判为已存在

---

#### 4. GRANT 语句生成 ✅
```python
# lines 15954-15983
def format_object_grant(grantee, entry):
    stmt = f"GRANT {entry.privilege} ON {entry.object_full} TO {grantee}"
    if entry.grantable:
        stmt += " WITH GRANT OPTION"  # ✅
    return stmt + ";"
```

**优点**：
- ✅ 正确生成 WITH GRANT OPTION
- ✅ 支持权限合并（多个权限合并为一条语句）

**缺陷**：
- ❌ 未生成授权顺序（依赖关系）
- ❌ 缺少授权验证脚本
- ❌ 无回滚/撤销脚本

---

### 识别的关键缺陷

| 缺陷编号 | 描述 | 严重性 | 影响 |
|---------|------|--------|------|
| **D1** | 缺少 GRANTOR 信息，无法追溯授权来源 | P1 | 审计困难，授权链断裂 |
| **D2** | 角色继承权限与直接授权混淆 | P1 | 可能重复授权，脚本冗余 |
| **D3** | WITH GRANT OPTION 的增量检测不准确 | P0 | 授权不完整，依赖对象无法创建 |
| **D4** | 缺少授权依赖排序 | P1 | 脚本执行顺序错误，授权失败 |
| **D5** | 无授权验证和回滚机制 | P2 | 运维风险高 |
| **D6** | 跨 Schema 依赖未完整覆盖 | P1 | 外键、视图等对象创建失败 |

---

## 🎯 完善方案设计

### 方案 1: 增强授权元数据收集

#### 目标
完整收集授权链路信息，包括 GRANTOR、授权层级、权限来源。

#### 设计

**新增数据结构**:
```python
class EnhancedObjectGrant(NamedTuple):
    """增强的对象权限记录"""
    grantee: str
    privilege: str
    object_owner: str
    object_name: str
    object_type: str
    grantable: bool
    grantor: str          # 🆕 授权者
    hierarchy: str        # 🆕 授权路径，如 "SYS→ROLE_DBA→SCHEMA_A"
    grant_source: str     # 🆕 "DIRECT" 或 "ROLE:ROLE_NAME"
    
class GrantDependency(NamedTuple):
    """授权依赖关系"""
    prerequisite_grantee: str  # 前置被授权者
    prerequisite_privilege: str
    prerequisite_object: str
    dependent_grantee: str     # 依赖的被授权者
    reason: str                # 依赖原因，如 "需要授权给其他用户"
```

**增强查询**:
```sql
-- 收集完整授权信息（含 GRANTOR）
SELECT 
    GRANTEE, 
    PRIVILEGE, 
    OWNER, 
    TABLE_NAME, 
    TYPE,
    GRANTABLE,
    GRANTOR,              -- 🆕 授权者
    HIERARCHY             -- 🆕 授权层级
FROM DBA_TAB_PRIVS
WHERE GRANTEE IN (...)
ORDER BY 
    -- 按授权层级排序，确保先创建授权者的权限
    CASE 
        WHEN GRANTOR = OWNER THEN 1  -- 对象所有者授权
        WHEN GRANTABLE = 'YES' THEN 2  -- 可授权权限
        ELSE 3
    END;
```

---

### 方案 2: 授权依赖拓扑排序

#### 目标
确保授权脚本按正确顺序执行，避免 "授权者尚无权限" 错误。

#### 设计

**依赖图构建**:
```python
def build_grant_dependency_graph(
    object_grants: List[EnhancedObjectGrant],
    sys_grants: List[SystemGrantEntry],
    role_grants: List[RoleGrantEntry]
) -> Dict[str, Set[str]]:
    """
    构建授权依赖图
    
    返回: {grantee: set(dependencies)}
    
    示例:
        如果 USER_B 需要授权给 USER_C，但权限来自 USER_A:
        {"USER_C": {"USER_B"}, "USER_B": {"USER_A"}}
    """
    graph: Dict[str, Set[str]] = defaultdict(set)
    
    for grant in object_grants:
        if grant.grantable and grant.grantor != grant.object_owner:
            # 如果是二次授权，需要先确保授权者有 WITH GRANT OPTION
            graph[grant.grantee].add(grant.grantor)
    
    for role_grant in role_grants:
        if role_grant.admin_option:
            # 角色授权需要先创建角色
            graph[role_grant.grantee].add(f"ROLE:{role_grant.role}")
    
    return graph

def topological_sort_grants(
    grants_by_grantee: Dict[str, List[str]],
    dependency_graph: Dict[str, Set[str]]
) -> List[Tuple[str, List[str]]]:
    """
    对授权语句进行拓扑排序
    
    返回: [(grantee, [grant_statements])] 按依赖顺序排列
    """
    from collections import deque
    
    in_degree = {grantee: 0 for grantee in grants_by_grantee}
    
    for grantee, deps in dependency_graph.items():
        if grantee in in_degree:
            in_degree[grantee] = len(deps & set(grants_by_grantee.keys()))
    
    queue = deque([g for g, d in in_degree.items() if d == 0])
    sorted_grantees = []
    
    while queue:
        current = queue.popleft()
        sorted_grantees.append(current)
        
        for grantee, deps in dependency_graph.items():
            if current in deps:
                in_degree[grantee] -= 1
                if in_degree[grantee] == 0:
                    queue.append(grantee)
    
    # 检测循环依赖
    if len(sorted_grantees) < len(grants_by_grantee):
        circular = [g for g, d in in_degree.items() if d > 0]
        log.warning(f"[GRANT] 发现 {len(circular)} 个循环依赖的授权，手工处理: {circular}")
    
    return [(g, grants_by_grantee[g]) for g in sorted_grantees]
```

---

### 方案 3: WITH GRANT OPTION 增量检测

#### 目标
精确检测 OB 端权限是否包含 WITH GRANT OPTION，避免漏授权。

#### 设计

**增强过滤逻辑**:
```python
def filter_missing_grant_entries_enhanced(
    object_grants_by_grantee: Dict[str, Set[ObjectGrantEntry]],
    ob_catalog: Optional[ObGrantCatalog]
) -> Dict[str, Set[ObjectGrantEntry]]:
    """
    增强版权限过滤，精确检测 WITH GRANT OPTION
    """
    if ob_catalog is None:
        return object_grants_by_grantee
    
    miss_obj: Dict[str, Set[ObjectGrantEntry]] = defaultdict(set)
    
    obj_basic = ob_catalog.object_privs           # 基本权限集合
    obj_grantable = ob_catalog.object_privs_grantable  # WITH GRANT OPTION 集合
    
    for grantee, entries in object_grants_by_grantee.items():
        g_u = grantee.upper()
        for entry in entries:
            priv_u = entry.privilege.upper()
            obj_u = entry.object_full.upper()
            key = (g_u, priv_u, obj_u)
            
            # 🆕 精确检测逻辑
            if entry.grantable:
                # 需要 WITH GRANT OPTION
                if key not in obj_grantable:
                    # OB 端要么没有该权限，要么只有基本权限
                    miss_obj[g_u].add(entry)
                    if key in obj_basic:
                        log.info(
                            f"[GRANT] {g_u} 已有 {priv_u} ON {obj_u}，但缺少 WITH GRANT OPTION，需补授权"
                        )
            else:
                # 只需要基本权限
                if key not in obj_basic and key not in obj_grantable:
                    miss_obj[g_u].add(entry)
    
    return miss_obj
```

**补授权策略**:
```python
def generate_upgrade_grant_statements(
    existing_grants: Set[Tuple[str, str, str]],  # (grantee, priv, obj)
    required_grantable: Set[Tuple[str, str, str]]
) -> List[str]:
    """
    为已有权限补充 WITH GRANT OPTION
    
    示例:
        已有: GRANT SELECT ON TABLE1 TO USER_A;
        需要: GRANT SELECT ON TABLE1 TO USER_A WITH GRANT OPTION;
        生成: GRANT SELECT ON TABLE1 TO USER_A WITH GRANT OPTION;  -- 补授权
    """
    upgrade_statements = []
    
    for grantee, priv, obj in required_grantable:
        if (grantee, priv, obj) in existing_grants:
            # 已有基本权限，升级为 WITH GRANT OPTION
            stmt = f"-- 升级已有权限\nGRANT {priv} ON {obj} TO {grantee} WITH GRANT OPTION;"
            upgrade_statements.append(stmt)
    
    return upgrade_statements
```

---

### 方案 4: 跨 Schema 依赖完整覆盖

#### 目标
自动识别所有跨 Schema 依赖并生成必要的 GRANT 语句。

#### 设计

**跨 Schema 场景识别**:
```python
class CrossSchemaScenario:
    """跨 Schema 场景定义"""
    
    # 场景1: 外键引用
    FK_REFERENCE = {
        "required_privileges": ["REFERENCES"],
        "detection": "DBA_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'R' AND R_OWNER != OWNER",
        "example": "SCHEMA_A.TABLE1 FK -> SCHEMA_B.TABLE2 (需要 GRANT REFERENCES ON SCHEMA_B.TABLE2 TO SCHEMA_A)"
    }
    
    # 场景2: 视图依赖
    VIEW_DEPENDENCY = {
        "required_privileges": ["SELECT"],
        "detection": "从 VIEW DDL 提取依赖表，检查是否跨 schema",
        "example": "SCHEMA_A.VIEW1 引用 SCHEMA_B.TABLE1 (需要 GRANT SELECT ON SCHEMA_B.TABLE1 TO SCHEMA_A)"
    }
    
    # 场景3: 触发器跨 Schema 调用
    TRIGGER_CROSS_CALL = {
        "required_privileges": ["EXECUTE"],
        "detection": "从 TRIGGER body 提取调用的 PACKAGE/PROCEDURE",
        "example": "SCHEMA_A.TRIGGER1 调用 SCHEMA_B.PKG1 (需要 GRANT EXECUTE ON SCHEMA_B.PKG1 TO SCHEMA_A)"
    }
    
    # 场景4: 同义词指向
    SYNONYM_REFERENCE = {
        "required_privileges": ["SELECT", "INSERT", "UPDATE", "DELETE"],
        "detection": "DBA_SYNONYMS WHERE TABLE_OWNER != OWNER",
        "example": "SCHEMA_A.SYN1 -> SCHEMA_B.TABLE1 (需要对应的权限)"
    }
    
    # 场景5: DBLINK 访问
    DBLINK_ACCESS = {
        "required_privileges": ["SELECT"],  # 取决于实际操作
        "detection": "VIEW/PROCEDURE 中包含 @DBLINK",
        "example": "SCHEMA_A.VIEW1 访问 TABLE1@REMOTE_DB (需要 DBLINK 权限)"
    }

def detect_cross_schema_dependencies(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    full_object_mapping: FullObjectMapping
) -> List[Tuple[str, str, str, str, str]]:
    """
    检测所有跨 Schema 依赖
    
    返回: [(from_schema, from_object, to_schema, to_object, required_privilege)]
    """
    dependencies = []
    
    # 1. 外键依赖
    for (owner, table), constraints in oracle_meta.constraints.items():
        for cons_name, cons_info in constraints.items():
            if cons_info.get("type") == "R":  # 外键
                ref_owner = cons_info.get("ref_table_owner")
                ref_table = cons_info.get("ref_table_name")
                if ref_owner and ref_table and ref_owner.upper() != owner.upper():
                    dependencies.append((
                        owner, f"{owner}.{table}",
                        ref_owner, f"{ref_owner}.{ref_table}",
                        "REFERENCES"
                    ))
    
    # 2. 视图依赖（从 DDL 提取）
    for (schema, view_name), compat in view_compat_map.items():
        if compat and compat.cleaned_ddl:
            deps = extract_view_dependencies(compat.cleaned_ddl, schema)
            for dep in deps:
                if "." in dep:
                    dep_schema, dep_obj = dep.split(".", 1)
                    if dep_schema.upper() != schema.upper():
                        dependencies.append((
                            schema, f"{schema}.{view_name}",
                            dep_schema, dep,
                            "SELECT"
                        ))
    
    # 3. 触发器依赖（需要解析 TRIGGER body，暂不实现）
    # 4. 同义词依赖
    for (schema, syn_name), syn_meta in synonym_meta_map.items():
        if syn_meta and syn_meta.table_owner:
            if syn_meta.table_owner.upper() != schema.upper():
                dependencies.append((
                    schema, f"{schema}.{syn_name}",
                    syn_meta.table_owner, f"{syn_meta.table_owner}.{syn_meta.table_name}",
                    "SELECT"  # 默认 SELECT，实际可能需要其他权限
                ))
    
    return dependencies

def generate_cross_schema_grant_statements(
    dependencies: List[Tuple[str, str, str, str, str]]
) -> Dict[str, List[str]]:
    """
    根据跨 Schema 依赖生成 GRANT 语句
    
    返回: {grantee_schema: [grant_statements]}
    """
    grants_by_grantee: Dict[str, List[str]] = defaultdict(list)
    
    for from_schema, from_obj, to_schema, to_obj, priv in dependencies:
        stmt = f"GRANT {priv} ON {to_obj} TO {from_schema};"
        grants_by_grantee[from_schema].append(stmt)
        log.info(
            f"[GRANT] 跨 Schema 依赖: {from_obj} 需要 {priv} ON {to_obj}"
        )
    
    return grants_by_grantee
```

---

### 方案 5: 授权验证和回滚脚本

#### 目标
生成验证脚本确认授权成功，以及回滚脚本用于撤销授权。

#### 设计

**验证脚本生成**:
```sql
-- verify_grants.sql
SET SERVEROUTPUT ON;
DECLARE
    v_count NUMBER;
    v_errors NUMBER := 0;
BEGIN
    -- 验证对象权限
    SELECT COUNT(*) INTO v_count
    FROM DBA_TAB_PRIVS
    WHERE GRANTEE = 'USER_A'
      AND PRIVILEGE = 'SELECT'
      AND OWNER = 'SCHEMA_B'
      AND TABLE_NAME = 'TABLE1'
      AND GRANTABLE = 'YES';  -- 检查 WITH GRANT OPTION
    
    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: USER_A 缺少 SELECT ON SCHEMA_B.TABLE1 WITH GRANT OPTION');
        v_errors := v_errors + 1;
    END IF;
    
    -- 验证系统权限
    SELECT COUNT(*) INTO v_count
    FROM DBA_SYS_PRIVS
    WHERE GRANTEE = 'USER_A'
      AND PRIVILEGE = 'CREATE TABLE';
    
    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: USER_A 缺少 CREATE TABLE 权限');
        v_errors := v_errors + 1;
    END IF;
    
    -- 验证角色授权
    SELECT COUNT(*) INTO v_count
    FROM DBA_ROLE_PRIVS
    WHERE GRANTEE = 'USER_A'
      AND GRANTED_ROLE = 'DBA_ROLE';
    
    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: USER_A 未被授予 DBA_ROLE 角色');
        v_errors := v_errors + 1;
    END IF;
    
    IF v_errors = 0 THEN
        DBMS_OUTPUT.PUT_LINE('SUCCESS: 所有授权验证通过');
    ELSE
        DBMS_OUTPUT.PUT_LINE('FAILED: 发现 ' || v_errors || ' 个授权问题');
    END IF;
END;
/
```

**回滚脚本生成**:
```python
def generate_grant_rollback_script(
    grant_statements: List[str]
) -> List[str]:
    """
    生成授权回滚脚本（REVOKE 语句）
    """
    revoke_statements = []
    
    for grant_stmt in grant_statements:
        # 解析 GRANT 语句
        # GRANT SELECT ON TABLE1 TO USER_A WITH GRANT OPTION;
        match = re.match(
            r'GRANT\s+(\w+)\s+ON\s+([\w.]+)\s+TO\s+(\w+)(\s+WITH\s+GRANT\s+OPTION)?',
            grant_stmt,
            re.IGNORECASE
        )
        if match:
            priv, obj, grantee, _ = match.groups()
            revoke_stmt = f"REVOKE {priv} ON {obj} FROM {grantee};"
            revoke_statements.append(revoke_stmt)
    
    return revoke_statements
```

---

## 📝 完整实施方案

### 阶段1: 元数据增强（P0）

**代码位置**: lines 523-562

**修改**:
```python
# 新增字段到现有 NamedTuple
class EnhancedObjectGrantEntry(NamedTuple):
    privilege: str
    object_full: str
    grantable: bool
    grantor: str          # 🆕
    grant_source: str     # 🆕 "DIRECT" 或 "ROLE:xxx"
```

**SQL 增强**:
```python
# lines 6191-6214 修改
sql = """
    SELECT GRANTEE, PRIVILEGE, OWNER, TABLE_NAME, GRANTABLE, GRANTOR
    FROM DBA_TAB_PRIVS
    WHERE GRANTEE IN ({grantee_list})
"""
```

---

### 阶段2: WITH GRANT OPTION 精确检测（P0）

**代码位置**: lines 8259-8338

**修改**:
```python
def filter_missing_grant_entries(...):
    # 增强检测逻辑，区分基本权限和 WITH GRANT OPTION
    if entry.grantable:
        if key not in obj_grantable:
            miss_obj[g_u].add(entry)
            if key in obj_basic:
                log.info(f"需补充 WITH GRANT OPTION: {key}")
```

---

### 阶段3: 授权拓扑排序（P1）

**新增函数**:
```python
# 在 generate_fixup_scripts 函数之前添加
def sort_grants_by_dependency(
    grants_by_grantee: Dict[str, List[str]]
) -> List[Tuple[str, List[str]]]:
    """对授权语句按依赖关系排序"""
    # 实现拓扑排序逻辑
    pass
```

---

### 阶段4: 跨 Schema 依赖自动检测（P1）

**新增函数**:
```python
# 在 collect_expected_dependencies 附近添加
def detect_and_generate_cross_schema_grants(
    oracle_meta,
    ob_meta,
    full_object_mapping
) -> Dict[str, List[str]]:
    """自动检测跨 Schema 依赖并生成授权"""
    pass
```

---

### 阶段5: 验证和回滚脚本（P2）

**修改 write_fixup_file**:
```python
# lines 14635-14664 增强
def write_fixup_file(..., generate_verify=True, generate_rollback=True):
    # 生成主脚本
    # 生成验证脚本
    # 生成回滚脚本
    pass
```

---

## 🎯 预期效果

### 1. 授权完整性
- ✅ 精确检测 WITH GRANT OPTION 缺失
- ✅ 自动识别跨 Schema 依赖
- ✅ 授权链路可追溯

### 2. 执行可靠性
- ✅ 按依赖顺序生成脚本
- ✅ 避免授权失败
- ✅ 提供验证和回滚能力

### 3. 运维友好性
- ✅ 清晰的授权说明（from_object → to_object）
- ✅ 可重复执行的脚本
- ✅ 审计友好（包含 GRANTOR 信息）

---

## 📊 优先级建议

| 阶段 | 优先级 | 工作量 | 收益 | 建议时间 |
|-----|-------|--------|------|---------|
| 阶段1 | P0 | 2天 | 高 | 立即实施 |
| 阶段2 | P0 | 1天 | 高 | 立即实施 |
| 阶段3 | P1 | 3天 | 中 | 1周内 |
| 阶段4 | P1 | 2天 | 高 | 1周内 |
| 阶段5 | P2 | 1天 | 中 | 1个月内 |

---

## 🔧 示例代码

见附录：`grant_solution_example.py`
