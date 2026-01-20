#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
跨 Schema 授权完整性方案 - 示例代码

本文件提供完善 GRANT 方案的参考实现代码
"""

from typing import NamedTuple, Dict, Set, List, Tuple, Optional
from collections import defaultdict, deque


# ============================================================================
# 阶段1: 增强数据结构
# ============================================================================

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
    reason: str                # 依赖原因


# ============================================================================
# 阶段2: 增强元数据收集（替换现有的 load_ob_grant_catalog）
# ============================================================================

def load_oracle_object_grants_enhanced(
    ora_conn,
    grantees: Set[str]
) -> List[EnhancedObjectGrant]:
    """
    增强版对象权限收集，包含 GRANTOR 和授权层级
    
    替换位置: lines 6175-6214
    """
    if not grantees:
        return []
    
    grants: List[EnhancedObjectGrant] = []
    
    # 构建 grantee 列表
    grantee_list = ",".join(f"'{g.upper()}'" for g in grantees if g)
    if not grantee_list:
        return grants
    
    # 🆕 增强 SQL：新增 GRANTOR 字段
    sql = f"""
        SELECT 
            GRANTEE, 
            PRIVILEGE, 
            OWNER, 
            TABLE_NAME, 
            TYPE,
            GRANTABLE,
            GRANTOR,              -- 🆕 授权者
            HIERARCHY             -- 🆕 授权层级（可选，需要递归查询构建）
        FROM DBA_TAB_PRIVS
        WHERE GRANTEE IN ({grantee_list})
        ORDER BY 
            -- 按授权层级排序，确保先创建授权者的权限
            CASE 
                WHEN GRANTOR = OWNER THEN 1  -- 对象所有者授权
                WHEN GRANTABLE = 'YES' THEN 2  -- 可授权权限
                ELSE 3
            END
    """
    
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(sql)
            for row in cursor:
                grantee = (row[0] or "").strip().upper()
                privilege = (row[1] or "").strip().upper()
                owner = (row[2] or "").strip().upper()
                obj_name = (row[3] or "").strip().upper()
                obj_type = (row[4] or "").strip().upper() if len(row) > 4 else ""
                grantable = (row[5] or "").strip().upper() == "YES" if len(row) > 5 else False
                grantor = (row[6] or "").strip().upper() if len(row) > 6 else owner
                
                # 推断授权路径（简化版）
                if grantor == owner:
                    hierarchy = f"{owner}"
                    grant_source = "DIRECT"
                else:
                    hierarchy = f"{owner}→{grantor}→{grantee}"
                    grant_source = f"INDIRECT:{grantor}"
                
                if not grantee or not privilege or not owner or not obj_name:
                    continue
                
                grants.append(EnhancedObjectGrant(
                    grantee=grantee,
                    privilege=privilege,
                    object_owner=owner,
                    object_name=obj_name,
                    object_type=obj_type,
                    grantable=grantable,
                    grantor=grantor,
                    hierarchy=hierarchy,
                    grant_source=grant_source
                ))
    except Exception as exc:
        print(f"[GRANT] 读取对象权限失败: {exc}")
    
    return grants


# ============================================================================
# 阶段3: WITH GRANT OPTION 精确检测
# ============================================================================

def filter_missing_grants_precise(
    required_grants: List[EnhancedObjectGrant],
    existing_basic: Set[Tuple[str, str, str]],      # (grantee, priv, obj)
    existing_grantable: Set[Tuple[str, str, str]]   # (grantee, priv, obj) with WGO
) -> Tuple[List[EnhancedObjectGrant], List[str]]:
    """
    精确过滤缺失的授权，区分基本权限和 WITH GRANT OPTION
    
    返回: (缺失的授权列表, 需要升级的授权说明列表)
    """
    missing_grants = []
    upgrade_notes = []
    
    for grant in required_grants:
        key = (grant.grantee, grant.privilege, f"{grant.object_owner}.{grant.object_name}")
        
        if grant.grantable:
            # 需要 WITH GRANT OPTION
            if key not in existing_grantable:
                if key in existing_basic:
                    # 已有基本权限，需要升级
                    upgrade_notes.append(
                        f"-- 升级权限: {grant.grantee} 已有 {grant.privilege} ON "
                        f"{grant.object_owner}.{grant.object_name}，需补充 WITH GRANT OPTION"
                    )
                missing_grants.append(grant)
        else:
            # 只需要基本权限
            if key not in existing_basic and key not in existing_grantable:
                missing_grants.append(grant)
    
    return missing_grants, upgrade_notes


def generate_upgrade_grant_statements(
    existing_basic: Set[Tuple[str, str, str]],
    required_grantable: Set[Tuple[str, str, str]]
) -> List[str]:
    """
    为已有权限补充 WITH GRANT OPTION
    """
    upgrade_statements = []
    
    for grantee, priv, obj in required_grantable:
        if (grantee, priv, obj) in existing_basic:
            stmt = (
                f"-- 升级已有权限\n"
                f"GRANT {priv} ON {obj} TO {grantee} WITH GRANT OPTION;"
            )
            upgrade_statements.append(stmt)
    
    return upgrade_statements


# ============================================================================
# 阶段4: 授权依赖拓扑排序
# ============================================================================

def build_grant_dependency_graph(
    grants: List[EnhancedObjectGrant]
) -> Dict[str, Set[str]]:
    """
    构建授权依赖图
    
    返回: {grantee: set(dependencies)}
    
    示例:
        如果 USER_B 需要授权给 USER_C，但 USER_B 的权限来自 USER_A:
        {"USER_C": {"USER_B"}, "USER_B": {"USER_A"}}
    """
    graph: Dict[str, Set[str]] = defaultdict(set)
    
    for grant in grants:
        if grant.grantable and grant.grantor != grant.object_owner:
            # 如果是二次授权（非对象所有者授权），需要先确保授权者有 WITH GRANT OPTION
            graph[grant.grantee].add(grant.grantor)
    
    return graph


def topological_sort_grants(
    grants_by_grantee: Dict[str, List[str]],
    dependency_graph: Dict[str, Set[str]]
) -> Tuple[List[Tuple[str, List[str]]], List[str]]:
    """
    对授权语句进行拓扑排序
    
    返回: (排序后的授权列表, 循环依赖的被授权者列表)
    """
    in_degree = {grantee: 0 for grantee in grants_by_grantee}
    
    # 计算入度
    for grantee, deps in dependency_graph.items():
        if grantee in in_degree:
            in_degree[grantee] = len(deps & set(grants_by_grantee.keys()))
    
    # Kahn's 算法
    queue = deque([g for g, d in in_degree.items() if d == 0])
    sorted_grantees = []
    
    while queue:
        current = queue.popleft()
        sorted_grantees.append(current)
        
        # 减少依赖此节点的其他节点的入度
        for grantee, deps in dependency_graph.items():
            if current in deps:
                in_degree[grantee] -= 1
                if in_degree[grantee] == 0:
                    queue.append(grantee)
    
    # 检测循环依赖
    circular = [g for g, d in in_degree.items() if d > 0]
    
    sorted_grants = [(g, grants_by_grantee[g]) for g in sorted_grantees]
    
    return sorted_grants, circular


# ============================================================================
# 阶段5: 跨 Schema 依赖检测
# ============================================================================

class CrossSchemaDependency(NamedTuple):
    """跨 Schema 依赖"""
    from_schema: str
    from_object: str
    to_schema: str
    to_object: str
    required_privilege: str
    dependency_type: str  # "FK", "VIEW", "TRIGGER", "SYNONYM"


def detect_fk_cross_schema_dependencies(
    constraints: Dict[Tuple[str, str], Dict[str, Dict]]
) -> List[CrossSchemaDependency]:
    """
    检测外键跨 Schema 依赖
    
    参数: oracle_meta.constraints
    返回: 外键跨 Schema 依赖列表
    """
    dependencies = []
    
    for (owner, table), constraints_map in constraints.items():
        for cons_name, cons_info in constraints_map.items():
            if cons_info.get("type") == "R":  # 外键
                ref_owner = cons_info.get("ref_table_owner") or cons_info.get("r_owner")
                ref_table = cons_info.get("ref_table_name")
                
                if ref_owner and ref_table and ref_owner.upper() != owner.upper():
                    dependencies.append(CrossSchemaDependency(
                        from_schema=owner,
                        from_object=f"{owner}.{table}",
                        to_schema=ref_owner,
                        to_object=f"{ref_owner}.{ref_table}",
                        required_privilege="REFERENCES",
                        dependency_type="FK"
                    ))
    
    return dependencies


def detect_view_cross_schema_dependencies(
    view_ddl_map: Dict[Tuple[str, str], str]
) -> List[CrossSchemaDependency]:
    """
    从 VIEW DDL 中提取跨 Schema 依赖
    
    参数: {(schema, view_name): ddl_text}
    返回: 视图跨 Schema 依赖列表
    """
    dependencies = []
    
    for (schema, view_name), ddl in view_ddl_map.items():
        # 简化版：使用正则提取引用的表/视图
        import re
        
        # 匹配 FROM/JOIN 后的对象名（schema.object）
        pattern = r'\b(FROM|JOIN)\s+([A-Z_][A-Z0-9_$]*\.[A-Z_][A-Z0-9_$]*)\b'
        matches = re.findall(pattern, ddl, re.IGNORECASE)
        
        for _, obj_full in matches:
            if "." in obj_full:
                dep_schema, dep_obj = obj_full.split(".", 1)
                dep_schema = dep_schema.upper()
                
                if dep_schema != schema.upper():
                    dependencies.append(CrossSchemaDependency(
                        from_schema=schema,
                        from_object=f"{schema}.{view_name}",
                        to_schema=dep_schema,
                        to_object=obj_full.upper(),
                        required_privilege="SELECT",
                        dependency_type="VIEW"
                    ))
    
    return dependencies


def detect_synonym_cross_schema_dependencies(
    synonym_meta_map: Dict[Tuple[str, str], any]
) -> List[CrossSchemaDependency]:
    """
    检测同义词跨 Schema 依赖
    
    参数: {(schema, syn_name): SynonymMeta}
    返回: 同义词跨 Schema 依赖列表
    """
    dependencies = []
    
    for (schema, syn_name), syn_meta in synonym_meta_map.items():
        if syn_meta and hasattr(syn_meta, 'table_owner') and syn_meta.table_owner:
            if syn_meta.table_owner.upper() != schema.upper():
                dependencies.append(CrossSchemaDependency(
                    from_schema=schema,
                    from_object=f"{schema}.{syn_name}",
                    to_schema=syn_meta.table_owner,
                    to_object=f"{syn_meta.table_owner}.{syn_meta.table_name}",
                    required_privilege="SELECT",  # 默认，实际可能需要其他权限
                    dependency_type="SYNONYM"
                ))
    
    return dependencies


def generate_cross_schema_grant_statements(
    dependencies: List[CrossSchemaDependency]
) -> Dict[str, List[str]]:
    """
    根据跨 Schema 依赖生成 GRANT 语句
    
    返回: {grantee_schema: [grant_statements]}
    """
    grants_by_grantee: Dict[str, List[str]] = defaultdict(list)
    
    for dep in dependencies:
        stmt = f"GRANT {dep.required_privilege} ON {dep.to_object} TO {dep.from_schema};"
        comment = f"-- {dep.dependency_type}: {dep.from_object} 需要 {dep.required_privilege} ON {dep.to_object}"
        
        grants_by_grantee[dep.from_schema].append(comment)
        grants_by_grantee[dep.from_schema].append(stmt)
    
    return grants_by_grantee


# ============================================================================
# 阶段6: 授权验证脚本生成
# ============================================================================

def generate_grant_verification_script(
    required_grants: List[EnhancedObjectGrant]
) -> str:
    """
    生成授权验证 PL/SQL 脚本
    """
    verification_checks = []
    
    for idx, grant in enumerate(required_grants, 1):
        obj_full = f"{grant.object_owner}.{grant.object_name}"
        wgo_check = "AND GRANTABLE = 'YES'" if grant.grantable else ""
        
        check = f"""
    -- 检查 {idx}: {grant.grantee} - {grant.privilege} ON {obj_full}
    SELECT COUNT(*) INTO v_count
    FROM DBA_TAB_PRIVS
    WHERE GRANTEE = '{grant.grantee}'
      AND PRIVILEGE = '{grant.privilege}'
      AND OWNER = '{grant.object_owner}'
      AND TABLE_NAME = '{grant.object_name}'
      {wgo_check};
    
    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: {grant.grantee} 缺少 {grant.privilege} ON {obj_full}{"WITH GRANT OPTION" if grant.grantable else ""}');
        v_errors := v_errors + 1;
    END IF;
"""
        verification_checks.append(check)
    
    script = f"""
SET SERVEROUTPUT ON;
DECLARE
    v_count NUMBER;
    v_errors NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('开始验证授权 ({len(required_grants)} 条)...');
    DBMS_OUTPUT.PUT_LINE('');
    
{"".join(verification_checks)}
    
    DBMS_OUTPUT.PUT_LINE('');
    IF v_errors = 0 THEN
        DBMS_OUTPUT.PUT_LINE('SUCCESS: 所有授权验证通过！');
    ELSE
        DBMS_OUTPUT.PUT_LINE('FAILED: 发现 ' || v_errors || ' 个授权问题');
        RAISE_APPLICATION_ERROR(-20001, '授权验证失败');
    END IF;
END;
/
"""
    
    return script


def generate_grant_rollback_script(
    grant_statements: List[str]
) -> List[str]:
    """
    生成授权回滚脚本（REVOKE 语句）
    """
    import re
    
    revoke_statements = []
    
    for grant_stmt in grant_statements:
        # 跳过注释
        if grant_stmt.strip().startswith("--"):
            continue
        
        # 解析 GRANT 语句
        # GRANT SELECT ON TABLE1 TO USER_A WITH GRANT OPTION;
        match = re.match(
            r'GRANT\s+(\w+)\s+ON\s+([\w.]+)\s+TO\s+(\w+)(\s+WITH\s+GRANT\s+OPTION)?',
            grant_stmt.strip(),
            re.IGNORECASE
        )
        
        if match:
            priv, obj, grantee, _ = match.groups()
            revoke_stmt = f"REVOKE {priv} ON {obj} FROM {grantee};"
            revoke_statements.append(revoke_stmt)
    
    return revoke_statements


# ============================================================================
# 阶段7: 完整授权方案生成器
# ============================================================================

def generate_comprehensive_grant_solution(
    required_grants: List[EnhancedObjectGrant],
    existing_basic: Set[Tuple[str, str, str]],
    existing_grantable: Set[Tuple[str, str, str]],
    cross_schema_deps: List[CrossSchemaDependency],
    output_dir: str
) -> None:
    """
    生成完整的授权方案，包括：
    1. 主授权脚本（按依赖排序）
    2. 验证脚本
    3. 回滚脚本
    4. 跨 Schema 授权脚本
    """
    from pathlib import Path
    
    # 1. 过滤缺失授权
    missing_grants, upgrade_notes = filter_missing_grants_precise(
        required_grants, existing_basic, existing_grantable
    )
    
    print(f"[GRANT] 发现缺失授权 {len(missing_grants)} 条")
    
    # 2. 按被授权者分组
    grants_by_grantee: Dict[str, List[str]] = defaultdict(list)
    for grant in missing_grants:
        obj_full = f"{grant.object_owner}.{grant.object_name}"
        stmt = f"GRANT {grant.privilege} ON {obj_full} TO {grant.grantee}"
        if grant.grantable:
            stmt += " WITH GRANT OPTION"
        stmt += ";"
        
        grants_by_grantee[grant.grantee].append(stmt)
    
    # 3. 构建依赖图并拓扑排序
    dep_graph = build_grant_dependency_graph(missing_grants)
    sorted_grants, circular = topological_sort_grants(grants_by_grantee, dep_graph)
    
    if circular:
        print(f"[GRANT] 警告：发现循环依赖的被授权者: {circular}")
    
    # 4. 生成主脚本
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    with open(output_path / "01_grants_main.sql", "w", encoding="utf-8") as f:
        f.write("-- 主授权脚本（按依赖顺序生成）\n")
        f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- 授权总数: {len(missing_grants)}\n\n")
        
        if upgrade_notes:
            f.write("-- ============================================\n")
            f.write("-- 需要升级的权限（补充 WITH GRANT OPTION）\n")
            f.write("-- ============================================\n\n")
            for note in upgrade_notes:
                f.write(f"{note}\n")
            f.write("\n")
        
        for idx, (grantee, stmts) in enumerate(sorted_grants, 1):
            f.write(f"-- ============================================\n")
            f.write(f"-- {idx}. 授权给: {grantee}\n")
            f.write(f"-- ============================================\n\n")
            for stmt in stmts:
                f.write(f"{stmt}\n")
            f.write("\n")
    
    # 5. 生成跨 Schema 授权脚本
    cross_grants = generate_cross_schema_grant_statements(cross_schema_deps)
    
    if cross_grants:
        with open(output_path / "02_grants_cross_schema.sql", "w", encoding="utf-8") as f:
            f.write("-- 跨 Schema 授权脚本\n")
            f.write(f"-- 依赖总数: {len(cross_schema_deps)}\n\n")
            
            for grantee, stmts in cross_grants.items():
                f.write(f"-- ============================================\n")
                f.write(f"-- 授权给: {grantee}\n")
                f.write(f"-- ============================================\n\n")
                for stmt in stmts:
                    f.write(f"{stmt}\n")
                f.write("\n")
    
    # 6. 生成验证脚本
    verification_script = generate_grant_verification_script(missing_grants)
    
    with open(output_path / "03_grants_verify.sql", "w", encoding="utf-8") as f:
        f.write(verification_script)
    
    # 7. 生成回滚脚本
    all_grant_stmts = [stmt for _, stmts in sorted_grants for stmt in stmts]
    rollback_stmts = generate_grant_rollback_script(all_grant_stmts)
    
    with open(output_path / "04_grants_rollback.sql", "w", encoding="utf-8") as f:
        f.write("-- 授权回滚脚本（REVOKE）\n")
        f.write("-- 警告：执行此脚本将撤销所有授权\n\n")
        for stmt in rollback_stmts:
            f.write(f"{stmt}\n")
    
    print(f"[GRANT] 授权方案已生成到: {output_path}")
    print(f"[GRANT] - 01_grants_main.sql: 主授权脚本（{len(missing_grants)} 条）")
    if cross_grants:
        print(f"[GRANT] - 02_grants_cross_schema.sql: 跨 Schema 授权（{len(cross_schema_deps)} 条）")
    print(f"[GRANT] - 03_grants_verify.sql: 验证脚本")
    print(f"[GRANT] - 04_grants_rollback.sql: 回滚脚本")


# ============================================================================
# 使用示例
# ============================================================================

if __name__ == "__main__":
    from datetime import datetime
    
    # 示例数据
    required_grants = [
        EnhancedObjectGrant(
            grantee="USER_B",
            privilege="SELECT",
            object_owner="USER_A",
            object_name="TABLE1",
            object_type="TABLE",
            grantable=True,
            grantor="USER_A",
            hierarchy="USER_A→USER_B",
            grant_source="DIRECT"
        ),
        EnhancedObjectGrant(
            grantee="USER_C",
            privilege="SELECT",
            object_owner="USER_A",
            object_name="TABLE1",
            object_type="TABLE",
            grantable=False,
            grantor="USER_B",
            hierarchy="USER_A→USER_B→USER_C",
            grant_source="INDIRECT:USER_B"
        ),
    ]
    
    existing_basic = {("USER_B", "SELECT", "USER_A.TABLE1")}
    existing_grantable = set()
    
    cross_schema_deps = [
        CrossSchemaDependency(
            from_schema="SCHEMA_A",
            from_object="SCHEMA_A.TABLE1",
            to_schema="SCHEMA_B",
            to_object="SCHEMA_B.TABLE2",
            required_privilege="REFERENCES",
            dependency_type="FK"
        )
    ]
    
    # 生成完整方案
    generate_comprehensive_grant_solution(
        required_grants=required_grants,
        existing_basic=existing_basic,
        existing_grantable=existing_grantable,
        cross_schema_deps=cross_schema_deps,
        output_dir="./grant_scripts"
    )
