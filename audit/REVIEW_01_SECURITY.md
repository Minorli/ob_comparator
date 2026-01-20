# 安全问题审查报告

**风险等级**: ⚠️ 中  
**建议修复时间**: 1-2周

---

## 说明

根据项目实际使用场景，密码明文存储不作为安全风险项。本报告聚焦于其他安全问题。

---

## 1. SQL注入风险 ⚠️ 中危

### 问题描述

使用字符串拼接构造SQL，未充分转义特殊字符。

**位置**: `schema_diff_reconciler.py:5196`
```python
owners_in = ",".join(f"'{s}'" for s in chunk)
sql = sql_tpl.format(owners_in=owners_in)
```

### 风险场景

```python
# 如果 owner 包含单引号
owner = "TEST'OR'1'='1"
owners_in = f"'{owner}'"  # 结果: 'TEST'OR'1'='1'
sql = f"SELECT * FROM DBA_OBJECTS WHERE OWNER IN ({owners_in})"
# 生成的SQL: SELECT * FROM DBA_OBJECTS WHERE OWNER IN ('TEST'OR'1'='1')
# 可能导致SQL语法错误或注入
```

### 修复方案

```python
def escape_sql_identifier(value: str) -> str:
    """
    转义SQL标识符中的特殊字符
    Oracle/OceanBase规则：单引号需要双写
    """
    if not value:
        return value
    return value.replace("'", "''")

def build_in_clause_safe(values: List[str], max_length: int = 900) -> str:
    """安全构建 IN 子句"""
    if not values:
        return "''"
    escaped_values = [escape_sql_identifier(v) for v in values]
    return ",".join(f"'{v}'" for v in escaped_values)

# 使用示例
owners_in = build_in_clause_safe(chunk)
sql = sql_tpl.format(owners_in=owners_in)
```

### 应用位置

需要修复的函数：
- `obclient_query_by_owner_chunks()`
- `obclient_query_by_owner_pairs()`
- 所有使用字符串拼接构造SQL的地方

---

## 2. 动态SQL列名验证 ⚠️ 低危

### 问题描述

动态构造SQL时未验证列名。

**位置**: `schema_diff_reconciler.py:6084`
```python
sql = f"SELECT {select_cols} FROM DBA_ROLES"  # 未验证列名
```

### 修复方案

```python
# 定义白名单
ALLOWED_DBA_ROLES_COLUMNS = {
    'ROLE', 
    'AUTHENTICATION_TYPE', 
    'PASSWORD_REQUIRED', 
    'ORACLE_MAINTAINED',
    'COMMON',
    'INHERITED'
}

def validate_column_names(columns: List[str], allowed: Set[str]) -> List[str]:
    """验证列名是否在白名单中"""
    validated = []
    for col in columns:
        col_upper = col.strip().upper()
        if col_upper not in allowed:
            raise ValueError(f"不允许的列名: {col}")
        validated.append(col_upper)
    return validated

# 使用
validated_cols = validate_column_names(columns, ALLOWED_DBA_ROLES_COLUMNS)
sql = f"SELECT {', '.join(validated_cols)} FROM DBA_ROLES"
```

---

## 3. 文件权限问题 ⚠️ 低危

### 问题描述

生成的脚本和日志文件未设置安全权限。

### 风险文件
- `fixup_scripts/*.sql` - DDL脚本
- `logs/*.log` - 日志文件
- `main_reports/*` - 报告文件

### 修复方案

```python
import os
import stat
from pathlib import Path

def write_file_secure(
    filepath: Path,
    content: str,
    mode: int = 0o640,  # 所有者读写，组只读
    encoding: str = 'utf-8'
):
    """安全写入文件并设置权限"""
    filepath.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    
    with open(filepath, 'w', encoding=encoding) as f:
        f.write(content)
    
    os.chmod(filepath, mode)

# 使用示例
write_file_secure(
    Path('fixup_scripts/table/T1.sql'),
    ddl_content,
    mode=0o640
)
```

### 目录权限建议

```bash
# 设置目录权限
chmod 750 fixup_scripts/
chmod 750 logs/
chmod 750 main_reports/
```

---

## 安全检查清单

### 本周完成
- [ ] 添加 `escape_sql_identifier()` 函数
- [ ] 修复所有SQL拼接处的转义
- [ ] 添加列名白名单验证

### 本月完成
- [ ] 设置文件和目录权限
- [ ] 添加输入验证
- [ ] 代码安全审计

---

## 参考资料

- [CWE-89: SQL Injection](https://cwe.mitre.org/data/definitions/89.html)
- [OWASP SQL Injection Prevention](https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html)
