# schema_diff_reconciler.py 硬编码规则分析报告

> **生成时间**: 2026-02-05  
> **分析文件**: `schema_diff_reconciler.py`  
> **目的**: 梳理所有硬编码规则，评估新项目迁移时的风险和改造需求

---

## 目录

1. [对象类型定义](#一对象类型定义)
2. [列/对象过滤规则](#二列对象过滤规则)
3. [数据类型校验规则](#三数据类型校验规则)
4. [VIEW 兼容性规则](#四view-兼容性规则)
5. [约束校验规则](#五约束校验规则)
6. [黑名单/阻断规则](#六黑名单阻断规则)
7. [Fixup 生成规则](#七fixup-生成规则)
8. [DDL Hint 过滤规则](#八ddl-hint-过滤规则)
9. [权限过滤规则](#九权限过滤规则)
10. [批量/性能常量](#十批量性能常量)
11. [正则表达式模式](#十一正则表达式模式)
12. [风险评估与建议](#十二风险评估与建议)

---

## 一、对象类型定义

### 1.1 主对象类型 (PRIMARY_OBJECT_TYPES)

**位置**: `schema_diff_reconciler.py:693-706`

```python
PRIMARY_OBJECT_TYPES: Tuple[str, ...] = (
    'TABLE',
    'VIEW',
    'MATERIALIZED VIEW',
    'PROCEDURE',
    'FUNCTION',
    'PACKAGE',
    'PACKAGE BODY',
    'SYNONYM',
    'JOB',
    'SCHEDULE',
    'TYPE',
    'TYPE BODY'
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🔴 高 | 新增对象类型（如 `DATABASE LINK`、`DIRECTORY`、`QUEUE`）需修改源代码 |
| 🔴 高 | 无法通过配置扩展支持的对象类型 |

---

### 1.2 仅打印不校验类型 (PRINT_ONLY_PRIMARY_TYPES)

**位置**: `schema_diff_reconciler.py:708-714`

```python
PRINT_ONLY_PRIMARY_TYPES: Tuple[str, ...] = (
    'MATERIALIZED VIEW'
)

PRINT_ONLY_PRIMARY_REASONS: Dict[str, str] = {
    'MATERIALIZED VIEW': "OB 暂不支持 MATERIALIZED VIEW，仅打印不校验"
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 无法通过配置开启 MATERIALIZED VIEW 校验 |
| 🟠 中 | OB 新版本支持 MVIEW 后需要修改代码 |

---

### 1.3 扩展对象类型 (EXTRA_OBJECT_CHECK_TYPES)

**位置**: `schema_diff_reconciler.py:821-826`

```python
EXTRA_OBJECT_CHECK_TYPES: Tuple[str, ...] = (
    'INDEX',
    'CONSTRAINT',
    'SEQUENCE',
    'TRIGGER'
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 扩展对象类型固定，无法添加新类型 |

---

### 1.4 不参与 Schema 推导的类型 (NO_INFER_SCHEMA_TYPES)

**位置**: `schema_diff_reconciler.py:742-748`

```python
NO_INFER_SCHEMA_TYPES: Set[str] = {
    'VIEW',
    'MATERIALIZED VIEW',
    'TRIGGER',
    'PACKAGE',
    'PACKAGE BODY'
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 这些类型不参与自动 schema 推导，需显式配置 remap |

---

### 1.5 INVALID 状态检测类型 (INVALID_STATUS_TYPES)

**位置**: `schema_diff_reconciler.py:750-759`

```python
INVALID_STATUS_TYPES: Set[str] = {
    'VIEW',
    'PROCEDURE',
    'FUNCTION',
    'PACKAGE',
    'PACKAGE BODY',
    'TYPE',
    'TYPE BODY',
    'TRIGGER'
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 仅影响 INVALID 状态检测范围 |

---

### 1.6 PL/SQL 对象优先级 (PLSQL_ORDER_PRIORITY)

**位置**: `schema_diff_reconciler.py:731-739`

```python
PLSQL_ORDER_PRIORITY: Dict[str, int] = {
    'TYPE': 0,
    'PACKAGE': 1,
    'PROCEDURE': 2,
    'FUNCTION': 3,
    'TRIGGER': 4,
    'TYPE BODY': 5,
    'PACKAGE BODY': 6,
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 影响 fixup 脚本生成顺序，一般无需修改 |

---

## 二、列/对象过滤规则

### 2.1 OMS 迁移列忽略 (IGNORED_OMS_COLUMNS)

**位置**: `schema_diff_reconciler.py:867-872`

```python
IGNORED_OMS_COLUMNS: Tuple[str, ...] = (
    "OMS_OBJECT_NUMBER",
    "OMS_RELATIVE_FNO",
    "OMS_BLOCK_NUMBER",
    "OMS_ROW_NUMBER",
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🔴 高 | **如果目标项目不使用 OMS 迁移，这些列仍会被无条件忽略** |
| 🔴 高 | 如果业务表恰好有同名列，会被误过滤 |
| 🟠 中 | 无法通过配置关闭此过滤 |

---

### 2.2 系统自动生成列 (AUTO_GENERATED_COLUMNS)

**位置**: `schema_diff_reconciler.py:875-877`

```python
AUTO_GENERATED_COLUMNS: Tuple[str, ...] = (
    "__PK_INCREMENT",
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 固定忽略 `__PK_INCREMENT` 列 |

---

### 2.3 自动序列模式 (AUTO_SEQUENCE_PATTERNS)

**位置**: `schema_diff_reconciler.py:878-880`

```python
AUTO_SEQUENCE_PATTERNS = (
    re.compile(r"^ISEQ\$\$_", re.IGNORECASE),
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 以 `ISEQ$$_` 开头的序列被视为自动生成，降噪处理 |
| 🟡 低 | 如果业务序列恰好以此开头，会被误过滤 |

---

### 2.4 SYS_NC 列模式 (SYS_NC_COLUMN_PATTERNS)

**位置**: `schema_diff_reconciler.py:881-884`

```python
SYS_NC_COLUMN_PATTERNS = (
    re.compile(r"^SYS_NC\d+\$", re.IGNORECASE),
    re.compile(r"^SYS_NC_[A-Z_]+\$", re.IGNORECASE),
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | Oracle 内部列，一般不影响业务 |

---

### 2.5 SYS_C 列/约束模式 (SYS_C_COLUMN_PATTERNS)

**位置**: `schema_diff_reconciler.py:886-888`

```python
SYS_C_COLUMN_PATTERNS = (
    re.compile(r"^SYS_C\d+", re.IGNORECASE),
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | Oracle 自动生成的约束名，一般不影响业务 |

---

### 2.6 IOT 表跳过

**位置**: `schema_diff_reconciler.py:5310-5311`, `5370-5371`

```python
if obj_name.startswith("SYS_IOT_OVER_"):
    skipped_iot += 1
    continue
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 所有 `SYS_IOT_OVER_*` 对象被无条件跳过 |
| 🟠 中 | 无法通过配置开关控制 |

---

### 2.7 OBNOTNULL 约束忽略

**位置**: `schema_diff_reconciler.py:1200-1205`

```python
def is_ob_notnull_constraint(name: Optional[object]) -> bool:
    return "OBNOTNULL" in extract_constraint_name(name).upper()
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | OB 自动生成的 `*_OBNOTNULL_*` CHECK 约束被静默过滤 |
| 🟡 低 | 这是正确行为，一般不需要修改 |

---

### 2.8 OMS 索引识别

**位置**: `schema_diff_reconciler.py:1162-1179`

```python
def is_oms_index(name: str, columns: List[str]) -> bool:
    if is_oms_rowid_index_name(name):
        return True
    # 检查名称是否以 _OMS_ROWID 结尾
    if not name_u.endswith("_OMS_ROWID"):
        return False
    # 检查列集合是否包含所有标准 OMS 列
    cols_set = set(cols_u)
    oms_cols_set = set(IGNORED_OMS_COLUMNS)
    return oms_cols_set.issubset(cols_set)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 以 `_OMS_ROWID` 结尾的索引被识别为 OMS 索引并过滤 |
| 🟠 中 | 如果不使用 OMS，此逻辑仍会生效 |

---

## 三、数据类型校验规则

### 3.1 VARCHAR 长度倍数

**位置**: `schema_diff_reconciler.py:1000-1001`

```python
VARCHAR_LEN_MIN_MULTIPLIER = 1.5   # 目标端 VARCHAR/2 长度需 >= ceil(src * 1.5)
VARCHAR_LEN_OVERSIZE_MULTIPLIER = 2.5  # 超过该倍数认为"过大"
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🔴 高 | **长度校验区间 [1.5x, 2.5x] 完全硬编码** |
| 🔴 高 | 不同项目可能有不同的扩展策略（如 2x、3x），无法配置 |
| 🟠 中 | 可能导致误报（过短/过大） |

---

### 3.2 NUMBER 精度

**位置**: `schema_diff_reconciler.py:1002`

```python
NUMBER_STAR_PRECISION = 38  # NUMBER(*) 等价精度
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | Oracle/OB 标准精度，一般不需修改 |

---

### 3.3 LONG 类型映射

**位置**: `schema_diff_reconciler.py:1154-1160`

```python
def map_long_type_to_ob(data_type: Optional[str]) -> str:
    dt = (data_type or "").strip().upper()
    if dt == "LONG":
        return "CLOB"
    if dt == "LONG RAW":
        return "BLOB"
    return dt
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | `LONG → CLOB`、`LONG RAW → BLOB` 是固定映射 |
| 🟡 低 | 这是 Oracle 到 OB 的标准转换，一般正确 |

---

## 四、VIEW 兼容性规则

### 4.1 不支持的 VIEW 名单 (VIEW_UNSUPPORTED_DEFAULT_VIEWS)

**位置**: `schema_diff_reconciler.py:777-780`

```python
VIEW_UNSUPPORTED_DEFAULT_VIEWS: Set[str] = {
    "DBA_DATA_FILES",
    "ALL_RULES",
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 引用这些视图的 VIEW 被标记为不支持 |
| 🟠 中 | 可通过 `view_compat_rules_path` 配置文件扩展，但默认值硬编码 |

---

### 4.2 需要权限的 VIEW 名单 (VIEW_PRIVILEGE_DEFAULT_VIEWS)

**位置**: `schema_diff_reconciler.py:781-785`

```python
VIEW_PRIVILEGE_DEFAULT_VIEWS: Set[str] = {
    "DBA_JOBS",
    "DBA_OBJECTS",
    "DBA_SOURCE",
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 引用这些视图的 VIEW 需要额外权限检查 |

---

### 4.3 VIEW 不支持模式 (VIEW_UNSUPPORTED_PATTERNS)

**位置**: `schema_diff_reconciler.py:786-788`

```python
VIEW_UNSUPPORTED_PATTERNS: Tuple[str, ...] = (
    r'(?<!\w)"?SYS"?\s*\.\s*"?OBJ\$"?(?!\w)',
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 引用 `SYS.OBJ$` 的 VIEW 被标记为不支持 |
| 🟠 中 | 正则模式硬编码，新增不支持模式需改代码 |

---

### 4.4 X$ 内部视图模式 (VIEW_X_DOLLAR_PATTERN)

**位置**: `schema_diff_reconciler.py:789-792`

```python
VIEW_X_DOLLAR_PATTERN = re.compile(
    r'(?<![A-Z0-9_\$#])"?X\$[A-Z0-9_#$]+"?(?![A-Z0-9_\$#])',
    flags=re.IGNORECASE
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 引用 `X$*` 内部视图的 VIEW 被标记 |

---

### 4.5 DBLINK 策略

**位置**: `schema_diff_reconciler.py:793`

```python
VIEW_DBLINK_POLICIES: Set[str] = {"block", "allow"}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 仅支持两种策略，可通过配置选择 |

---

## 五、约束校验规则

### 5.1 不支持的约束类型

**位置**: `schema_diff_reconciler.py:1512-1533`

```python
def classify_unsupported_constraint(cons_meta: Optional[Dict]) -> Optional[Tuple[str, str, str]]:
    # ...
    # DEFERRABLE/DEFERRED 约束不支持
    if deferrable in ("DEFERRABLE", "DEFERRED"):
        return (
            reason_code,
            f"{label} 约束为 DEFERRABLE/DEFERRED，OceanBase 不支持。",
            "ORA-00900"
        )
    # CHECK 中含 SYS_CONTEXT('USERENV', ...) 不支持
    if ctype == "C":
        if expr and CHECK_SYS_CONTEXT_USERENV_RE.search(str(expr)):
            return (
                "CHECK_SYS_CONTEXT",
                "CHECK 约束包含 SYS_CONTEXT('USERENV', ...)，OceanBase 不支持。",
                "ORA-02436"
            )
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🔴 高 | **DEFERRABLE/DEFERRED 约束被硬判定为不支持** |
| 🔴 高 | **CHECK 约束中的 SYS_CONTEXT 被硬判定为不支持** |
| 🟠 中 | OB 新版本支持后需修改代码 |

---

### 5.2 SYS_CONTEXT 检测正则

**位置**: `schema_diff_reconciler.py:1455`

```python
CHECK_SYS_CONTEXT_USERENV_RE = re.compile(
    r"SYS_CONTEXT\s*\(\s*['\"]USERENV['\"]", 
    flags=re.IGNORECASE
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 仅检测 USERENV 命名空间，其他命名空间可能漏判 |

---

## 六、黑名单/阻断规则

### 6.1 黑名单类型原因 (BLACKLIST_REASON_BY_TYPE)

**位置**: `schema_diff_reconciler.py:761-769`

```python
BLACKLIST_REASON_BY_TYPE: Dict[str, str] = {
    'SPE': "表字段存在不支持的类型，不支持创建，不需要生成DDL",
    'TEMP_TABLE': "临时表，不支持创建，不需要生成DDL",
    'TEMPORARY_TABLE': "源表是临时表，不需要生成DDL",
    'DIY': "表中字段存在自定义类型，不支持创建，不需要生成DDL",
    'LOB_OVERSIZE': "表中存在的LOB字段体积超过512 MiB，可以在目标端创建表，但是 OMS 不支持同步",
    'LONG': "LONG/LONG RAW 需转换为 CLOB/BLOB",
    'DBLINK': "源表可能是 IOT 表或者外部表，不需要生成DDL"
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 黑名单类型代码和描述硬编码 |
| 🟠 中 | 新增黑名单类型需修改代码 |

---

### 6.2 黑名单模式

**位置**: `schema_diff_reconciler.py:770`

```python
BLACKLIST_MODES: Set[str] = {"auto", "table_only", "rules_only", "disabled"}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 可通过配置选择模式 |

---

### 6.3 LOB 大小限制默认值

**位置**: `schema_diff_reconciler.py:3036`, `3173-3177`

```python
settings.setdefault('blacklist_lob_max_mb', '512')

settings['blacklist_lob_max_mb'] = int(settings.get('blacklist_lob_max_mb', '512'))
if settings['blacklist_lob_max_mb'] <= 0:
    settings['blacklist_lob_max_mb'] = 512
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 默认 512 MiB，**可通过配置修改** |

---

## 七、Fixup 生成规则

### 7.1 支持 CREATE OR REPLACE 的类型 (FIXUP_CREATE_REPLACE_TYPES)

**位置**: `schema_diff_reconciler.py:828-838`

```python
FIXUP_CREATE_REPLACE_TYPES: Set[str] = {
    'VIEW',
    'PROCEDURE',
    'FUNCTION',
    'PACKAGE',
    'PACKAGE BODY',
    'TRIGGER',
    'TYPE',
    'TYPE BODY',
    'SYNONYM',
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 符合 Oracle/OB DDL 语法，一般不需修改 |

---

### 7.2 自动补权限类型 (FIXUP_AUTO_GRANT_DEFAULT_TYPES_ORDERED)

**位置**: `schema_diff_reconciler.py:842-854`

```python
FIXUP_AUTO_GRANT_DEFAULT_TYPES_ORDERED: Tuple[str, ...] = (
    'VIEW',
    'MATERIALIZED VIEW',
    'SYNONYM',
    'PROCEDURE',
    'FUNCTION',
    'PACKAGE',
    'PACKAGE BODY',
    'TRIGGER',
    'TYPE',
    'TYPE BODY',
)
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 可通过 `fixup_auto_grant_types` 配置覆盖 |

---

## 八、DDL Hint 过滤规则

### 8.1 Hint 策略

**位置**: `schema_diff_reconciler.py:2742-2752`

```python
DDL_HINT_POLICY_DROP_ALL = "drop_all"
DDL_HINT_POLICY_KEEP_SUPPORTED = "keep_supported"
DDL_HINT_POLICY_KEEP_ALL = "keep_all"
DDL_HINT_POLICY_REPORT_ONLY = "report_only"
DDL_HINT_POLICY_VALUES = {
    DDL_HINT_POLICY_DROP_ALL,
    DDL_HINT_POLICY_KEEP_SUPPORTED,
    DDL_HINT_POLICY_KEEP_ALL,
    DDL_HINT_POLICY_REPORT_ONLY
}
DDL_HINT_POLICY_DEFAULT = DDL_HINT_POLICY_KEEP_SUPPORTED
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 可通过 `ddl_hint_policy` 配置 |

---

### 8.2 OB Oracle 模式支持的 Hint 白名单 (OB_ORACLE_HINT_ALLOWLIST)

**位置**: `schema_diff_reconciler.py:2804-2940`（约 140 个 Hint）

```python
OB_ORACLE_HINT_ALLOWLIST: Set[str] = {
    "AGGR_FIRST_UNNEST",
    "APPEND",
    "BEGIN_OUTLINE_DATA",
    # ... 约 140 个 Hint
    "USE_PX",
    "USE_SPF",
    "USE_TOPO",
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🔴 高 | **140+ 个 Hint 硬编码** |
| 🔴 高 | OB 新版本新增 Hint 需要修改代码 |
| 🟠 中 | 可通过 `ddl_hint_allowlist` 和 `ddl_hint_allowlist_file` 配置扩展 |

---

## 九、权限过滤规则

### 9.1 支持的对象权限 (DEFAULT_SUPPORTED_OBJECT_PRIVS)

**位置**: `schema_diff_reconciler.py:2662-2678`

```python
DEFAULT_SUPPORTED_OBJECT_PRIVS: Set[str] = {
    'SELECT',
    'INSERT',
    'UPDATE',
    'DELETE',
    'ALTER',
    'INDEX',
    'REFERENCES',
    'EXECUTE',
    'DEBUG',
    'READ',
    'WRITE',
    'FLASHBACK',
    'ON COMMIT REFRESH',
    'QUERY REWRITE',
    'UNDER',
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟠 中 | 可通过 `grant_supported_object_privs` 配置覆盖 |
| 🟠 中 | 不在此列表中的权限会被过滤 |

---

### 9.2 系统权限到对象权限映射 (SYS_PRIV_TO_OBJ_PRIV_MAP)

**位置**: `schema_diff_reconciler.py:2680-2660`

```python
SYS_PRIV_TO_OBJ_PRIV_MAP: Dict[str, Set[str]] = {
    'SELECT ANY TABLE': {'SELECT'},
    'INSERT ANY TABLE': {'INSERT'},
    'UPDATE ANY TABLE': {'UPDATE'},
    'DELETE ANY TABLE': {'DELETE'},
    # ...
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 映射关系硬编码，一般符合 Oracle 语义 |

---

### 9.3 角色认证类型告警 (ROLE_AUTH_WARN_TYPES)

**位置**: `schema_diff_reconciler.py:2671-2675`

```python
ROLE_AUTH_WARN_TYPES: Set[str] = {
    'PASSWORD',
    'EXTERNAL',
    'GLOBAL'
}
```

| 风险等级 | 风险描述 |
|----------|----------|
| 🟡 低 | 仅用于告警提示 |

---

## 十、批量/性能常量

**位置**: `schema_diff_reconciler.py:857-864`, `1002`

| 常量名 | 值 | 位置 | 说明 | 风险 |
|--------|-----|------|------|------|
| `COMMENT_BATCH_SIZE` | 200 | 858 | 注释对比批量大小 | 🟡 低 |
| `ORACLE_IN_BATCH_SIZE` | 900 | 860 | Oracle IN 列表限制（避免 ORA-01795） | 🟡 低 |
| `GRANT_WARN_THRESHOLD` | 200000 | 862 | 授权规模告警阈值 | 🟡 低 |
| `EXTRA_CHECK_PROCESS_MAX_TABLES` | 2000 | 864 | 多进程扩展校验阈值 | 🟡 低 |
| `NUMBER_STAR_PRECISION` | 38 | 1002 | NUMBER(*) 等价精度 | 🟡 低 |

---

## 十一、正则表达式模式

### 11.1 对象名称/DDL 解析模式

| 模式名 | 位置 | 用途 | 风险 |
|--------|------|------|------|
| `QUALIFIED_NAME_PATTERN` | 17255-17258 | 解析 `SCHEMA.OBJECT` 格式 | 🟡 低 |
| `USING_INDEX_PATTERN_*` | 17696-17707 | 解析 USING INDEX 子句 | 🟡 低 |
| `MV_REFRESH_ON_DEMAND_PATTERN` | 17708 | 识别 MVIEW ON DEMAND | 🟡 低 |
| `END_SCHEMA_PREFIX_PATTERN` | 17755-17758 | 清理 END 语句 schema 前缀 | 🟡 低 |
| `FOR_LOOP_RANGE_SINGLE_DOT_PATTERN` | 17772-17775 | 修复 FOR LOOP 单点号 | 🟡 低 |
| `DELIMITER_LINE_PATTERN` | 17662 | 识别 DELIMITER 行 | 🟡 低 |
| `TRIGGER_QUALIFIED_REF_PATTERN` | 17961-17964 | 解析触发器引用 | 🟡 低 |

---

## 十二、风险评估与建议

### 12.1 高风险项（需优先改造）

| 序号 | 规则 | 位置 | 风险 | 建议 |
|------|------|------|------|------|
| 1 | `IGNORED_OMS_COLUMNS` | 867-872 | 不使用 OMS 时误过滤 | 添加开关 `enable_oms_column_filter` |
| 2 | `VARCHAR_LEN_*_MULTIPLIER` | 1000-1001 | 无法配置倍数 | 添加配置项 `varchar_len_min_multiplier` / `varchar_len_max_multiplier` |
| 3 | DEFERRABLE 约束判定 | 1512-1525 | OB 新版本可能支持 | 添加版本判断或配置开关 |
| 4 | `OB_ORACLE_HINT_ALLOWLIST` | 2804-2940 | 140+ Hint 硬编码 | 改为配置文件加载 |
| 5 | `PRIMARY_OBJECT_TYPES` | 693-706 | 无法扩展对象类型 | 改为配置文件加载 |

---

### 12.2 中风险项（建议改造）

| 序号 | 规则 | 位置 | 风险 | 建议 |
|------|------|------|------|------|
| 1 | `PRINT_ONLY_PRIMARY_TYPES` | 708-714 | MVIEW 无法校验 | 添加配置项 |
| 2 | `VIEW_UNSUPPORTED_*` | 777-788 | 默认值硬编码 | 已支持配置文件，需文档说明 |
| 3 | `SYS_IOT_OVER_*` 跳过 | 5310-5371 | 无法关闭 | 添加开关 |
| 4 | `AUTO_SEQUENCE_PATTERNS` | 878-880 | 序列误过滤 | 添加开关 |
| 5 | `is_oms_index` | 1162-1179 | 索引误过滤 | 添加开关 |
| 6 | `CHECK_SYS_CONTEXT` | 1455, 1527-1532 | 约束误判 | 添加开关 |

---

### 12.3 低风险项（可保持）

| 序号 | 规则 | 理由 |
|------|------|------|
| 1 | `NUMBER_STAR_PRECISION = 38` | Oracle/OB 标准 |
| 2 | `LONG → CLOB` 映射 | 标准转换 |
| 3 | `ORACLE_IN_BATCH_SIZE = 900` | Oracle 限制 |
| 4 | `OBNOTNULL` 过滤 | OB 自动生成，正确行为 |
| 5 | `SYS_NC_*` / `SYS_C_*` 模式 | Oracle 内部对象 |
| 6 | `PLSQL_ORDER_PRIORITY` | 依赖顺序正确 |

---

### 12.4 改造优先级建议

```
Phase 1 (紧急):
├── IGNORED_OMS_COLUMNS → 添加开关
├── VARCHAR_LEN_*_MULTIPLIER → 添加配置
└── PRIMARY_OBJECT_TYPES → 考虑配置化

Phase 2 (重要):
├── OB_ORACLE_HINT_ALLOWLIST → 外部文件
├── DEFERRABLE 约束 → 版本判断
└── PRINT_ONLY_PRIMARY_TYPES → 配置化

Phase 3 (优化):
├── SYS_IOT_OVER 跳过 → 添加开关
├── VIEW_UNSUPPORTED_* → 完善文档
└── 其他中风险项 → 按需处理
```

---

## 附录：配置项与硬编码对照表

| 规则类别 | 当前状态 | 是否可配置 | 配置项名称 |
|----------|----------|------------|------------|
| 主对象类型 | 硬编码 | ✅ 部分 | `check_primary_types` |
| 扩展对象类型 | 硬编码 | ✅ 部分 | `check_extra_types` |
| OMS 列忽略 | 硬编码 | ❌ 否 | 无 |
| VARCHAR 倍数 | 硬编码 | ❌ 否 | 无 |
| VIEW 不支持列表 | 硬编码默认值 | ✅ 是 | `view_compat_rules_path` |
| Hint 白名单 | 硬编码默认值 | ✅ 部分 | `ddl_hint_allowlist` / `ddl_hint_allowlist_file` |
| 对象权限 | 硬编码默认值 | ✅ 是 | `grant_supported_object_privs` |
| LOB 大小限制 | 硬编码默认值 | ✅ 是 | `blacklist_lob_max_mb` |
| 黑名单模式 | 硬编码选项 | ✅ 是 | `blacklist_mode` |

---

> **结论**: 当前代码中约有 **50+ 处硬编码规则**，其中 **5 项高风险**、**6 项中风险**。新项目迁移时，需特别关注 OMS 列过滤、VARCHAR 倍数、约束类型判定 等规则，建议优先改造为可配置项。
