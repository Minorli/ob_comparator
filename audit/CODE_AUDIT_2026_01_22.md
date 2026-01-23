# 全量代码审核报告

**审核日期**: 2026-01-22  
**代码版本**: commit 6e6a99d  
**审核范围**: 全代码库  

---

## 1. 代码库概览

| 文件 | 行数 | 说明 |
|------|------|------|
| `schema_diff_reconciler.py` | ~22,400 | 核心对比逻辑 |
| `run_fixup.py` | ~3,400 | 修复脚本执行器 |
| `test_schema_diff_reconciler.py` | ~3,000 | 单元测试 |
| `init_users_roles.py` | ~25,000 | 用户角色初始化 |
| 其他辅助文件 | - | 配置、工具等 |

**总函数数**: schema_diff_reconciler.py 308个, run_fixup.py 82个

---

## 2. 业务逻辑审查

### 2.1 Remap 映射逻辑 ✅

**位置**: `build_full_object_mapping()`, `generate_master_list()`

**审查结论**: 
- 支持多种 remap 场景：1:1, N:1, 1:N
- 依赖图递归推导目标 schema 已实现
- `transitive_table_cache` 预计算优化性能
- `remap_conflicts` 记录无法自动推导的对象

**潜在风险**:
- ⚠️ 复杂的 one-to-many remap 场景下，依赖推导可能因循环依赖导致无限递归（已通过 `visited` 集合保护）

### 2.2 依赖图构建 ✅

**位置**: `build_dependency_graph()`, `build_blocked_dependency_map()`

**审查结论**:
- BFS 遍历构建反向依赖图
- 正确处理 TRIGGER 等依附对象的父表关系
- 阻断传播逻辑正确

**已修复问题**:
- ✅ LONG 表黑名单逻辑已优化，校验转换状态后不再阻断依赖

### 2.3 DDL 生成逻辑 ✅

**位置**: `generate_fixup_scripts()`, `fetch_dbcat_schema_objects()`

**审查结论**:
- 支持 dbcat 批量导出 + DBMS_METADATA 兜底
- DDL 清理规则按对象类型分层应用
- 并发导出支持 (ThreadPoolExecutor)

**潜在风险**:
- ⚠️ dbcat 超时后进程 kill 可能留下僵尸进程（已有超时保护）
- ⚠️ MATERIALIZED VIEW 不支持 dbcat 自动导出，需手工处理

### 2.4 Grant 计划生成 ✅

**位置**: `build_grant_plan()`

**审查结论**:
- 支持对象权限、系统权限、角色授权
- 过滤不兼容的 Oracle 权限
- 支持 OB 现有角色跳过

---

## 3. 代码逻辑审查

### 3.1 异常处理 ⚠️

**发现问题**:

| 级别 | 位置 | 问题 | 建议 |
|------|------|------|------|
| P2 | `obclient_run_sql()` | 捕获 `Exception` 后仅记录日志，未区分可恢复/不可恢复错误 | 细化异常类型 |
| P2 | `oracle_get_ddl_batch()` | 批量获取 DDL 失败时回退单个获取，但未限制重试次数 | 添加重试限制 |
| P3 | 多处 `sys.exit(1)` | 直接退出不利于作为库使用 | 考虑抛出自定义异常 |

### 3.2 边界条件处理 ✅

**审查结论**:
- 空列表/空集合检查完善
- schema/object 名称大小写统一处理 (`.upper()`)
- IN 子句拆分避免超过 1000 限制

### 3.3 并发安全 ✅

**位置**: `fetch_dbcat_schema_objects()`, `generate_fixup_scripts()`

**审查结论**:
- 使用 `threading.Lock()` 保护共享数据
- `ThreadPoolExecutor` 正确使用 `as_completed()`
- `error_occurred` Event 用于快速取消

### 3.4 资源管理 ✅

**审查结论**:
- 数据库连接使用 `with` 语句自动关闭
- 临时文件使用 `tempfile.TemporaryFile()`
- 进程超时后有 kill 处理

---

## 4. 代码质量审查

### 4.1 代码风格 ⚠️

**发现问题**:

| 级别 | 问题 | 位置 | 建议 |
|------|------|------|------|
| P3 | 单文件过大 (22,400行) | `schema_diff_reconciler.py` | 考虑按功能拆分模块 |
| P3 | 重复的日志初始化代码 | 两个主文件 | 提取为共享模块 |
| P3 | 部分函数过长 (>200行) | `dump_oracle_metadata()`, `generate_fixup_scripts()` | 拆分为小函数 |
| P4 | 混用 f-string 和 .format() | 全局 | 统一使用 f-string |

### 4.2 类型注解 ✅

**审查结论**:
- 核心函数有完整的类型注解
- 使用 `NamedTuple` 和 `@dataclass` 定义结构化数据
- `Optional`, `Dict`, `Set` 等泛型正确使用

### 4.3 文档与注释 ✅

**审查结论**:
- 模块级文档完整，说明功能和版本历史
- 关键函数有 docstring
- 中文注释便于理解业务逻辑

### 4.4 测试覆盖 ⚠️

**位置**: `test_schema_diff_reconciler.py`

**审查结论**:
- 测试用例约 3,000 行，覆盖纯函数
- 使用 `unittest.mock` 模拟外部依赖
- 新增 `test_integration_visibility.py` 集成测试

**不足**:
- ⚠️ 缺少 `run_fixup.py` 的集成测试
- ⚠️ 缺少端到端测试 (E2E)

---

## 5. 安全审查

### 5.1 SQL 注入风险 ✅

**审查结论**:
- Oracle 查询使用绑定变量 (`build_bind_placeholders()`)
- OceanBase 查询使用字符串拼接但限于内部 schema/object 名称（已 `.upper()` 处理）
- 无用户直接输入拼接到 SQL

### 5.2 凭证处理 ⚠️

**发现问题**:

| 级别 | 问题 | 建议 |
|------|------|------|
| P2 | 密码通过命令行传递 (`-p` + password) | 进程列表可见密码，建议使用环境变量或配置文件 |
| P3 | 日志中可能泄露连接字符串 | 确保敏感信息不写入日志 |

---

## 6. 性能审查

### 6.1 内存使用 ⚠️

**潜在风险**:
- 大规模 schema 场景下，`full_object_mapping` 和 `dependency_graph` 可能占用大量内存
- 建议: 对于超大规模迁移，考虑分批处理

### 6.2 数据库查询 ✅

**审查结论**:
- "Dump-Once, Compare-Locally" 架构减少数据库往返
- IN 子句分块避免 SQL 过长
- 批量查询优化 (如 `oracle_get_ddl_batch()`)

---

## 7. 发现的具体问题

### P1 级 (需立即修复)

无

### P2 级 (建议修复)

| ID | 问题 | 位置 | 修复建议 |
|----|------|------|----------|
| P2-1 | `run_view_chain_autofix` 忽略 `--only-dirs` 参数 | run_fixup.py:2824-2833 | **已修复** (commit 1d54b44) |
| P2-2 | 密码通过命令行传递可见 | obclient 调用 | 使用 `--password-stdin` 或环境变量 |
| P2-3 | 异常处理过于宽泛 | 多处 `except Exception` | 细化异常类型 |
| P2-4 | 约束统计遗漏 CHECK 约束 | 第 5488 行 `_count_pkukfk()` | 添加 `'C'` 到条件 |
| P2-5 | `GRANT_PRIVILEGE_BY_TYPE` 遗漏对象类型 | 第 1760-1772 行 | 补充 TRIGGER/JOB/SCHEDULE/INDEX |

### P3 级 (建议改进)

| ID | 问题 | 位置 | 改进建议 |
|----|------|------|----------|
| P3-1 | 单文件过大 | schema_diff_reconciler.py | 拆分为多个模块 |
| P3-2 | 日志代码重复 | 两个主文件 | 提取共享模块 |
| P3-3 | 缺少 run_fixup.py 测试 | test_run_fixup.py | 扩展测试覆盖 |
| P3-4 | 文档/注释遗漏 CHECK 约束 | 第 26/11479/21274 行 | 改为 `(PK/UK/FK/CK)` |

---

## 8. 对象类型覆盖深度审查

### 8.1 约束类型 (CONSTRAINT) 遗漏分析

#### 发现问题

| 位置 | 问题 | 影响 |
|------|------|------|
| 第 5488 行 | `_count_pkukfk()` 只统计 `('P', 'U', 'R')`，遗漏 `'C'` | 报告中约束数量统计不完整 |
| 第 26 行 | 文档写 `CONSTRAINT (PK/UK/FK)` | 文档不准确 |
| 第 11479 行 | 注释写 `约束 (PK/UK/FK)` | 文档不准确 |
| 第 21274 行 | 报告标题写 `6. 约束 (PK/UK/FK) 一致性检查` | 报告标题不准确 |

#### 已正确实现

| 位置 | 说明 |
|------|------|
| SQL 查询 (6130/6145/6158/7294 行) | `CONSTRAINT_TYPE IN ('P','U','R','C')` ✅ |
| `bucket_check()` (10790 行) | 正确处理 `ctype == "C"` ✅ |
| `match_check_constraints()` (10923 行) | CHECK 约束匹配逻辑完整 ✅ |
| 修复脚本生成 (18742-18750 行) | `ADD CONSTRAINT ... CHECK (...)` ✅ |

### 8.2 权限映射 (GRANT_PRIVILEGE_BY_TYPE) 遗漏分析

#### 当前定义 (第 1760-1772 行)

```python
GRANT_PRIVILEGE_BY_TYPE: Dict[str, str] = {
    'TABLE': 'SELECT',
    'VIEW': 'SELECT',
    'MATERIALIZED VIEW': 'SELECT',
    'SYNONYM': 'SELECT',
    'SEQUENCE': 'SELECT',
    'TYPE': 'EXECUTE',
    'TYPE BODY': 'EXECUTE',
    'PROCEDURE': 'EXECUTE',
    'FUNCTION': 'EXECUTE',
    'PACKAGE': 'EXECUTE',
    'PACKAGE BODY': 'EXECUTE'
}
```

#### 遗漏的对象类型

| 对象类型 | 建议权限 | 影响 |
|----------|----------|------|
| `TRIGGER` | 无需单独授权（跟随父表） | 低 - 代码已特殊处理 |
| `INDEX` | 无需单独授权（跟随父表） | 低 |
| `JOB` | `EXECUTE` 或无 | 中 - 跨 schema JOB 可能需要 |
| `SCHEDULE` | `EXECUTE` 或无 | 中 - 跨 schema SCHEDULE 可能需要 |

**评估**：TRIGGER 和 INDEX 的授权已在代码中特殊处理（使用父表的 SELECT 权限），JOB/SCHEDULE 的跨 schema 授权场景较少见，影响有限。

### 8.3 INVALID 状态类型 (INVALID_STATUS_TYPES) 审查

#### 当前定义 (第 638-647 行)

```python
INVALID_STATUS_TYPES: Set[str] = {
    'VIEW', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY',
    'TYPE', 'TYPE BODY', 'TRIGGER'
}
```

#### 审查结论

- ✅ 覆盖了所有可能出现 INVALID 状态的 PL/SQL 对象
- ⚠️ `MATERIALIZED VIEW` 也可能有 INVALID 状态，但当前标记为 `PRINT_ONLY`，不参与校验

### 8.4 对象类型常量统一性审查

#### 常量定义完整性

| 常量 | 定义位置 | 完整性 |
|------|----------|--------|
| `PRIMARY_OBJECT_TYPES` | 第 601-614 行 | ✅ 完整 |
| `EXTRA_OBJECT_CHECK_TYPES` | 第 695-700 行 | ✅ 完整 |
| `ALL_TRACKED_OBJECT_TYPES` | 第 691-693 行 | ✅ 自动合并 |
| `OBJECT_COUNT_TYPES` | 第 1030-1047 行 | ✅ 完整 |
| `GRANT_PRIVILEGE_BY_TYPE` | 第 1760-1772 行 | ⚠️ 缺少 JOB/SCHEDULE |

### 8.5 根因分析

**问题根源**：开发时对某些对象特性认知不完整，导致硬编码列表遗漏。

**典型模式**：
1. 约束只考虑了 PK/UK/FK，遗漏了 CK
2. 权限映射只考虑了常见对象，遗漏了 JOB/SCHEDULE

**改进建议**：
1. 建立对象类型枚举常量，避免多处硬编码
2. 添加单元测试覆盖所有对象类型
3. 代码审查时关注"列表完整性"

---

## 9. 依赖链处理深度审查

### 9.1 当前实现概述

程序对不同对象类型的依赖链处理存在差异：

| 对象类型 | 依赖链处理 | 拓扑排序 | 授权联动 | 评估 |
|----------|------------|----------|----------|------|
| **VIEW** | ✅ 完整 | ✅ `topo_sort_nodes` | ✅ `build_view_chain_plan` | 优秀 |
| **PACKAGE/BODY** | ✅ 有 | ✅ `_order_package_fixups` | ❌ 无 | 良好 |
| **TYPE/BODY** | ❌ **遗漏** | ❌ 应复用 PACKAGE 逻辑 | ❌ 无 | **需修复** |
| **PROCEDURE** | ❌ 无 | ❌ 仅静态层级 | ❌ 无 | 需改进 |
| **FUNCTION** | ❌ 无 | ❌ 仅静态层级 | ❌ 无 | 需改进 |
| **TRIGGER** | ❌ 无 | ❌ 仅静态层级 | ❌ 无 | 需改进 |

### 9.2 VIEW 依赖链处理 (正面案例)

**实现位置**：
- `schema_diff_reconciler.py:8298` - `build_view_fixup_chains()`
- `run_fixup.py:906` - `topo_sort_nodes()`
- `run_fixup.py:1337` - `build_view_chain_plan()`
- `run_fixup.py:2775` - `run_view_chain_autofix()`

**核心逻辑**：
1. 从 `DBA_DEPENDENCIES` 构建依赖图
2. 对缺失 VIEW 进行拓扑排序（依赖对象先创建）
3. 自动规划授权语句（跨 schema 访问）
4. 按正确顺序执行 DDL

```
VIEW_A -> TABLE_B -> GRANT -> CREATE VIEW_A
         VIEW_C  -> GRANT -> CREATE VIEW_C -> CREATE VIEW_A
```

### 9.3 发现的问题

#### P1-1: DEPENDENCY_LAYERS 顺序错误 (严重)

**位置**: `run_fixup.py:309-322`

```python
DEPENDENCY_LAYERS = [
    ...
    ["procedure", "function"],    # Layer 6
    ["package", "type"],          # Layer 7  ← TYPE 在 FUNCTION 之后!
    ["package_body", "type_body"], # Layer 8
    ...
]
```

**问题**: FUNCTION 可能依赖 TYPE，但 TYPE 在 Layer 7，执行顺序晚于 FUNCTION (Layer 6)。

**场景示例**:
```sql
-- 缺失对象1: TYPE user_type
CREATE TYPE user_type AS OBJECT (...);

-- 缺失对象2: FUNCTION get_user (依赖 user_type)
CREATE FUNCTION get_user RETURN user_type IS ...
```

**当前行为**: FUNCTION 先执行，因 TYPE 不存在而失败。

**修复建议**: 调整层级顺序或实现全对象拓扑排序。

#### P2-6: TYPE/TYPE BODY 遗漏拓扑排序 (与 PACKAGE 同类问题)

**位置**: `schema_diff_reconciler.py:624-627` 和 `17519-17584`

**问题**: `_order_package_fixups` 只处理 `PACKAGE_OBJECT_TYPES`，不包含 TYPE：

```python
PACKAGE_OBJECT_TYPES: Tuple[str, ...] = (
    'PACKAGE',
    'PACKAGE BODY'
)  # ← 遗漏了 'TYPE', 'TYPE BODY'
```

**场景示例**:
```sql
-- TYPE A 依赖 TYPE B
CREATE TYPE type_a AS OBJECT (ref type_b);
CREATE TYPE type_b AS OBJECT (...);
```

**修复建议**: 扩展常量或创建独立的 `_order_type_fixups` 函数。

#### P2-7: PROCEDURE/FUNCTION/TRIGGER 缺少层内拓扑排序

**影响对象**: PROCEDURE, FUNCTION, TRIGGER

**问题**: 同一层级内的对象按字母序执行，不考虑相互依赖。

**场景示例**:
```sql
-- PROCEDURE A 依赖 PROCEDURE B
CREATE PROCEDURE proc_a AS BEGIN proc_b; END;
CREATE PROCEDURE proc_b AS BEGIN NULL; END;
```

**当前行为**: `proc_a` 先执行（字母序），因 `proc_b` 不存在而失败。

**缓解措施**: 使用 `--iterative` 模式可多轮重试，但效率低。

#### P2-8: 非 VIEW 对象缺少授权联动

**问题**: VIEW 的 `build_view_chain_plan` 会自动规划授权，但其他对象没有。

**场景示例**:
```sql
-- SCHEMA_A.PROC_A 调用 SCHEMA_B.PROC_B
-- 需要: GRANT EXECUTE ON SCHEMA_B.PROC_B TO SCHEMA_A
```

**当前行为**: 需手动执行 `grants_miss` 目录下的授权脚本。

### 9.4 缓解机制评估

| 机制 | 描述 | 有效性 |
|------|------|--------|
| `--iterative` | 多轮重试失败脚本 | ⚠️ 可行但低效 |
| `--smart-order` | 启用依赖层级排序 | ⚠️ 仅解决跨类型问题 |
| `--recompile` | 自动重编译 INVALID 对象 | ⚠️ 仅处理已存在对象 |
| `--view-chain-autofix` | VIEW 专用拓扑排序 | ✅ 仅限 VIEW |

### 9.5 改进建议

1. **短期**: 调整 `DEPENDENCY_LAYERS` 顺序，将 TYPE 移至 FUNCTION 之前
2. **中期**: 为 PROCEDURE/FUNCTION/TYPE 实现类似 VIEW 的拓扑排序
3. **长期**: 统一所有对象类型的依赖链处理框架

**建议的层级顺序调整**:
```python
DEPENDENCY_LAYERS = [
    ["sequence"],                    # Layer 0
    ["table"],                       # Layer 1
    ["table_alter"],                 # Layer 2
    ["grants"],                      # Layer 3
    ["type"],                        # Layer 4: TYPE 先于 FUNCTION
    ["view", "synonym"],             # Layer 5
    ["materialized_view"],           # Layer 6
    ["procedure", "function"],       # Layer 7
    ["package"],                     # Layer 8
    ["package_body", "type_body"],   # Layer 9
    ["constraint", "index"],         # Layer 10
    ["trigger"],                     # Layer 11
    ["job", "schedule"],             # Layer 12
]
```

---

## 10. Autofix 机制审查

### 10.1 view_chain_autofix 实现审查

**位置**: `run_fixup.py:1337-1480` `build_view_chain_plan()`

**审查结论**: ✅ 实现正确

| 功能点 | 实现 | 评估 |
|--------|------|------|
| 拓扑排序 | `topo_sort_nodes()` | ✅ 正确 |
| 循环检测 | 检测并阻止执行 | ✅ 正确 |
| 授权规划 | 自动生成跨 schema GRANT | ✅ 正确 |
| GRANT OPTION | 处理级联授权场景 | ✅ 正确 |
| DDL 收集 | 按依赖顺序收集 | ✅ 正确 |

### 10.2 其他对象类型 autofix 可行性

| 对象类型 | 可行性 | 复杂度 | 建议 |
|----------|--------|--------|------|
| PROCEDURE/FUNCTION | ✅ 高 | 中 | 复用 VIEW 拓扑排序框架 |
| PACKAGE/BODY | ✅ 高 | 低 | 已有 `_order_package_fixups`，可扩展 |
| TYPE/BODY | ✅ 高 | 低 | 与 PACKAGE 逻辑相同，可复用 |
| TRIGGER | ⚠️ 中 | 低 | 依赖表，通常表已存在 |
| SYNONYM | ⚠️ 中 | 中 | 需处理 PUBLIC 特殊情况 |

### 10.3 统一 autofix 框架建议

```python
def run_plsql_chain_autofix(
    object_type: str,  # 'PROCEDURE', 'FUNCTION', 'TYPE', 'PACKAGE'
    chains: List[List[Tuple[str, str]]],
    ...
) -> None:
    # 1. 解析依赖链文件
    # 2. 构建依赖图 (复用 build_view_dependency_graph)
    # 3. 拓扑排序 (复用 topo_sort_nodes)
    # 4. 规划授权 (EXECUTE 权限)
    # 5. 按序执行 DDL
```

---

## 11. PUBLIC 同义词处理审查

### 11.1 预期行为

PUBLIC 同义词 DDL 应不含 schema 前缀:
```sql
-- 正确
CREATE OR REPLACE PUBLIC SYNONYM MY_SYN FOR SCHEMA_A.TABLE_A;

-- 错误
CREATE OR REPLACE PUBLIC SYNONYM PUBLIC.MY_SYN FOR SCHEMA_A.TABLE_A;
```

### 11.2 代码审查结果

**已有保护机制**:

1. **META_SYN 路径** (`schema_diff_reconciler.py:16811-16812`):
   ```python
   if syn_meta.owner == 'PUBLIC':
       ddl = f"CREATE OR REPLACE PUBLIC SYNONYM {syn_name} FOR {target};"
   ```
   ✅ 正确生成，无 schema 前缀

2. **adjust_ddl_for_object** (`schema_diff_reconciler.py:14043-14046`):
   ```python
   if obj_type.upper() == 'SYNONYM' and tgt_schema_u == 'PUBLIC':
       return result  # 跳过 qualify_main_object_creation
   ```
   ✅ 跳过添加 schema 前缀

3. **normalize_public_synonym_name** (`schema_diff_reconciler.py:13582-13598`):
   ```python
   # 移除 DDL 中可能存在的 schema 前缀
   pattern = r'(CREATE\s+(?:OR\s+REPLACE\s+)?PUBLIC\s+SYNONYM\s+)(?:"?[A-Z0-9_\$#]+"?\s*\.)?"?...'
   ```
   ✅ 作为兜底清理

### 11.3 潜在问题点

**待确认**: 用户报告的问题可能来自以下场景:
- 文件名格式 `PUBLIC.SYNONYM_NAME.sql` (这是正确的文件命名)
- 或特定 DBMS_METADATA 返回格式未被 `normalize_public_synonym_name` 正则覆盖

**建议**: 如遇到具体问题文件，请提供 DDL 内容以便精确定位。

---

## 12. 架构建议

### 12.1 模块化拆分建议

```
schema_diff_reconciler/
├── __init__.py
├── config.py          # 配置加载
├── oracle_meta.py     # Oracle 元数据
├── ob_meta.py         # OceanBase 元数据
├── compare.py         # 对比逻辑
├── remap.py           # Remap 映射
├── ddl.py             # DDL 生成
├── grant.py           # Grant 计划
├── report.py          # 报告输出
└── utils.py           # 工具函数
```

### 12.2 测试增强建议

1. 添加 `run_fixup.py` 单元测试
2. 添加端到端测试 (使用 Docker Compose 启动测试数据库)
3. 添加性能基准测试

---

## 13. 深度模块审查

### 13.1 配置加载模块 (`load_config`)

**位置**: `schema_diff_reconciler.py:2052-2394`

**审查结论**: ✅ 实现良好

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 默认值设置 | ✅ | 100+ 配置项有合理默认值 |
| 类型转换 | ✅ | int/bool 转换均有 try/except 保护 |
| 路径验证 | ✅ | `validate_runtime_paths` 全面检查 |
| 必填项检查 | ✅ | `source_schemas` 等关键项有校验 |

**改进建议**: 考虑使用 Pydantic 或 dataclass 进行配置模型化。

### 13.2 错误处理模式

**审查结论**: ✅ 一致性良好

```python
# 标准模式
try:
    value = int(settings.get('key', 'default'))
except (TypeError, ValueError):
    value = default_value
if value <= 0:
    value = default_value
```

**统计**:
- `try/except` 块: 200+ 处
- `sys.exit(1)` 严重错误退出: 20+ 处
- 日志级别使用: `log.error` / `log.warning` / `log.info` / `log.debug` 分层清晰

### 13.3 DDL 清理模块

**位置**: 多个函数分布在 `schema_diff_reconciler.py`

| 函数 | 功能 | 评估 |
|------|------|------|
| `sanitize_plsql_punctuation` | 全角→半角标点 | ✅ 正确，保护字符串字面量 |
| `cleanup_dbcat_wrappers` | 移除 DELIMITER/$$ | ✅ 正确 |
| `prepend_set_schema` | 添加 ALTER SESSION | ✅ 防重复 |
| `normalize_public_synonym_name` | 移除 PUBLIC SYNONYM schema 前缀 | ✅ 正确 |
| `fix_inline_comment_collapse` | 修复行内注释吞行 | ✅ 正确 |
| `clean_plsql_ending` | 清理 PL/SQL 结尾语法 | ✅ 正确 |

### 13.4 并发模块

**审查结论**: ✅ 实现正确，无明显竞态条件

**使用模式**:
```python
# 标准并发模式
results_lock = threading.Lock()
with ThreadPoolExecutor(max_workers=worker_count) as executor:
    for result in executor.map(task_func, tasks):
        with results_lock:
            shared_data.update(result)
```

**锁使用统计**:
- `threading.Lock()`: 10+ 处，保护共享数据
- `threading.Event()`: 1 处，用于错误信号
- 所有 `ThreadPoolExecutor` 使用 `with` 语句确保资源释放

### 13.5 安全审查

#### P3-1: 密码命令行传递 (低风险)

**位置**: `schema_diff_reconciler.py:5540`, `run_fixup.py`

```python
'-p' + ob_cfg['password'],  # 密码直接拼接到命令行
```

**风险**: 
- 进程列表 (`ps aux`) 可能暴露密码
- 日志可能记录命令行

**建议**: 使用环境变量或临时配置文件传递敏感信息。

#### P3-2: 无输入校验的 SQL 拼接

**位置**: 多处动态 SQL 构建

**风险**: 低（仅内部使用，无外部输入）

**现状**: 使用 bind placeholders 的场景已正确处理。

### 13.6 OceanBase 元数据加载模块

**位置**: `schema_diff_reconciler.py:5636-6421` `dump_ob_metadata()`

**审查结论**: ✅ 实现良好

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 空 schema 处理 | ✅ | 返回空 `ObMetadata` 结构 |
| PUBLIC 同义词处理 | ✅ | `__PUBLIC` → `PUBLIC` 转换 |
| TYPE/TYPE BODY 补充 | ✅ | 通过 DBA_TYPES 和 DBA_SOURCE 补充 |
| 查询失败处理 | ✅ | 关键视图失败则 `sys.exit(1)` |
| 分块查询 | ✅ | `obclient_query_by_owner_chunks` 防止 SQL 过长 |

### 13.7 报告生成模块

**位置**: `schema_diff_reconciler.py:20614-21000+` `print_final_report()`

**审查结论**: ✅ 实现良好

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 参数默认值 | ✅ | 所有 Optional 参数有默认空结构 |
| 报告宽度 | ✅ | 可配置 `report_width`，避免 nohup 截断 |
| 详情模式 | ✅ | 支持 `full`/`split` 两种模式 |
| Rich Console | ✅ | 使用 `record=True` 支持文件输出 |

### 13.8 dbcat 集成模块

**位置**: `schema_diff_reconciler.py:12500-12800` `fetch_dbcat_schema_objects()`

**审查结论**: ✅ 实现良好

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 并行导出 | ✅ | `ThreadPoolExecutor` + 可配置 workers |
| 超时处理 | ✅ | `cli_timeout` 可配置，超时后 `proc.kill()` |
| 错误传播 | ✅ | `error_occurred` Event 终止其他任务 |
| 分块处理 | ✅ | `dbcat_chunk_size` 防止命令行过长 |
| MVIEW 跳过 | ✅ | 明确日志提示 dbcat 不支持 |

### 13.9 边界情况处理

**审查结论**: ✅ 整体良好

| 场景 | 处理方式 | 评估 |
|------|----------|------|
| 空列表 | `if not items: return` | ✅ |
| None 值 | `value or ""`, `if value is None` | ✅ |
| 空字符串 | `if not raw_value.strip()` | ✅ |
| 超时 | `TimeoutExpired` 异常捕获 | ✅ |
| OB 特殊约束 | `is_ob_notnull_constraint` 过滤 | ✅ |
| OMS 迁移列 | `is_oms_hidden_column` 忽略 | ✅ |

**潜在改进点**:
- 部分 `split('.')` 未检查结果长度，建议使用 `split('.', 1)` + 解构检查

### 13.10 Oracle 元数据加载模块

**位置**: `schema_diff_reconciler.py:6868-7900+` `dump_oracle_metadata()`

**审查结论**: ✅ 实现良好

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 空 schema 处理 | ✅ | 返回空 `OracleMetadata` 结构 |
| 字段探测 | ✅ | 动态检测 HIDDEN_COLUMN/VIRTUAL_COLUMN 等 |
| 分块查询 | ✅ | `ORACLE_IN_BATCH_SIZE` 防止 IN 子句过长 |
| 连接复用 | ✅ | 单连接内执行所有查询 |
| 特性版本兼容 | ✅ | 低版本 Oracle 自动跳过不支持字段 |

### 13.11 分区表处理模块

**位置**: `schema_diff_reconciler.py:10580-10700+` `generate_interval_partition_statements()`

**审查结论**: ✅ 实现良好

| 检查项 | 状态 | 说明 |
|--------|------|------|
| RANGE 分区校验 | ✅ | 非 RANGE 类型跳过 |
| 单列分区键校验 | ✅ | 多列分区键跳过 |
| 分区名冲突 | ✅ | 自动添加后缀避免重名 |
| 迭代上限 | ✅ | `max_iters=10000` 防止无限循环 |
| 日期/数值双模式 | ✅ | 支持 DATE 和 NUMBER 两种分区类型 |

### 13.12 约束和索引处理

**审查结论**: ✅ 实现良好

| 约束类型 | 处理状态 | 说明 |
|----------|----------|------|
| PRIMARY KEY (P) | ✅ | 正确识别和处理 |
| UNIQUE (U) | ✅ | 正确识别和处理 |
| FOREIGN KEY (R) | ✅ | 支持 DELETE RULE 对比 |
| CHECK (C) | ✅ | 支持，过滤系统 NOT NULL 约束 |
| OB NOTNULL | ✅ | `is_ob_notnull_constraint` 自动过滤 |

**索引处理**:
- 支持普通索引、唯一索引、位图索引
- `is_oms_unique_index` 过滤 OMS 迁移工具索引

---

## 14. 总结

### 整体评价

| 维度 | 评分 | 说明 |
|------|------|------|
| 业务逻辑正确性 | ⭐⭐⭐⭐⭐ | 核心对比和 remap 逻辑正确 |
| 代码质量 | ⭐⭐⭐⭐ | 良好，但文件过大 |
| 异常处理 | ⭐⭐⭐ | 有改进空间 |
| 测试覆盖 | ⭐⭐⭐ | 纯函数覆盖好，集成测试不足 |
| 安全性 | ⭐⭐⭐⭐ | 基本安全，密码处理可改进 |
| 性能 | ⭐⭐⭐⭐⭐ | Dump-Once 架构高效 |

### 优先修复项

1. ✅ **已修复**: `--only-dirs` 参数被忽略问题
2. 🔴 **P1-1**: `DEPENDENCY_LAYERS` 顺序错误 (TYPE 应在 FUNCTION 之前)
3. 🔶 **P2-4**: 约束统计函数遗漏 CHECK 约束
4. 🔶 **P2-5**: 权限映射遗漏 JOB/SCHEDULE
5. 🔶 **P2-6**: TYPE/TYPE BODY 遗漏拓扑排序 (PACKAGE 有但 TYPE 没有)
6. 🔶 **P2-7**: PROCEDURE/FUNCTION/TRIGGER 缺少层内拓扑排序
7. 🔶 **P2-8**: 非 VIEW 对象缺少授权联动
8. 🔶 **建议修复**: 密码传递方式改进
9. 🔶 **建议改进**: 代码模块化拆分

### 本次审查新增发现

| 类别 | 发现数量 | 说明 |
|------|----------|------|
| 依赖链处理缺陷 | 3 处 | TYPE/FUNCTION 顺序错误、非VIEW缺少拓扑排序、缺少授权联动 |
| 约束类型遗漏 | 4 处 | CHECK 约束在统计和文档中被遗漏 |
| 权限映射遗漏 | 2 处 | JOB/SCHEDULE 未定义默认权限 |
| 文档不准确 | 3 处 | 约束类型描述不完整 |

**根因分析**：
1. **依赖链问题**: VIEW 实现了完整的拓扑排序，但其他 PL/SQL 对象未复用此机制
2. **类型遗漏问题**: 开发时对某些对象特性认知不完整，导致硬编码列表遗漏

---

## 15. 深度审查 (2026-01-23 更新)

### 15.1 代码版本变更

**审查日期**: 2026-01-23  
**代码行数**: 22,701 行 (较上次 +307 行)  
**主要变更**: CHECK 约束增强、DEFERRABLE 属性支持

### 15.2 CHECK 约束处理增强 ✅

**新增函数**: `normalize_check_constraint_signature()` (第 1002-1013 行)

```python
def normalize_check_constraint_signature(
    expr: Optional[str],
    cons_name: Optional[str],
    cons_meta: Optional[Dict]
) -> str:
    expr_norm = normalize_sql_expression(expr)
    deferrable = normalize_deferrable_flag((cons_meta or {}).get("deferrable"))
    deferred = normalize_deferred_flag((cons_meta or {}).get("deferred"))
    return f"{expr_norm}||DEFERRABLE={deferrable}||DEFERRED={deferred}"
```

**改进点**:
- ✅ CHECK 约束比对现在包含 DEFERRABLE/DEFERRED 属性
- ✅ 添加 `classify_unsupported_check_constraint()` 检测 OB 不支持的 CHECK 约束
- ✅ 自动识别包含 `SYS_CONTEXT('USERENV', ...)` 的不兼容约束

### 15.3 发现的新问题

#### P2-9: 代码冗余 - extra_results 重复调用

**位置**: `schema_diff_reconciler.py:22406-22424`

```python
if enabled_extra_types:
    with phase_timer("扩展对象校验", phase_durations):
        extra_results = check_extra_objects(...)  # ← 调用1
else:
    extra_results = check_extra_objects(...)      # ← 调用2 (完全相同)
```

**问题**: 无论 `enabled_extra_types` 是否为空，都调用相同的函数，唯一区别是是否记录计时。

**影响**: 低 - 功能正确但代码冗余

**修复建议**:
```python
with phase_timer("扩展对象校验", phase_durations) if enabled_extra_types else nullcontext():
    extra_results = check_extra_objects(...)
```

#### P2-10: 变量使用前未初始化

**位置**: `schema_diff_reconciler.py:22428-22434`

```python
trigger_status_rows: List[TriggerStatusReportRow] = []
support_summary: Optional[ObjectSupportSummary] = None  # ← 初始化为 None
if 'TRIGGER' in enabled_extra_types:
    trigger_status_rows = collect_trigger_status_rows(
        ...
        unsupported_table_keys=(support_summary.unsupported_table_keys if support_summary else None)
        # ↑ 此时 support_summary 始终为 None!
    )

support_summary = classify_missing_objects(...)  # ← 在此之后才赋值
```

**问题**: `collect_trigger_status_rows` 中使用 `support_summary.unsupported_table_keys`，但此时 `support_summary` 尚未赋值。

**影响**: 中 - `unsupported_table_keys` 参数始终为 None，可能导致 unsupported 表的触发器被错误处理

**修复建议**: 将 `classify_missing_objects()` 调用移至 `collect_trigger_status_rows` 之前

### 15.4 主流程验证 ✅

**位置**: `main()` 函数 (第 21928-22700 行)

**验证结果**: 之前发现的缩进问题已修复

```python
# 正确的代码结构 (第 22614-22640 行)
view_chain_file = generate_fixup_scripts(
    ora_cfg,
    ob_cfg,
    settings,
    tv_results,
    ...
)
```

✅ `generate_fixup_scripts` 现在正确地在 `phase_timer("修补脚本生成")` 内调用，不再受 `enable_grant_generation` 条件影响。

### 15.5 元数据加载交叉验证 ✅

#### Oracle 元数据 vs OceanBase 元数据字段对齐

| 字段 | Oracle (第7289行) | OceanBase (第6125行) | 对齐状态 |
|------|-------------------|----------------------|----------|
| CONSTRAINT_TYPE | `('P','U','R','C')` | `('P','U','R','C')` | ✅ |
| SEARCH_CONDITION | ✅ 读取 | ✅ 读取 (带回车清理) | ✅ |
| DELETE_RULE | ✅ 读取 | ✅ 读取 | ✅ |
| DEFERRABLE | ✅ 读取 (第7505行) | ❌ 硬编码 None (第6388行) | 🔴 不对齐 |
| DEFERRED | ✅ 读取 (第7540行) | ❌ 硬编码 None (第6389行) | 🔴 不对齐 |

**严重发现**: OceanBase 元数据加载将 `deferrable` 和 `deferred` 字段硬编码为 `None`：

```python
# schema_diff_reconciler.py:6385-6389
{
    ...
    "deferrable": None,  # ← 硬编码！
    "deferred": None,    # ← 硬编码！
}
```

**影响**: 
- Oracle 端 DEFERRABLE 约束与 OceanBase 端比对时，OB 侧始终为 `NOT DEFERRABLE/IMMEDIATE`
- 可能导致正常约束被误报为"条件/延迟属性不一致"

**修复建议**: 在 OceanBase 约束查询中添加 DEFERRABLE/DEFERRED 字段读取

### 15.6 约束比对逻辑交叉验证 ✅

**位置**: `compare_constraints_for_table()` (第 10933-11241 行)

| 约束类型 | bucket 函数 | match 函数 | 评估 |
|----------|-------------|------------|------|
| PRIMARY KEY | `bucket_pk_uk()` | `match_constraints()` | ✅ |
| UNIQUE KEY | `bucket_pk_uk()` | `match_constraints()` | ✅ |
| FOREIGN KEY | `bucket_fk()` | `match_foreign_keys()` | ✅ |
| CHECK | `bucket_check()` | `match_check_constraints()` | ✅ |

**验证结论**: 所有四种约束类型都有完整的分桶和匹配逻辑。

### 15.7 DDL 调整逻辑审查 ✅

**位置**: `adjust_ddl_for_object()` (第 14030-14300+ 行)

**审查结论**: 实现复杂但正确

| 功能 | 实现方式 | 评估 |
|------|----------|------|
| 主对象 schema/name 替换 | 正则 + 引号处理 | ✅ |
| 无限定名称替换 | 上下文感知替换 | ✅ |
| 避免误替换列名 | `stop_tokens` 检测 | ✅ |
| PUBLIC SYNONYM 特殊处理 | 跳过 schema 添加 | ✅ |
| SEQUENCE.NEXTVAL 识别 | `_looks_like_namespace()` | ✅ |

### 15.8 依赖链处理验证 ✅

**位置**: `build_view_fixup_chains()` (第 8521-8641 行)

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 循环检测 | ✅ | DFS 中 `seen` 集合防止无限递归 |
| 深度限制 | ✅ | `max_depth=30` 防止过深遍历 |
| SYNONYM 解析 | ✅ | `resolve_synonym_chain_target` 处理同义词链 |
| 授权状态检测 | ✅ | `_grant_status()` 检测跨 schema 权限 |

### 15.9 Fixup 脚本生成验证 ✅

**位置**: `generate_fixup_scripts()` (第 16696-19450 行)

**执行阶段验证**:

| 阶段 | 对象类型 | 任务收集 | 并发执行 | 评估 |
|------|----------|----------|----------|------|
| 1/9 | SEQUENCE | ✅ `sequence_tasks` | ✅ `run_tasks()` | ✅ |
| 2/9 | TABLE (CREATE) | ✅ `missing_tables` | ✅ | ✅ |
| 3/9 | TABLE (ALTER) | ✅ `mismatched_tables` | ✅ | ✅ |
| 4/9 | 代码对象 (VIEW等) | ✅ `view_missing` 等 | ✅ | ✅ |
| 5/9 | TABLE (INTERVAL) | ✅ 条件检查 | ✅ | ✅ |
| 6/9 | INDEX | ✅ `index_tasks` | ✅ | ✅ |
| 7/9 | CONSTRAINT | ✅ `constraint_tasks` | ✅ | ✅ |
| 8/9 | TRIGGER | ✅ `trigger_tasks` | ✅ | ✅ |
| 9/9 | GRANT | ✅ `grant_plan` | ✅ | ✅ |

### 15.10 DDL 清洗规则缺失：FOR LOOP 集合属性范围语法

**问题描述**: Oracle 非标准语法 `FOR idx IN collection.FIRST.collection.LAST LOOP` 需要转换为标准语法 `FOR idx IN collection.FIRST..collection.LAST LOOP`（单点改双点）

#### 现有实现分析

**位置**: `schema_diff_reconciler.py:14404-14417`

```python
FOR_LOOP_RANGE_SINGLE_DOT_PATTERN = re.compile(
    r'(\bIN\s+-?\d+)\s*\.(\s*)(?=(?:"[^"]+"|[A-Z_]))',
    re.IGNORECASE
)

def clean_for_loop_single_dot_range(ddl: str) -> str:
    """修复 FOR ... IN 1.var 这种单点范围写法为 1..var"""
    ...
```

**当前覆盖范围**:
- ✅ `FOR i IN 1.n LOOP` → `FOR i IN 1..n LOOP` (整数起始)
- ❌ `FOR idx IN col.FIRST.col.LAST LOOP` (集合属性) **未覆盖**

#### 需要新增的规则

**规则名称**: `clean_for_loop_collection_attr_range`

**正则模式**:
```python
FOR_LOOP_COLLECTION_ATTR_PATTERN = re.compile(
    r'(\.(?:FIRST|LAST))\s*\.(?!\.)(\s*(?:[A-Z_][A-Z0-9_$#]*|\d+))',
    re.IGNORECASE
)
```

**实现代码**:
```python
def clean_for_loop_collection_attr_range(ddl: str) -> str:
    """
    修复 FOR ... IN collection.FIRST.xxx 或 collection.LAST.xxx
    将 .FIRST. 或 .LAST. 后的单点改为双点 (..)
    
    原理：.FIRST/.LAST 返回标量索引值，后面不可能有合法的单点字段访问，
    因此 .FIRST. 或 .LAST. 后跟单点必定是 range 运算符 (..) 写错。
    
    示例:
      col.FIRST.col.LAST  →  col.FIRST..col.LAST
      col.FIRST.10        →  col.FIRST..10
      col.FIRST.v_end     →  col.FIRST..v_end
    """
    if not ddl:
        return ddl
    return FOR_LOOP_COLLECTION_ATTR_PATTERN.sub(r"\1..\2", ddl)
```

**正则说明**:
- `(\.(?:FIRST|LAST))` - 匹配 `.FIRST` 或 `.LAST`
- `\s*\.` - 匹配可能有空格的单点
- `(?!\.)` - 负向前瞻，确保不是已经正确的 `..`
- `(\s*(?:[A-Z_][A-Z0-9_$#]*|\d+))` - 匹配后续的标识符或数字

#### 添加位置

**文件**: `schema_diff_reconciler.py`

**步骤 1**: 在 `FOR_LOOP_RANGE_SINGLE_DOT_PATTERN` 定义后添加新模式和函数 (约第 14418 行)

**步骤 2**: 在 `DDL_CLEANUP_RULES['PLSQL_OBJECTS']['rules']` 中添加新规则 (约第 15249 行):

```python
'PLSQL_OBJECTS': {
    'types': ['PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'TYPE', 'TYPE BODY', 'TRIGGER'],
    'rules': [
        clean_end_schema_prefix,
        clean_for_loop_single_dot_range,
        clean_for_loop_collection_attr_range,  # ← 新增
        clean_plsql_ending,
        ...
    ]
}
```

#### PL/SQL 循环语法完整分类

**Oracle PL/SQL 支持的循环类型**:

| 循环类型 | 语法示例 | 是否涉及 `..` 范围 |
|----------|----------|-------------------|
| **数值 FOR 循环** | `FOR i IN 1..10 LOOP` | ✅ 是 |
| **REVERSE FOR 循环** | `FOR i IN REVERSE 1..10 LOOP` | ✅ 是 |
| **游标 FOR 循环** | `FOR rec IN cursor_name LOOP` | ❌ 否 |
| **SELECT FOR 循环** | `FOR rec IN (SELECT ...) LOOP` | ❌ 否 |
| **集合遍历循环** | `FOR idx IN col.FIRST..col.LAST LOOP` | ✅ 是 |
| **WHILE 循环** | `WHILE condition LOOP` | ❌ 否 |
| **基本 LOOP** | `LOOP ... EXIT WHEN ... END LOOP` | ❌ 否 |

**需要清洗的场景（涉及 `..` 范围运算符）**:

| 场景 | 错误写法 | 正确写法 | 覆盖状态 |
|------|----------|----------|----------|
| 整数..变量 | `FOR i IN 1.n LOOP` | `FOR i IN 1..n LOOP` | ✅ 现有规则 |
| 整数..集合属性 | `FOR i IN 1.col.LAST LOOP` | `FOR i IN 1..col.LAST LOOP` | ✅ 现有规则 |
| 负整数..变量 | `FOR i IN -1.n LOOP` | `FOR i IN -1..n LOOP` | ✅ 现有规则 |
| 集合.FIRST..集合.LAST | `col.FIRST.col.LAST` | `col.FIRST..col.LAST` | 🔴 新规则 |
| 集合.FIRST..数字 | `col.FIRST.10` | `col.FIRST..10` | 🔴 新规则 |
| 集合.FIRST..变量 | `col.FIRST.v_end` | `col.FIRST..v_end` | 🔴 新规则 |
| 集合.LAST..数字 | `col.LAST.1` | `col.LAST..1` (REVERSE) | 🔴 新规则 |
| 变量..变量 | `v_start.v_end` | `v_start..v_end` | ❌ 无法自动判断 |
| 集合.COUNT | `1.col.COUNT` | `1..col.COUNT` | ✅ 现有规则 |

**其他集合属性（需扩展支持）**:

| 属性 | 说明 | 是否需要清洗 |
|------|------|--------------|
| `.FIRST` | 返回集合第一个索引 | ✅ 是 |
| `.LAST` | 返回集合最后一个索引 | ✅ 是 |
| `.COUNT` | 返回集合元素数量 | ✅ 是 (建议扩展) |
| `.NEXT(n)` | 返回 n 之后的索引 | ❌ 否 (函数调用) |
| `.PRIOR(n)` | 返回 n 之前的索引 | ❌ 否 (函数调用) |

#### 建议的完整正则模式

```python
# 扩展支持 FIRST, LAST, COUNT 三种集合属性
FOR_LOOP_COLLECTION_ATTR_PATTERN = re.compile(
    r'(\.(?:FIRST|LAST|COUNT))\s*\.(?!\.)(\s*(?:[A-Z_][A-Z0-9_$#]*|\d+))',
    re.IGNORECASE
)
```

#### 无法覆盖的场景

`v_start.v_end` 这种两个普通变量之间缺少 `..` 的情况**无法自动修复**，因为程序无法区分：
- `obj.field` (合法的字段访问)
- `lower.upper` (错误的 range 写法)

此类情况需要人工审查修复。

#### 测试用例建议

```python
def test_clean_for_loop_collection_attr_range():
    # 基本场景
    assert clean_for_loop_collection_attr_range(
        "FOR idx IN v_rcpt_info.FIRST.v_rcpt_info.LAST LOOP"
    ) == "FOR idx IN v_rcpt_info.FIRST..v_rcpt_info.LAST LOOP"
    
    # .FIRST + 数字
    assert clean_for_loop_collection_attr_range(
        "FOR i IN arr.FIRST.10 LOOP"
    ) == "FOR i IN arr.FIRST..10 LOOP"
    
    # .LAST + 变量
    assert clean_for_loop_collection_attr_range(
        "FOR i IN 1.arr.LAST LOOP"
    ) == "FOR i IN 1.arr.LAST LOOP"  # 现有规则已处理
    
    # 已经正确的不应修改
    assert clean_for_loop_collection_attr_range(
        "FOR idx IN col.FIRST..col.LAST LOOP"
    ) == "FOR idx IN col.FIRST..col.LAST LOOP"
```

### 15.11 表列检查能力矩阵

**位置**: `check_primary_objects()` (第 9800-10018 行)

#### 一、列存在性检查 ✅

| 检查项 | 变量名 | 说明 |
|--------|--------|------|
| **缺失列** | `missing_in_tgt` | 源端有、目标端无的列 |
| **多余列** | `extra_in_tgt` | 目标端有、源端无的列 (含 OMS_* 列检测) |

#### 二、VARCHAR/VARCHAR2 长度检查 ✅

| 检查项 | issue_type | 触发条件 |
|--------|------------|----------|
| **长度过短** | `short` | BYTE 语义: `tgt_len < src_len × 1.5` |
| **长度过大** | `oversize` | BYTE 语义: `tgt_len > src_len × 上限倍数` |
| **CHAR 语义不匹配** | `char_mismatch` | CHAR_USED='C' 时要求长度和语义完全一致 |

#### 三、NUMBER 类型精度检查 ✅

| 检查项 | issue_type | 触发条件 |
|--------|------------|----------|
| **精度不足** | `number_precision` | `tgt_prec < src_prec` |
| **小数位不一致** | `number_precision` | `tgt_scale != src_scale` |
| **精度约束变化** | `number_precision` | 源无精度限制，目标有精度限制 |

#### 四、虚拟列 (VIRTUAL COLUMN) 检查 ✅

| 检查项 | issue_type | 说明 |
|--------|------------|------|
| **虚拟列缺失** | `virtual_missing` | 源是虚拟列，目标不是 |
| **表达式不一致** | `virtual_expr_mismatch` | 虚拟列计算表达式不同 |

#### 五、IDENTITY 列检查 ✅

| 检查项 | issue_type | 说明 |
|--------|------------|------|
| **IDENTITY 缺失** | `identity_missing` | 源有 IDENTITY 属性，目标无 |

#### 六、DEFAULT ON NULL 检查 ✅

| 检查项 | issue_type | 说明 |
|--------|------------|------|
| **DEFAULT ON NULL 缺失** | `default_on_null_missing` | 源有此属性，目标无 |

#### 七、列可见性检查 ✅

| 检查项 | issue_type | 说明 |
|--------|------------|------|
| **INVISIBLE→VISIBLE** | `visibility_mismatch` | 源隐藏列变为可见 |
| **VISIBLE→INVISIBLE** | `visibility_mismatch` | 源可见列变为隐藏 |

#### 八、LONG 类型映射检查 ✅

| 检查项 | issue_type | 说明 |
|--------|------------|------|
| **LONG→CLOB 未转换** | `long_type` | LONG 应转为 CLOB |
| **LONG RAW→BLOB 未转换** | `long_type` | LONG RAW 应转为 BLOB |

#### 九、未实现的检查项 ⚠️

| 属性 | 元数据字段 | 当前状态 | 建议 |
|------|------------|----------|------|
| **NULLABLE** | `nullable` | ❌ 已读取未比对 | P3: 可选实现 |
| **DATA_DEFAULT** | `data_default` | ❌ 已读取未比对 | P3: 可选实现 |
| **列顺序** | - | ➖ 不关心 | 无需实现 |
| **字符集** | - | ➖ 不关心 | 无需实现 |

#### 检查能力汇总

| 类别 | 检查数量 | 状态 |
|------|----------|------|
| ✅ 已实现检查 | **10 类** | 生产可用 |
| ⚠️ 元数据已有但未检查 | **2 类** | NULLABLE, DEFAULT |
| ➖ 业务不关心 | **2 类** | 列顺序, 字符集 |

### 15.12 Fixup 目录清理问题 🔴

**用户反馈**: 进入 fixup 逻辑后不会清空之前的 fixup 目录，导致新的 fixup 无法生成

**位置**: `generate_fixup_scripts()` (第 16809-16838 行)

#### 问题 1: master_list 为空时提前返回 (P1)

```python
# schema_diff_reconciler.py:16809-16813
if not master_list:
    log.info("[FIXUP] master_list 为空，未生成目标端订正 SQL。")
    return None  # ← 在清理逻辑之前就返回！

ensure_dir(base_dir)
# ... 清理逻辑在第 16822 行 ...
```

**影响**: 第二次运行时若无需修复对象，直接返回，**不清理上次遗留的脚本**，用户误以为新脚本未生成。

**修复建议**:
```python
# 将目录清理移到 master_list 检查之前
ensure_dir(base_dir)
# ... 清理逻辑 ...

if not master_list:
    log.info("[FIXUP] master_list 为空，目录已清理，无新增订正 SQL。")
    return None
```

#### 问题 2: 文件删除无异常处理 (P2)

```python
# schema_diff_reconciler.py:16826-16832
for child in base_dir.iterdir():
    if child.is_file():
        child.unlink()  # ← 无 try/except，删除失败会中断整个清理
        removed_files += 1
```

**影响**: 任何文件被锁定或无权限，后续所有文件都不会被清理。

**修复建议**:
```python
for child in base_dir.iterdir():
    try:
        if child.is_file():
            child.unlink()
            removed_files += 1
        elif child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
            removed_dirs += 1
    except OSError as e:
        log.warning("[FIXUP] 无法删除 %s: %s", child, e)
        failed_count += 1
```

#### 问题 3: 绝对路径在 cwd 外时不清理 (P3)

```python
# schema_diff_reconciler.py:16818
safe_to_clean = (not base_dir.is_absolute()) or (run_root == base_resolved or run_root in base_resolved.parents)
```

**影响**: 若 `fixup_dir` 是绝对路径且不在当前工作目录下，旧脚本不会被清理。

**修复建议**: 添加配置项 `fixup_force_clean=true` 允许用户强制清理，或在日志中明确提示用户手动清理。

### 15.13 潜在风险汇总

| ID | 级别 | 问题 | 影响 | 建议 |
|----|------|------|------|------|
| P1-2 | P1 | master_list 为空时不清理 fixup 目录 | 旧脚本残留误导用户 | 调整清理逻辑顺序 |
| P2-9 | P3 | extra_results 重复调用 | 代码冗余 | 重构条件逻辑 |
| P2-10 | P2 | support_summary 使用前未初始化 | trigger 过滤不准确 | 调整调用顺序 |
| P2-11 | P2 | OB 元数据未读取 DEFERRABLE/DEFERRED | CHECK 约束比对误报 | 修改 OB 约束查询 |
| P2-12 | P2 | FOR LOOP 集合属性范围语法未清洗 | PL/SQL 对象迁移失败 | 添加新清洗规则 |
| P2-13 | P2 | fixup 文件删除无异常处理 | 清理中断导致残留 | 添加 try/except |
| P2-14 | P3 | 绝对路径在 cwd 外不清理 | 旧脚本可能残留 | 添加强制清理配置 |

---

## 16. 总结更新

### 本次深度审查新增发现

| 类别 | 发现数量 | 说明 |
|------|----------|------|
| **Fixup 目录清理问题** | 3 处 | master_list 空时不清理、无异常处理、绝对路径限制 (详见 15.12) |
| 代码逻辑问题 | 3 处 | 冗余调用、变量初始化顺序、OB 元数据字段缺失 |
| DDL 清洗规则缺失 | 1 处 | FOR LOOP 集合属性范围语法 (详见 15.10) |
| 表列检查能力 | 10 类已实现 | VARCHAR/NUMBER/VIRTUAL/IDENTITY/可见性等 (详见 15.11) |
| 表列检查缺失 | 2 类 | NULLABLE、DATA_DEFAULT 已读取未比对 |
| 改进确认 | 3 处 | CHECK 约束增强、主流程修复、DDL 调整正确 |
| 元数据不对齐 | 1 处 | OB 侧 DEFERRABLE/DEFERRED 硬编码为 None |

### 整体评价更新

| 维度 | 评分 | 变化 | 说明 |
|------|------|------|------|
| 业务逻辑正确性 | ⭐⭐⭐⭐⭐ | → | CHECK 约束处理增强 |
| 代码质量 | ⭐⭐⭐⭐ | → | 仍有小问题待修复 |
| 异常处理 | ⭐⭐⭐ | → | 无变化 |
| 测试覆盖 | ⭐⭐⭐ | → | 无变化 |
| 安全性 | ⭐⭐⭐⭐ | → | 无变化 |
| 性能 | ⭐⭐⭐⭐⭐ | → | 无变化 |

### 优先修复项更新

1. 🔴 **P1-2**: **Fixup 目录清理问题** - master_list 为空时不清理旧脚本 (用户反馈，详见 15.12)
2. ✅ **已修复**: `generate_fixup_scripts` 缩进问题
3. ✅ **已增强**: CHECK 约束 DEFERRABLE/DEFERRED 支持 (但 OB 侧数据缺失)
4. 🔴 **P2-13**: fixup 文件删除无异常处理 (新发现)
5. 🔴 **P2-12**: FOR LOOP 集合属性范围语法清洗规则缺失 (新发现，详见 15.10)
6. 🔴 **P2-11**: OB 元数据未读取 DEFERRABLE/DEFERRED 字段 (新发现)
7. 🔴 **P2-10**: `support_summary` 使用前未初始化 (新发现)
8. 🔴 **P1-1**: `DEPENDENCY_LAYERS` 顺序错误 (待修复)
9. 🔶 其他之前发现的问题 (参见第 7 节)

---

*审核人: Cascade AI*  
*审核工具版本: 2026.01*  
*更新时间: 2026-01-23 09:40*
