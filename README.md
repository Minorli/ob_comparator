# OceanBase Comparator Toolkit

> 当前版本：V0.9.7  
> 关键词：一次转储、本地对比、Remap 推导、精确修复脚本

这是一套面向 Oracle → OceanBase 的对象对比与修复工具。它把元数据一次性拉到本地内存进行比对，避免循环查库带来的性能与稳定性问题，并能生成可审计的修复脚本。

## 3 分钟上手（新手版）

### 1) 准备环境
- Python 3.7+
- Oracle Instant Client（19c+）
- obclient
- JDK + dbcat

### 2) 安装依赖
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3) 生成并填写配置
```bash
cp config.ini.template config.ini
```

最小必填项示例（只列关键项）：
```ini
[ORACLE_SOURCE]
user = scott
password = tiger
dsn = 127.0.0.1:1521/orclpdb1

[OCEANBASE_TARGET]
executable = /usr/bin/obclient
host = 127.0.0.1
port = 2883
user_string = root@sys#obcluster
password = xxx

[SETTINGS]
source_schemas = SCOTT,HR
remap_file = remap_rules.txt
oracle_client_lib_dir = /opt/instantclient_19_28
dbcat_bin = /opt/dbcat-2.5.0-SNAPSHOT
dbcat_output_dir = dbcat_output
java_home = /usr/lib/jvm/java-11
```

> 配置项很多？不用怕。完整说明请看 `readme_config.txt`，模板默认值见 `config.ini.template`。

### 4) 运行对比
```bash
python3 schema_diff_reconciler.py
# 缺项可用向导
python3 schema_diff_reconciler.py --wizard
```

### 5) 审核并执行修复

**标准执行（单次运行）**:
```bash
# 先审核 fixup_scripts/ 下的 SQL
python3 run_fixup.py --smart-order --recompile
```

**迭代执行（推荐用于VIEW）**:
```bash
# 自动重试失败的脚本，特别适合有依赖关系的VIEW
python3 run_fixup.py --iterative --smart-order --recompile --max-rounds 10

# 仅处理VIEW并迭代
python3 run_fixup.py --iterative --only-types VIEW --max-rounds 5
```

**VIEW 链路自动修复**:
```bash
# 基于 VIEWs_chain 生成的依赖链进行精准修复
python3 run_fixup.py --view-chain-autofix
```

**新增参数说明**:
- `--iterative`: 启用多轮迭代执行，自动重试失败脚本
- `--max-rounds N`: 最大迭代轮次（默认10）
- `--min-progress N`: 每轮最小进展数，低于此值停止（默认1）
- `--view-chain-autofix`: 依据最新 VIEWs_chain 生成并执行按 VIEW 拆分的修复计划

> 💡 **提示**: 迭代模式会自动分析失败原因并提供可操作建议。对于有复杂依赖关系的VIEW，成功率可从0.5%提升至93%+。

> 💡 **提示**: view-chain 模式默认跳过已存在 VIEW（仍输出 plan/sql 标记 SKIPPED），并在 DDL 缺失时从 `fixup_scripts/done/` 兜底查找。


## Remap 规则速记

**显式规则优先级最高**，未写规则的对象遵循以下默认逻辑：

- **TABLE 必须显式**：如果表被 remap，建议只写表的规则。
- **VIEW / MATERIALIZED VIEW / TRIGGER**：默认保持原 schema，不会跟随父表 remap。
- **INDEX / CONSTRAINT / SEQUENCE**：依附表，默认跟随父表的 remap 目标。
- **PROCEDURE / FUNCTION / TYPE / SYNONYM**：允许通过依赖推导目标 schema（可关闭）。
- **PACKAGE / PACKAGE BODY**：默认仅打印不校验。
- **MATERIALIZED VIEW**：OB 不支持，仅打印不校验。

如果推导失败或出现冲突，报告会输出 `remap_conflicts_*.txt`，需要在 `remap_rules.txt` 中显式补齐。

**规则示例：**
```
# 表 remap
SRC_A.ORDERS = OB_A.ORDERS
SRC_A.CUSTOMERS = OB_A.CUSTOMERS

# 需要强制改 schema 的视图/触发器，必须显式写
SRC_A.VW_REPORT = OB_A.VW_REPORT
SRC_A.TRG_ORDER = OB_A.TRG_ORDER
```

## 运行后会生成什么？

- `main_reports/report_*.txt`：完整对比报告（建议先看这个）
- `main_reports/remap_conflicts_*.txt`：无法自动推导的对象清单
- `main_reports/tables_views_miss/`：按目标 schema 输出缺失 TABLE/VIEW 规则（`schema_T.txt` / `schema_V.txt`）
- `main_reports/blacklist_tables.txt`：黑名单表清单（按 schema 分组，附原因与 LONG 转换校验状态）
- `main_reports/filtered_grants.txt`：被过滤的不兼容 GRANT 权限清单
- `main_reports/VIEWs_chain_*.txt`：缺失 VIEW 的依赖链路与权限标记
- `fixup_scripts/`：按对象类型生成的修复 SQL（执行前需审核，VIEW DDL 使用 DBMS_METADATA）
- `fixup_scripts/grants_miss/`：缺失授权脚本（优先执行）
- `fixup_scripts/grants_all/`：全量授权脚本（用于审计）
- `fixup_scripts/view_chain_plans/`：VIEW 链路自动修复计划
- `fixup_scripts/view_chain_sql/`：VIEW 链路自动修复 SQL
- `dbcat_output/`：DDL 缓存（下次复用）

> 如果源库存在 `OMS_USER.TMP_BLACK_TABLE`，则缺失表会先与黑名单比对：黑名单缺失表不会进入 `tables_views_miss/`，仅在 `blacklist_tables.txt` 中说明原因与状态。
> `LONG/LONG RAW` 列在补列 DDL 中会自动转换为 `CLOB/BLOB`。

## 常见使用场景

**只看表结构，不做修复：**
```ini
check_primary_types = TABLE
generate_fixup = false
check_dependencies = false
```

**全量比对 + 修复脚本：**
```ini
check_primary_types =
check_extra_types = INDEX,CONSTRAINT,SEQUENCE,TRIGGER
check_dependencies = true
generate_fixup = true
generate_grants = true
```

**授权脚本压缩（大量 GRANT 场景）：**
```ini
# 仅抽取 source_schemas 拥有的对象权限，减少规模
grant_tab_privs_scope = owner
# 合并多权限/多 grantee，显著减少 GRANT 语句数量
grant_merge_privileges = true
grant_merge_grantees = true
# 可选：覆盖系统/对象权限白名单
# grant_supported_sys_privs = CREATE SESSION,CREATE TABLE
# grant_supported_object_privs = SELECT,INSERT,UPDATE,DELETE,REFERENCES,EXECUTE
```

## 项目结构速览

| 路径 | 说明 |
| --- | --- |
| `schema_diff_reconciler.py` | 主程序：对比、推导、报告、fixup 生成 |
| `run_fixup.py` | 修复脚本执行器（支持 smart-order、recompile 与迭代执行） |
| `config.ini.template` | 配置模板 |
| `readme_config.txt` | 配置项完整说明 |
| `remap_rules.txt` | Remap 规则 |
| `main_reports/` | 报告输出 |
| `fixup_scripts/` | 修复脚本输出 |
| `docs/ADVANCED_USAGE.md` | Remap 推导和 run_fixup 高级说明 |
| `docs/ARCHITECTURE.md` | 架构设计与内部实现 |
| `docs/DEPLOYMENT.md` | 离线部署与跨平台打包 |

## 进一步阅读

1) `readme_config.txt`：配置项与默认值  
2) `docs/ADVANCED_USAGE.md`：Remap 细节、冲突处理、执行策略  
3) `docs/ARCHITECTURE.md`：核心流程与关键算法  
4) `docs/DEPLOYMENT.md`：离线部署与跨平台运行  

---
© 2025 Minor Li.
