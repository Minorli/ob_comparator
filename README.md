# OceanBase Comparator Toolkit

> 当前版本：V0.9.8.3  
> 面向 Oracle → OceanBase (Oracle 模式) 的结构一致性校验与修补脚本生成工具  
> 核心理念：一次转储、本地对比、脚本审计优先

## 近期更新（0.9.8.3）
- report_to_db 覆盖范围扩展（支持 full 模式更多表）。
- 新增 DIFF_REPORT_DETAIL_ITEM（明细行化），便于列级排查。
- 新增 report_db_store_scope 与 report_db_detail_item_enable 配置。
- 新增报告分析视图（actions/profile/trends/pending/grant/usability）。
- 新增写库失败追踪与整改闭环表（WRITE_ERRORS / RESOLUTION）。
- HOW_TO_READ_REPORTS_IN_OB SQL 全量校验与修正。

详见：`docs/RELEASE_NOTES_0.9.8.3.md`

## 核心能力
- **对象覆盖完整**：TABLE/VIEW/MVIEW/PLSQL/TYPE/JOB/SCHEDULE + INDEX/CONSTRAINT/SEQUENCE/TRIGGER。
- **Dump-Once 架构**：Oracle Thick Mode + 少量 obclient 调用，元数据一次性落本地内存。
- **Remap 推导**：支持显式规则、依附对象跟随、依赖推导、schema 回退策略。
- **依赖与授权**：基于 DBA_DEPENDENCIES/DBA_*_PRIVS 生成缺失依赖与授权脚本。
- **DDL 清洗与兼容**：VIEW DDL 走 DBMS_METADATA，PL/SQL 语法清洗与 Hint 过滤。
- **DDL 输出格式化**：可选 SQLcl 格式化 fixup DDL（不影响校验与修补逻辑）。
- **修补脚本执行器**：支持 smart-order、迭代重试、VIEW 链路自动修复、错误报告。
- **报告体系**：Rich 控制台 + 纯文本快照 + 细节分拆报告（可配置）。
- **不支持对象识别**：黑名单/依赖阻断对象单独统计与分流输出。

## 适用场景
- Oracle → OceanBase 迁移后的结构一致性审计
- OMS 仅迁移表结构/数据后，补齐非表对象与授权
- 多 schema Remap、依赖复杂、VIEW 链较长的迁移项目

## 环境与依赖
- Python 3.7+
- Oracle Instant Client 19c+（oracledb Thick Mode）
- obclient（目标 OceanBase 可连接）
- JDK + dbcat（DDL 批量提取）
- SQLcl（可选，用于 DDL 格式化）
- 运行账号需具备 DBA_* 视图访问权限（Oracle 与 OB）

## 快速开始

### 1) 安装依赖
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) 生成配置
```bash
cp config.ini.template config.ini
```

最小示例（只列关键项）：
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
synonym_fixup_scope = public_only
sequence_remap_policy = source_only
trigger_qualify_schema = true
report_dir_layout = per_run
report_detail_mode = split
report_to_db = true
oracle_client_lib_dir = /opt/instantclient_19_28
dbcat_bin = /opt/dbcat-2.5.0-SNAPSHOT
dbcat_output_dir = dbcat_output
java_home = /usr/lib/jvm/java-11
```
完整配置说明见 `readme_config.txt`。

### 3) 运行对比
```bash
python3 schema_diff_reconciler.py
# 配置缺项可用向导
python3 schema_diff_reconciler.py --wizard
```

### 4) 审核并执行修复
```bash
# 先审核 fixup_scripts/ 下的 SQL
python3 run_fixup.py --smart-order --recompile
```

## Remap 规则速记
- **显式规则优先级最高**，未写规则的对象按默认推导。
- **TABLE 必须显式**：表的 remap 建议只写表规则。
- **VIEW/MVIEW/TRIGGER/PACKAGE** 默认保持原 schema，需显式 remap 才改。
- **INDEX/CONSTRAINT/SEQUENCE** 默认跟随父表。
- **PROCEDURE/FUNCTION/TYPE/SYNONYM** 可通过依赖推导目标 schema。

示例：
```
SRC_A.ORDERS = OB_A.ORDERS
SRC_A.VW_REPORT = OB_A.VW_REPORT
SRC_A.TRG_ORDER = OB_A.TRG_ORDER
```

## run_fixup 执行模式

**标准执行**（一次运行）：
```bash
python3 run_fixup.py --smart-order --recompile
```

**迭代执行**（推荐用于 VIEW/依赖复杂场景）：
```bash
python3 run_fixup.py --iterative --smart-order --recompile --max-rounds 10
```

**VIEW 链路自动修复**（依赖链驱动）：
```bash
python3 run_fixup.py --view-chain-autofix
```

## 额外工具
- `init_users_roles.py`：以 Oracle 为准创建用户/角色并同步系统权限与角色授权。
- `init_test.py`：基于 `test_scenarios/` 快速搭建测试场景。

> 注意：`init_users_roles.py` 当前使用固定初始密码，需在上线前统一改密或二次调整。

## 主要输出
- `main_reports/run_<ts>/report_<ts>.txt`：完整对比报告（默认 per_run）
- `main_reports/run_<ts>/package_compare_<ts>.txt`：PACKAGE/PKG BODY 明细
- `main_reports/run_<ts>/remap_conflicts_<ts>.txt`：Remap 冲突清单
- `main_reports/run_<ts>/VIEWs_chain_<ts>.txt`：VIEW 依赖链报告
- `main_reports/run_<ts>/blacklist_tables.txt`：黑名单表清单
- `main_reports/run_<ts>/filtered_grants.txt`：过滤授权清单
- `main_reports/run_<ts>/trigger_status_report.txt`：触发器清单/状态差异报告
- `main_reports/run_<ts>/missing_objects_detail_<ts>.txt`：缺失对象支持性明细（report_detail_mode=split）
- `main_reports/run_<ts>/unsupported_objects_detail_<ts>.txt`：不支持/阻断对象明细（report_detail_mode=split）
- `main_reports/run_<ts>/extra_mismatch_detail_<ts>.txt`：扩展对象差异明细（report_detail_mode=split）
- `main_reports/run_<ts>/dependency_detail_<ts>.txt`：依赖差异明细（report_detail_mode=split）
- `*_detail_*.txt` 明细文件采用 `|` 分隔，并包含 `# total/# 字段说明` 头，格式与 `package_compare` 一致，便于 Excel 直接分隔导入。
- `main_reports/run_<ts>/missed_tables_views_for_OMS/`：OMS 缺失 TABLE/VIEW 规则
- `fixup_scripts/`：修补脚本输出（执行前需人工审核）
- `fixup_scripts/grants_miss/`：缺失授权脚本
- `fixup_scripts/tables_unsupported/`：不支持 TABLE 的 DDL（默认不执行）
- `fixup_scripts/unsupported/`：不支持/阻断对象 DDL（默认不执行）
- `fixup_scripts/view_chain_plans/`：VIEW 链路修复计划
- `fixup_scripts/errors/`：run_fixup 错误报告

## 黑名单规则
- 默认启用 `blacklist_rules.json` 规则并尝试读取 `OMS_USER.TMP_BLACK_TABLE`（`blacklist_mode=auto`）。
- 可通过 `blacklist_mode` 切换来源（table_only/rules_only/disabled），或用 `blacklist_rules_enable/disable` 精细控制规则。
- LOB 体积阈值由 `blacklist_lob_max_mb` 控制（默认 512MB）。
- 当使用 `blacklist_mode=auto` 或 `rules_only` 时，请确保 `blacklist_rules.json` 随工具部署；缺失时规则会被跳过。

## 常见配置片段
**只看表结构，不生成修复：**
```ini
check_primary_types = TABLE
generate_fixup = false
check_dependencies = false
```

**全量比对 + 修复脚本：**
```ini
check_primary_types =
check_extra_types = INDEX,CONSTRAINT,SEQUENCE,TRIGGER
generate_fixup = true
generate_grants = true
```

**扩展对象校验加速：**
```ini
extra_check_workers = 16
extra_check_chunk_size = 200
extra_check_progress_interval = 10
```

**授权脚本压缩：**
```ini
grant_tab_privs_scope = owner
grant_merge_privileges = true
grant_merge_grantees = true
```

## 已知限制与注意事项
- **字符串/注释中的特殊语法**：DDL 清洗与脚本拆分主要面向常见语法，遇到复杂 `q'[...]'` 或极端注释格式可能需要人工调整。
- **默认密码策略**：用户/角色初始化脚本使用固定初始密码，需后续改密。
- **配置含 `%` 字符**：部分环境下 `configparser` 会对 `%` 做插值，建议避免直接使用或改为转义。

## 项目结构速览
| 路径 | 说明 |
| --- | --- |
| `schema_diff_reconciler.py` | 主程序：对比、推导、报告、fixup 生成 |
| `run_fixup.py` | 修复脚本执行器（smart-order/迭代/view-chain） |
| `init_users_roles.py` | 用户/角色初始化 |
| `init_test.py` | 测试场景初始化 |
| `docs/` | 详细文档 |
| `readme_config.txt` | 配置项完整说明 |

## 更多文档
1) `readme_config.txt`：配置项与默认值
2) `docs/ADVANCED_USAGE.md`：Remap 推导与 run_fixup 高级说明
3) `docs/ARCHITECTURE.md`：架构设计与实现细节
4) `docs/DEPLOYMENT.md`：离线部署与跨平台运行
5) `docs/TECHNICAL_SPECIFICATION.md`：技术规格说明

---
© 2025 Minor Li.
