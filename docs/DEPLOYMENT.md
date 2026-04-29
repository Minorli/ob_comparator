# 数据库对象对比工具 - 跨平台打包与执行指南 (Wheelhouse 版)

> 适用版本：V0.9.9.6

> 适用场景：需要在无网络或不同机器上运行本工具，且保持源码不改。  
> 方案：wheelhouse + venv，无需 PyInstaller。

## 0. 目标机必备条件
- Python 版本与架构匹配（建议 3.7+）。
- Oracle Instant Client（19c+；仅 `source_db_mode=oracle` 必需）。
- obclient（可连目标 OceanBase）。
- JDK + dbcat（Oracle source 的 DDL 批量提取时使用；`source_db_mode=oceanbase` 不依赖 dbcat 做 source-side extract）。
- SQLcl（可选，用于 DDL 格式化）。
- 输出目录可写（`fixup_scripts/`、`main_reports/`、`dbcat_output/`）。

## 1. 在构建机准备 wheelhouse
```bash
python3 -m venv .venv
source .venv/bin/activate
mkdir -p wheelhouse
pip wheel --wheel-dir=./wheelhouse -r requirements.txt
```

若需为其他平台或 Python 小版本打包，需使用对应平台标签。

## 2. 打包部署目录建议
```
deployment_package/
├── schema_diff_reconciler.py
├── run_fixup.py
├── diagnostic_bundle.py
├── comparator_reliability.py
├── init_users_roles.py
├── requirements.txt
├── config.ini                 # 可保留模板
├── remap_rules.txt
├── blacklist_rules.json
├── compatibility_registry.json
├── readme_config.txt
├── HOW_TO_READ_REPORTS_IN_OB_latest.txt
├── HOW_TO_READ_REPORTS_IN_OB_20260311_12_sqls.txt
├── docs/
├── wheelhouse/
├── dbcat-2.5.0-SNAPSHOT/
├── instantclient_19_28/
└── setup_env.sh
```

`setup_env.sh` 示例：
```bash
export JAVA_HOME=/home/user/comparator/jdk-11
export LD_LIBRARY_PATH=/home/user/comparator/instantclient_19_28:${LD_LIBRARY_PATH}
export PATH=/home/user/comparator/dbcat-2.5.0-SNAPSHOT/bin:${PATH}
```

## 3. 目标机解压与离线安装
```bash
cd /path/to/deployment_package
python3 -m venv .venv
source .venv/bin/activate
pip install --no-index --find-links=./wheelhouse -r requirements.txt
```

## 4. 配置与运行
```bash
# 配置（尽量使用绝对路径）
cp config.ini.template.txt config.ini

# 运行主对比
python schema_diff_reconciler.py

# 运行修补执行器
python run_fixup.py --smart-order --recompile
```

提示：`run_fixup` 默认跳过 `fixup_scripts/table/`（防止误建空表）。  
如需执行建表脚本，显式添加：`--allow-table-create`。

提示：`run_fixup` 默认只执行 `safe,review` safety tiers；`destructive` 需要 `--safety-tiers destructive --confirm-destructive`，`manual` 需要 `--safety-tiers manual --confirm-manual`。执行前可先运行 `python3 run_fixup.py config.ini --plan-only` 做非破坏性计划验证；仍必须审核 `fixup_plan_<timestamp>.jsonl`、`fixup_safety_summary_<timestamp>.txt` 和对应 SQL。

提示：若本次只想输出“对象能创建/能编译”的最小授权，可在 `config.ini` 中设置 `grant_generation_mode=structural`。

提示：默认报告输出为 `main_reports/run_<timestamp>/`，如需兼容旧流程可设置 `report_dir_layout=flat`。

生产发布前必须生成 release evidence 并通过门禁：

```bash
python3 scripts/release_gate.py release_evidence_<version>.json
```

门禁要求至少一次 Oracle -> OceanBase 实库 smoke。运行时如客户反馈“不报错、不继续”，优先收集 `main_reports/run_<timestamp>/run_heartbeat_<timestamp>.json`、`runtime_timeout_summary_<timestamp>.txt`，以及 `run_fixup_heartbeat_<timestamp>.json` / `run_fixup_timeout_summary_<timestamp>.txt`（如执行过 fixup）。

推荐直接生成脱敏诊断包：

```bash
python3 diagnostic_bundle.py --run-dir main_reports/run_<timestamp> --config config.ini
python3 diagnostic_bundle.py --run-dir main_reports/run_<timestamp> --config config.ini --pid <pid> --hang
```

诊断包默认只包含 SQL 文件名、大小、hash 和摘要，不包含完整 SQL 正文；客户明确允许后才加 `--include-sql-content`。默认单文件上限 20MB、总采集上限 200MB，可用 `diagnostic_max_file_mb` / `diagnostic_max_bundle_mb` 或 CLI `--max-file-mb` / `--max-bundle-mb` 调整。

提示：`report_sql_<timestamp>.txt` 现在只提供 `report_id` 与 HOW TO 文档入口；如果交付包缺少 HOW TO 文件，客户无法按数据库侧剧本排查。

提示：若运行后出现 `runtime_degraded_detail_<timestamp>.txt`，说明本轮命中了保护性降级；交付时应把它与主报告一起提供，避免把 partial compare 当作最终结论。

## 5. 常见问题检查清单
- `LD_LIBRARY_PATH` 是否包含 instantclient。
- `JAVA_HOME` 是否正确且可执行 `java -version`。
- `obclient` 路径是否可执行，连接是否正常。
- `config.ini` 中路径是否使用绝对路径。

## 6. 安全提示
- `config.ini` 中包含明文密码，部署前请妥善脱敏与限制访问权限。
- `fixup_scripts/` 建议只保留必要脚本，执行前务必人工审核。

## 7. 交付前验收建议
```bash
# 1) 语法
python3 -m py_compile $(git ls-files '*.py')

# 2) 单测
.venv/bin/python -m unittest discover -v

# 3) 可选联调（需真实库）
.venv/bin/python schema_diff_reconciler.py config.ini
.venv/bin/python run_fixup.py config.ini --glob "__NO_MATCH__"
```
验收建议：
- 生成一次主报告并确认 `main_reports/run_<ts>/report_<ts>.txt` 可读；
- 若 `report_to_db=true`，确认 `DIFF_REPORT_SUMMARY` 出现新 `report_id`；
- 若生成了 `runtime_degraded_detail_<ts>.txt`，确认交付说明里已明确本轮是否 `compare incomplete`；
- 执行 `run_fixup.py --smart-order --recompile` 前先人工审核 `fixup_scripts/`。
- 若联调测试报 `ORA-12560` 或 OB socket 错误，优先排查客户端环境/网络连通性（非程序逻辑缺陷）。
