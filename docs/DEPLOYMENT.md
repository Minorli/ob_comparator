# 数据库对象对比工具 - 跨平台打包与执行指南 (Wheelhouse 版)

> 适用版本：V0.9.8.6

> 适用场景：需要在无网络或不同机器上运行本工具，且保持源码不改。  
> 方案：wheelhouse + venv，无需 PyInstaller。

## 0. 目标机必备条件
- Python 版本与架构匹配（建议 3.7+）。
- Oracle Instant Client（19c+）。
- obclient（可连目标 OceanBase）。
- JDK + dbcat（用于 DDL 批量提取）。
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
├── init_users_roles.py
├── requirements.txt
├── config.ini                 # 可保留模板
├── remap_rules.txt
├── blacklist_rules.json
├── readme_config.txt
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
cp config.ini.template config.ini

# 运行主对比
python schema_diff_reconciler.py

# 运行修补执行器
python run_fixup.py --smart-order --recompile
```

提示：`run_fixup` 默认跳过 `fixup_scripts/table/`（防止误建空表）。  
如需执行建表脚本，显式添加：`--allow-table-create`。

提示：默认报告输出为 `main_reports/run_<timestamp>/`，如需兼容旧流程可设置 `report_dir_layout=flat`。

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
- 执行 `run_fixup.py --smart-order --recompile` 前先人工审核 `fixup_scripts/`。
- 若联调测试报 `ORA-12560` 或 OB socket 错误，优先排查客户端环境/网络连通性（非程序逻辑缺陷）。
