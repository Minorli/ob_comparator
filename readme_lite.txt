OceanBase 对比工具极简手册（现场版）
当前版本：V0.9.9.6

0. 先更新版本
- 项目地址：https://github.com/Minorli/ob_comparator
- 建议整仓更新，不要只替换单个脚本。
- 当前版本同时支持 Oracle → OceanBase 与 OceanBase → OceanBase（Oracle mode source）两种校验/修补模式。
- 0.9.9.6 增加发布门禁、运行心跳、恢复 manifest、fixup 安全分层和独立诊断包；建议整仓更新，不要只替换单个脚本。
- 0.9.9.5 修复 Oracle source 默认 BYTE 语义下 VARCHAR2 长度误判与 DDL 类型字面量问题；如遇到 `VARCHAR2` 被生成成 `VARCHAR` 或长度回缩，请更新到本版后重跑。

1. 你需要的文件（同目录）
- schema_diff_reconciler.py（主程序）
- run_fixup.py（修复执行器）
- config.ini（由 config.ini.template.txt 复制）
- remap_rules.txt（remap 规则）
- blacklist_rules.json（黑名单规则）
- exclude_objects.txt（可选：手工排除对象）
- trigger_list.txt（可选：触发器白名单，config.ini 里 trigger_list= 指向它）

2. 环境要求
- Python 3.7+
- Oracle Instant Client
- obclient
- Java 8+（dbcat 需要）
- 安全说明：运行时不会把 OB/dbcat 密码作为明文参数暴露在 `ps` 中。

示例环境变量（按实际路径改）：
export LD_LIBRARY_PATH=/path/to/instantclient:$LD_LIBRARY_PATH
export JAVA_HOME=/path/to/java
export PATH=$JAVA_HOME/bin:$PATH

3. 安装（在线）
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.ini.template.txt config.ini

4. 安装（离线包）
tar -zxvf pa_comparator_offline_pkg.tar.gz
cd pa_comparator
rm -rf .venv
python3.7 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install --no-index --find-links=wheelhouse -r requirements.txt
cp config.ini.template.txt config.ini

5. 最小配置
- 在 config.ini 填写源库/目标库连接、source_schemas。
- remap_file 指向 remap_rules.txt。
- 建议保留：table_data_presence_check = auto
- 如果这次只想补“对象能创建/能编译”的最小权限，把 `grant_generation_mode = structural` 打开；业务访问权限让 DBA 另行发。

6. 运行主程序
python3 schema_diff_reconciler.py config.ini

重点看：
- main_reports/run_<ts>/report_<ts>.txt
- main_reports/run_<ts>/report_index_<ts>.txt
- main_reports/run_<ts>/run_heartbeat_<ts>.json（运行中/挂起时看当前阶段）
- main_reports/run_<ts>/runtime_timeout_summary_<ts>.txt（本轮 timeout 生效值与冲突提示）
- main_reports/run_<ts>/runtime_degraded_detail_<ts>.txt（如果存在，先看；这表示本轮命中了保护性降级）
- main_reports/run_<ts>/compatibility_matrix_<ts>.json（本轮兼容矩阵）
- main_reports/run_<ts>/recovery_manifest_<ts>.json（断点恢复/恢复校验证据）
- main_reports/run_<ts>/difference_explanations_<ts>.jsonl（差异解释记录）
- main_reports/run_<ts>/fixup_plan_<ts>.jsonl（fixup SQL 的 safe/review/destructive/manual 分层计划）
- main_reports/run_<ts>/fixup_safety_summary_<ts>.txt（fixup 分层统计；这是审核证据，不替代审核 SQL）
- main_reports/run_<ts>/ddl_cleanup_detail_<ts>.txt
- main_reports/run_<ts>/missing_objects_detail_<ts>.txt
- main_reports/run_<ts>/unsupported_objects_detail_<ts>.txt
- fixup_scripts/
- 触发器专项时，再看：
  main_reports/run_<ts>/triggers_view_reference_detail_<ts>.txt
  main_reports/run_<ts>/triggers_literal_object_path_detail_<ts>.txt

运行初期优先看：
- report_<ts>.txt 里的“执行结论”和“本次建议处理顺序”
- fixup_scripts/README_FIRST.txt（会提示哪些目录默认不要直接执行）
- 如果主报告提示 `compare incomplete`，先去看 `runtime_degraded_detail_<ts>.txt`，别把这轮结果当最终验收口径
- 如果看到“本次相关变化提醒”，优先花 10 秒看完；同一提醒默认只展示一次

执行 fixup 时：
- 默认 `python3 run_fixup.py config.ini --smart-order --recompile` 只执行 safe/review 层，并继续默认跳过 table 建表脚本。
- 可先运行 `python3 run_fixup.py config.ini --smart-order --plan-only`，只验证本次选择哪些脚本，不连接数据库、不执行 SQL。
- destructive 层必须显式：`--safety-tiers destructive --confirm-destructive --only-dirs cleanup_safe`。
- manual 层必须显式：`--safety-tiers manual --confirm-manual --only-dirs materialized_view`。
- 分层只控制执行风险入口，不代表 SQL 可以免审。

触发器补充规则：
- 触发器里完整等于 `SCHEMA.OBJECT` 的字符串字面量，会按 remap 自动改写。
- `SCHEMA.OBJECT.COLUMN` 这类三段式字符串默认不自动改，避免把列名/日志文本改坏；需要看 `triggers_literal_object_path_detail_<ts>.txt` 人工确认。
- `PRAGMA AUTONOMOUS_TRANSACTION` 会保留。

DDL 清理补充规则：
- `ddl_cleanup_detail_<ts>.txt` 会标出是“格式整理”还是“语义改写”。
- `PRAGMA SERIALLY_REUSABLE`、`STORAGE(...)` 默认也会保留，不再静默删掉；`TABLESPACE` 也不再按“不支持语法”自动删除。
- 如果 fixup SQL 头里看到 `DDL_REWRITE:`，说明发生了类型/分区这类兼容性改写，执行前要人工复核。

7. 执行修复（run_fixup）
- 默认安全执行（不会跑 table/ 建表脚本）：
  python3 run_fixup.py config.ini --smart-order --recompile
- 如果某条 SQL 语句超时，这个脚本后面的语句现在会停止继续执行；先看 `fixup_scripts/errors/`，不要假设“后面可能已经跑了一部分”。
- run_fixup 会写 `fixup_scripts/run_fixup_heartbeat_<ts>.json`；file 模式只表示当前 SQL 文件进度，statement 模式才有语句级进度。
- run_fixup 的 timeout 生效值会写入 `main_reports/run_<ts>/run_fixup_timeout_summary_<ts>.txt` 或配置中的 report_dir。
- run_fixup 默认使用 `.fixup_state_ledger.json` 跳过指纹匹配的已完成文件/语句；如支持人员要求完整重放，可加 `--no-resume-ledger`。
- PL/SQL / package body / trigger 脚本里的 Q-quote 字面量，单独一行 `/` 不会再被误当成块终止符。

- 视图链路自动修复：
  python3 run_fixup.py config.ini --view-chain-autofix

- 只有确认要建表时才放开：
  python3 run_fixup.py config.ini --smart-order --recompile --allow-table-create

8. 记住三句话
- 先跑主程序，再审 fixup_scripts，再跑 run_fixup。
- 不要随便执行建表脚本（避免目标端空表）。
- `dependency_chains_*.txt` / `VIEWs_chain_*.txt` 属于辅助附件；如果大图场景被跳过或截断，主报告会单独告诉你。
- 发现异常先 git pull 更新后重跑。

9. 生产快速排障（只读）
- 优先看主报告、`report_index_*.txt`、`fixup_skip_summary_*.txt`、`unsupported_objects_detail_*.txt`。
- 如果客户说“不报错、不继续”，先收集 heartbeat json、timeout summary 和最近的 logs/run_*.log。
- 推荐直接生成诊断包：
  python3 diagnostic_bundle.py --run-dir main_reports/run_<ts> --config config.ini
- 挂起未退出时追加：
  python3 diagnostic_bundle.py --run-dir main_reports/run_<ts> --config config.ini --pid <pid> --hang
- 诊断包默认不包含 SQL 正文；客户明确允许后才加 `--include-sql-content`。
- 诊断包默认限制单文件 20MB、总采集 200MB；可用配置项或 CLI `--max-file-mb` / `--max-bundle-mb` 调整。
