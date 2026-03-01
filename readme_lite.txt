OceanBase 对比工具极简手册（现场版）
当前版本：V0.9.8.7

0. 先更新版本
- 项目地址：https://github.com/Minorli/ob_comparator
- 建议整仓更新，不要只替换单个脚本。

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

6. 运行主程序
python3 schema_diff_reconciler.py config.ini

重点看：
- main_reports/run_<ts>/report_<ts>.txt
- main_reports/run_<ts>/missing_objects_detail_<ts>.txt
- main_reports/run_<ts>/unsupported_objects_detail_<ts>.txt
- fixup_scripts/

7. 执行修复（run_fixup）
- 默认安全执行（不会跑 table/ 建表脚本）：
  python3 run_fixup.py config.ini --smart-order --recompile

- 视图链路自动修复：
  python3 run_fixup.py config.ini --view-chain-autofix

- 只有确认要建表时才放开：
  python3 run_fixup.py config.ini --smart-order --recompile --allow-table-create

8. 记住三句话
- 先跑主程序，再审 fixup_scripts，再跑 run_fixup。
- 不要随便执行建表脚本（避免目标端空表）。
- 发现异常先 git pull 更新后重跑。

9. 生产快速排障（只读）
- 自动读取最新 report_id 并输出诊断报告：
  python3 prod_diagnose.py config.ini
- 聚焦单对象深挖（推荐用户反馈“某对象不对/没生成/schema错/语法错”时）：
  python3 prod_diagnose.py config.ini --report-id <report_id> --focus-object VIEW:SCHEMA.OBJ --deep
- 主要看 4 个文件：
  triage_summary_*.txt（口径是否漂移）
  triage_detail_*.txt（每条差异的根因/建议）
  triage_fixup_failures_*.txt（fixup 失败归因）
  triage_false_positive_candidates_*.txt（疑似误报）
  （--deep 时还会生成 triage_focus_deep_*.txt）
