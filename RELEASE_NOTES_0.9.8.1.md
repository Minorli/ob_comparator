# Release Notes — V0.9.8.1

发布日期：2026-01-29

## 摘要
本版本聚焦“正确性与可维护性”：修复 VIEW remap 别名误替换、修复 run_fixup 迭代失败统计错误、完善 SYS_C* 额外列的清理逻辑、统一统计脚本模板并补足回归测试。

## 新增 / 改进
- **SYS_C* 额外列清理**：`fixup_drop_sys_c_columns=true` 时生成 `ALTER TABLE ... FORCE`，适配带后缀列名（如 `SYS_C00025_2025121711:29:07$`）。
- **SQLcl 格式化与报告**：格式化输出与 `ddl_format_report_<timestamp>.txt` 持续完善（详见配置说明）。
- **统计工具一致性**：`collect_source_object_stats.py` 统一 INDEX/CONSTRAINT/TRIGGER SQL 模板，避免简版/全量输出漂移。

## 修复
- **VIEW remap 别名误替换**：修复表别名被替换为 `SCHEMA.ALIAS` 的问题，并补充回归测试。
- **run_fixup 迭代统计错误**：修复累计失败数仅统计最后一轮的问题，汇总结果更准确。
- **SYS_C* 列识别**：兼容带复杂后缀的 SYS_C 列名。

## 行为与输出变化
- `run_fixup --iterative` 的最终汇总改为“累计失败/总计失败”。
- `config.ini.template` 去除重复配置项，避免误读。

## 配置要点（新增/重点）
- `fixup_drop_sys_c_columns`：是否对目标端额外 SYS_C* 列生成 `ALTER TABLE ... FORCE`。
- `report_dir_layout`：报告目录布局（per_run/flat）。
- `report_detail_mode`：报告明细拆分模式（full/split/summary）。
- `ddl_format_*`：SQLcl 格式化相关开关与阈值。

> 完整配置说明请见 `readme_config.txt`。

## 影响范围
- 主对比逻辑未改动，修复集中在 fixup 执行统计、DDL 输出与工具链一致性。
- 文档与说明已同步更新至 0.9.8.1。
