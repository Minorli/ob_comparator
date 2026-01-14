# Change: 强化触发器/序列修补与报告隔离

## Why
当前触发器与序列的修补逻辑在跨 schema remap、对象引用补全、以及报告输出组织上存在一致性缺口，导致修补脚本不够可控、可审计性不足，且主报告目录混杂多次执行输出。

## What Changes
- 触发器修补 DDL：强制使用 remap 后的 schema 前缀重写触发器名称，并对触发器内引用的对象进行 schema 补全与 remap 统一处理，覆盖 ON 子句与 DML 语句。
- 序列 remap 策略：引入可配置策略，明确在“依赖推导 / 保持源 schema / 主流表 schema 推导”等模式间切换。
- 索引修补一致性：补充缺失索引生成前后的“跳过原因”统计与报告，解释缺失数量与最终生成数量的差异。
- OMS 输出目录重命名：将 tables_views_miss 目录改为 missed_tables_views_for_OMS。
- 报告目录隔离：每次执行在 report_dir 下生成独立子目录，避免多次执行混杂。
- 修补阶段配置冲突审计：识别 check_* 与 fixup_* 的组合冲突、触发器清单/检查禁用等情况，输出清晰提示。

## Impact
- Affected specs: generate-fixup, resolve-remap, compare-objects, export-reports, configuration-control
- Affected code: schema_diff_reconciler.py (fixup 生成、remap 推导、报告导出), config.ini.template/readme_config.txt/README.md
