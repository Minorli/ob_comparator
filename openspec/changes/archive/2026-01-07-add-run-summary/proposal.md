# Change: Add run summary to report and runtime logs

## Why
用户需要在运行末尾看到结构化总结，以快速了解耗时、执行与跳过的步骤、关键发现和下一步建议。

## What Changes
- 在最终报告末尾追加“运行总结”区块，包含总耗时、阶段耗时、执行/跳过事项、关键发现与注意事项、下一步建议。
- 在运行日志末尾输出对应的结构化总结。
- 增加阶段耗时统计，用于报告与日志展示。

## Impact
- Affected specs: export-reports
- Affected code: schema_diff_reconciler.py
