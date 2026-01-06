# Change: Trigger list fallback and project link placement

## Why
用户希望在 trigger_list 缺失或为空时仍生成全量触发器，并在程序多个位置提供项目地址与问题反馈入口。

## What Changes
- Trigger list 无法读取或无有效条目时，回退为全量触发器校验与生成，并给出明确提示。
- 报告与运行日志中增加项目主页与问题反馈链接，便于获取最新版与提交问题。

## Impact
- Affected specs: generate-fixup, export-reports, configuration-control
- Affected code: schema_diff_reconciler.py, run_fixup.py
