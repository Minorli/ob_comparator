# Change: Trigger/Constraint 状态差异检测与可选修复

## Why
当前程序对 TRIGGER/CONSTRAINT 主要覆盖“存在性与语义”差异，但对“对象已存在、状态不一致（ENABLED/DISABLED、VALIDATED）”仅有部分提示，无法稳定输出可执行修复脚本。

## What Changes
- 新增状态差异检测能力：
  - TRIGGER：ENABLED/DISABLED、VALID/INVALID。
  - CONSTRAINT：ENABLED/DISABLED（可选扩展到 VALIDATED）。
- 新增状态差异明细报告：`status_drift_detail_<ts>.txt`（`|` 分隔）。
- 新增可选状态修复脚本生成（默认关闭）：
  - `fixup_scripts/status/trigger/*.sql`
  - `fixup_scripts/status/constraint/*.sql`
- 新增配置开关与默认值（保守策略，避免误改业务状态）。

## Impact
- Affected specs: `compare-objects`, `generate-fixup`, `export-reports`, `configuration-control`
- Affected code: `schema_diff_reconciler.py`, `config.ini.template`, `readme_config.txt`, tests
