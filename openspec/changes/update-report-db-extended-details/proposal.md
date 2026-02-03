# Change: Extend Report-to-DB Storage for Usability / Package Compare / Trigger Status

## Why
当前 report_to_db 只覆盖 summary / counts / detail / grants。客户需要把可用性检查、PACKAGE 对比摘要、触发器状态报告也写入数据库，便于统一查询和长期留存。

## What Changes
- 新增 3 张报告表（diff_ 前缀）用于落地：
  - `DIFF_REPORT_USABILITY`
  - `DIFF_REPORT_PACKAGE_COMPARE`
  - `DIFF_REPORT_TRIGGER_STATUS`
- 缺失/不支持明细不新增表，继续使用 `DIFF_REPORT_DETAIL`（按 report_type/object_type 查询）。
- 复用现有 `report_to_db` 总开关和 `report_retention_days`，默认保留 90 天。
- report_to_db 默认值调整为 true（配置缺失时自动启用写库）。
- `package_compare` 只入库摘要 + hash + 文件路径（不入库存量 diff 文本）。

## Impact
- Affected specs: `export-reports`
- Affected code: `schema_diff_reconciler.py`（报告写库逻辑）
- Affected docs: `readme_config.txt`, `docs/ADVANCED_USAGE.md`
