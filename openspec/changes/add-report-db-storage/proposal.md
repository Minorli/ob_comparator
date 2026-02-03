# Change: Add Report Database Storage (obclient-only)

## Why
当前报告仅输出为文本文件，难以进行历史趋势分析、跨运行比对与团队共享。用户提出将报告写入 OceanBase 以便查询与分析，同时要求保留现有文件报告输出。

## What Changes
- 新增可选能力：将校验报告写入 OceanBase（**仅使用 obclient**）。
- 新增配置开关与细粒度写入控制（明细范围、批量大小、JSON 保存、保留天数）。
- 新增数据库表结构（统一 `diff_` 前缀）与自动建表逻辑。
- 新增“检查汇总”按类型统计表（DIFF_REPORT_COUNTS）。
- 新增报告写库与清理流程，失败可容错不中断主流程。

## Impact
- Affected specs: `export-reports`, `configuration-control`
- Affected code: 报告生成与输出路径（需新增写库模块与 obclient 执行）
- Non-breaking: 默认关闭，不影响现有文件报告
