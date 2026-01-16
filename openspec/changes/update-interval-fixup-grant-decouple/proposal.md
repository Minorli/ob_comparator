# Change: Support numeric interval fixup and decouple fixup from grants

## Why
- 数值型 interval 分区 (INTERVAL (1)) 当前无法解析，导致 interval 表补分区缺失。
- generate_fixup 在 generate_grants=false 时不会运行，用户无法只生成修补 DDL。

## What Changes
- 增加数值型 interval 表达式解析与分区边界递增，支持数值分区键的补分区脚本输出。
- 新增数值型 interval 截止值配置，避免误用日期型截止导致分区膨胀。
- 修补脚本生成与授权生成解耦：generate_fixup=true 时无论 generate_grants 是否开启，都输出 fixup；授权仅在 generate_grants=true 时生成。
- 增加相关诊断与日志提示，帮助定位数值 interval 跳过原因。

## Impact
- Affected specs: configuration-control, generate-fixup
- Affected code: schema_diff_reconciler.py, config.ini, config.ini.template, readme_config.txt, tests
