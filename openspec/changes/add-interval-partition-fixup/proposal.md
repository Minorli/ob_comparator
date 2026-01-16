# Change: Add interval partition fixup generation

## Why
OceanBase Oracle 模式不支持 interval 分区语法，当前表修补 DDL 需要人工清洗，同时 interval 分区的补分区依赖外部工具生成，流程割裂且难以复用。

## What Changes
- 增加 interval 分区识别与 CREATE TABLE DDL 清洗，移除 INTERVAL 子句以保证在 OceanBase 侧可执行。
- 新增开关与截止日期配置，按源表的 interval 规则生成补分区 DDL。
- 在 fixup_scripts/table_alter 下按批次输出补分区脚本目录（如 interval_add_20280301）。
- 扩展 Oracle 元数据抽取以获取 interval 分区规则与现有分区边界。

## Impact
- Affected specs: configuration-control, generate-fixup
- Affected code: schema_diff_reconciler.py (Oracle 元数据抽取、DDL 清洗、fixup 生成), docs/readme_config.txt
