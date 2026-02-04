# Change: Optional sequence RESTART sync fixup

## Why
当前序列仅做存在性校验，迁移后目标端序列从 START WITH 起步，可能与源端已使用的序列值发生冲突。需要提供一种**可选**的序列值同步方式，同时不破坏“序列仅存在性校验”的既有策略。

## What Changes
- 新增配置 `sequence_sync_mode`（默认 `off`），用于控制是否生成序列值同步脚本。
- 当 `sequence_sync_mode=last_number` 时：
  - 从 Oracle `DBA_SEQUENCES` 采集 `LAST_NUMBER`
  - 在 `fixup_scripts/sequence_restart/` 生成 `ALTER SEQUENCE ... RESTART WITH <last_number>` 脚本
- `run_fixup` 默认不执行 `sequence_restart` 目录；需要显式开关/参数才可执行。
- 在报告与说明文档中明确：**序列同步脚本需在数据迁移完成后执行**。

## Impact
- Affected specs:
  - `generate-fixup`
  - `execute-fixup`
  - `configuration-control`
- Affected code:
  - `schema_diff_reconciler.py`（Oracle 序列元数据、fixup 生成）
  - `run_fixup.py`（默认排除、显式执行开关）
  - `readme_config.txt`, `config.ini.template`（新开关说明）
- **No change** to sequence existence-only comparison logic.
