# Change: OB 4.4.2 特性门控（MATERIALIZED VIEW + interval 分区）

## Why
当前程序对 `MATERIALIZED VIEW` 与 `interval` 分区采用静态策略：
- `MATERIALIZED VIEW` 默认仅打印不校验、不生成修补；
- `generate_interval_partition_fixup` 默认开启。

当目标 OceanBase 升级到 `4.4.2`（含）后，上述静态行为会产生偏差：
- `MATERIALIZED VIEW` 实际已可迁移，但程序仍跳过；
- interval 分区补齐脚本在新版本中不再是默认必要动作。

需要引入“按 OB 版本自动判定 + 手动覆盖”的门控机制，确保默认行为与目标版本能力一致，同时保持向后兼容。

## What Changes
- 新增版本感知的能力门控（feature gates），核心阈值为 `OB 4.4.2`。
- 扩展 `generate_interval_partition_fixup` 为三态：`auto|true|false`（兼容旧布尔值）。
- 新增 `mview_check_fixup_mode`：`auto|on|off`。
- 在 `auto` 模式下：
  - `OB >= 4.4.2`：默认关闭 interval 分区补齐；默认开启 MATERIALIZED VIEW 校验与修补；
  - `OB < 4.4.2`：维持旧行为（interval 默认开启，MVIEW 默认仅打印）。
- 保留手动覆盖：
  - `generate_interval_partition_fixup=true|false` 直接覆盖自动判定；
  - `mview_check_fixup_mode=on|off` 直接覆盖自动判定。
- 当 OB 版本不可识别时，`auto` 回退为“保守旧行为”（interval 开启、MVIEW 仅打印）并输出明确日志/报告提示。
- 报告中新增“能力门控生效值”展示，避免用户误判“为何生成/未生成”。

## Impact
- Affected specs:
  - `configuration-control`
  - `compare-objects`
  - `generate-fixup`
  - `export-reports`
- Affected code:
  - `schema_diff_reconciler.py`（配置解析、版本判定、对象校验与修补分支）
  - `config.ini.template`
  - `readme_config.txt`
  - tests（单测 + 集成测试）
