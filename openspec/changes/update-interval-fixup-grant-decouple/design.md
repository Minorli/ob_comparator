## Context
当前 interval 分区补齐仅支持日期/时间分区键，数值分区键的 `INTERVAL (1)` 会被判为不可解析而跳过。同时，修补脚本生成依赖授权生成，导致 generate_grants=false 时无法输出 fixup。

## Goals / Non-Goals
- Goals:
  - 支持数值型 interval 表达式 (INTERVAL (n)) 的解析与补分区 DDL 生成。
  - 为数值型 interval 提供独立的截止值配置与校验。
  - 修补脚本生成不再依赖授权生成。
- Non-Goals:
  - 支持复杂表达式 (如函数、子查询) 的 interval 解析。
  - 自动推导数值型 interval 的默认截止值。

## Decisions
- Decision: 引入 `interval_partition_cutoff_numeric` 配置。
  - 仅对数值型分区键生效，默认空值表示跳过数值 interval 补齐。
  - 解析为 Decimal；<=0 或解析失败视为无效并跳过生成。
- Decision: 数值 interval 表达式仅支持简单数值字面量。
  - 识别模式：`INTERVAL ( <number> )`（允许空格）。
  - 目前不支持 `INTERVAL (expr)`、`INTERVAL ('1')` 或复杂函数。
- Decision: 数值分区边界解析与生成。
  - HIGH_VALUE 解析支持 `TO_NUMBER('...')` / `TO_NUMBER(...)` / 裸数值。
  - 分区名使用 `P<value>`，将非字母数字字符替换为 `_`，负数前缀 `P_NEG_`。
- Decision: 修补脚本生成与授权生成解耦。
  - generate_fixup=true 时始终执行 fixup 生成。
  - generate_grants=false 时跳过授权元数据加载与 GRANT 注入。

## Algorithm Sketch
1. 解析 interval 表达式：
   - 若匹配 NUMTOYMINTERVAL/NUMTODSINTERVAL → 日期/时间逻辑。
   - 若匹配 `INTERVAL (<number>)` 且分区键为数值型 → 数值逻辑。
2. 日期/时间逻辑使用 `interval_partition_cutoff`；数值逻辑使用 `interval_partition_cutoff_numeric`。
3. 迭代生成边界直到截止值或超过最大迭代数。
4. 输出 `ALTER TABLE ... ADD PARTITION` 脚本到 `fixup_scripts/table_alter/interval_add_<cutoff>/`。
5. 修补脚本生成阶段不依赖授权生成；授权仅在 generate_grants=true 时执行。

## Risks / Trade-offs
- 数值 interval 截止值需要手工配置，否则无法补齐。
- HIGH_VALUE 字符串解析存在格式差异风险（以日志提示与跳过策略兜底）。

## Migration Plan
- 默认不影响现有日期/时间 interval 行为。
- 数值 interval 需显式设置 `interval_partition_cutoff_numeric` 才会生成。
- generate_grants=false 不再影响 fixup 生成。

## Open Questions
- 是否需要支持 `INTERVAL ('1')` 或 `INTERVAL (1.5)` 等更宽松的表达式？
