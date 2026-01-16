## Context
当前工具对缺失 TABLE 生成 CREATE/ALTER DDL，但 interval 分区语法在 OceanBase Oracle 模式不可用，且补分区逻辑依赖外部脚本，导致迁移链路割裂。需要在主程序内完成 interval 分区识别、DDL 清洗与补分区脚本生成。

## Goals / Non-Goals
- Goals:
  - 自动识别 interval 分区表并移除 INTERVAL 子句。
  - 基于源端规则生成补分区 DDL，支持配置截止日期。
  - 输出结构可追踪（按批次目录），便于人工审核与执行顺序控制。
- Non-Goals:
  - 完整重建所有分区细节（如子分区模板等）。
  - 覆盖所有复杂 interval 表达式（先支持主流 NUMTOYMINTERVAL/NUMTODSINTERVAL）。

## Decisions
- Decision: 新增两个配置项。
  - `generate_interval_partition_fixup`: 是否生成补分区 DDL（默认关闭）。
  - `interval_partition_cutoff`: 截止日期（YYYYMMDD），用于补分区生成。
- Decision: interval 子句清洗为默认行为。
  - 对 CREATE TABLE DDL 中的 `INTERVAL (...)` 子句统一移除，避免 OB 执行错误。
- Decision: 规则来源优先使用 Oracle 元数据视图。
  - `DBA_PART_TABLES` 获取 interval 表达式与分区类型。
  - `DBA_TAB_PARTITIONS` 获取最后一个分区高值与分区名。
  - `DBA_PART_KEY_COLUMNS` 获取分区键（已存在于当前元数据加载）。

## Algorithm Sketch
1. 识别 interval 分区表：`DBA_PART_TABLES` 中 `INTERVAL` 不为空且 `PARTITIONING_TYPE='RANGE'`。
2. 读取最后一个分区的高值（`DBA_TAB_PARTITIONS`，`PARTITION_POSITION` 最大）。使用 XML/DBMS_XMLGEN 将 HIGH_VALUE 从 LONG 转 CLOB 以便解析。
3. 解析 interval 表达式（优先支持 `NUMTOYMINTERVAL` 与 `NUMTODSINTERVAL`），按分区键类型生成下一个边界。
4. 从最后分区边界开始，迭代生成直到 `interval_partition_cutoff`。
5. 按目标映射 schema/table 输出 `ALTER TABLE ... ADD PARTITION` DDL。

## Output Layout
- fixup_scripts/table_alter/interval_add_<YYYYMMDD>/
  - 每张表一份脚本，如 `<schema>.<table>.interval_add.sql`。
  - DDL 仅包含 ADD PARTITION 语句，便于在表创建之后执行。

## Risks / Trade-offs
- HIGH_VALUE 解析复杂：部分表达式或时区格式可能难以解析。解决：无法解析时跳过并记录原因。
- 命名规则不一致：分区名可能为 SYS_P*。解决：优先沿用现有命名模式，无法推断时使用递增编号。
- 性能：大量分区表可能生成较多 DDL。解决：仅在开关打开时生成，并按 schema 过滤。

## Migration Plan
- 默认关闭生成，开启后需显式设置截止日期。
- 先在小 schema 验证脚本数量与格式，再在全量环境执行。

## Open Questions
- 是否支持子分区模板同步（SUBPARTITION）？
- 是否需要为 interval 表单独输出清洗报告？
