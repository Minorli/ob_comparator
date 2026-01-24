## Context
实际测试显示 OB 与 Oracle 在约束/索引元数据上存在系统性差异，导致当前比较逻辑在“约束名称不重要”的迁移场景下产生误报：
- OB 会把 UNIQUE INDEX 反映为 UNIQUE CONSTRAINT（show create table 中可见）。
- CHECK 条件表达式被自动加括号、关键字小写。
- OB 不支持 DESC 索引与 `USING INDEX <index_name>`。
- DEFERRABLE 约束在 OB 无法创建。

## Goals / Non-Goals
- Goals:
  - 避免 UNIQUE INDEX 派生 UNIQUE CONSTRAINT 的误报。
  - CHECK 表达式在语义不变时视为一致。
  - 将 OB 不支持的约束/索引分类为“不支持”。
  - DDL 输出避免 OB 语法错误。
- Non-Goals:
  - 不改变现有“列顺序、引用表、delete rule”等严格校验规则。
  - 不默认放宽 PK/UK/FK 的列级比较标准。

## Decisions
- Decision: CHECK 表达式 normalization 增加冗余括号折叠（不改变逻辑优先级）。
- Decision: 额外 UNIQUE 约束若能证明源端已有等价 UNIQUE INDEX，则视为派生并忽略（仍可记录在 detail 中）。
- Decision: 对 DEFERRABLE/DEFERRED 的 PK/UK/FK/CHECK 标记为不支持，避免进入“缺失”。
- Decision: 识别 Oracle 源端 DESC 索引并标记为不支持。
- Decision: 扩展 DDL 清洗规则移除 `USING INDEX <index_name>`。

## Risks / Trade-offs
- 风险: 过度过滤可能掩盖真实差异。
  - 缓解: 只在“源端存在等价 UNIQUE INDEX”时过滤 extra UNIQUE 约束，并保留可选 detail 记录。
- 风险: CHECK 括号折叠误判复杂表达式。
  - 缓解: 仅折叠“单一谓词”或“全表达式包裹”括号；对 AND/OR 混合嵌套保守处理。

## Migration Plan
- 在比较逻辑与 DDL 清洗处逐步引入规则。
- 增加单元测试与 Oracle/OB 实测用例。
- 对照真实库抽样验证误报数量显著下降。

## Open Questions
- 是否需要新增开关控制“派生约束过滤”行为（默认启用/关闭）？
