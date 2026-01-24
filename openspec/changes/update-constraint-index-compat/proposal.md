# Change: Align constraint/index comparison with Oracle–OceanBase metadata differences

## Why
Oracle 与 OceanBase 在约束与索引的元数据呈现方式存在差异，导致比较阶段出现误报：
- OB 会为 UNIQUE INDEX 自动生成 UNIQUE 约束；Oracle 不会，导致“额外约束”误报。
- OB 对 CHECK 表达式会自动添加括号并小写关键字，导致条件不一致误报。
- OB 不支持 DESC 索引和 USING INDEX 子句，导致 DDL/比较不兼容。
- Oracle 支持 DEFERRABLE PK/UK/FK/CHECK，OB 不支持，应归类为“不支持”而非“缺失”。

需要在不削弱既有校验规则的前提下，补充兼容性判定与“派生约束”过滤逻辑。

## What Changes
- CHECK 约束表达式新增“冗余括号折叠”规范化规则（大小写已归一，保留字符串字面量）。
- 约束比较：过滤 OB 端由 UNIQUE INDEX 派生的 UNIQUE 约束（包含表达式索引场景），避免误报 extra。
- 约束比较：将 DEFERRABLE/DEFERRED 的 PK/UK/FK/CHECK 标记为不支持，避免误报缺失与生成修补。
- 索引比较：识别 DESC 索引为 OB 不支持，标记为不支持并从缺失中排除。
- DDL 清洗：移除 `USING INDEX <index_name>` 子句，避免 OB 解析错误。

## Impact
- Affected specs: `compare-objects`, `generate-fixup`.
- Affected code: `schema_diff_reconciler.py` (constraint/index compare, expression normalization, DDL cleanup), tests.
- No new user-facing switches by default；可选为“extra 约束过滤”引入开关（若需要）。
