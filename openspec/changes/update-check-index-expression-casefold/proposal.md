# Change: Case-insensitive normalization for CHECK and index expressions

## Why
Oracle 与 OceanBase 在约束与函数索引表达式上存在大小写/括号风格差异（例如 `IS NOT NULL` vs `is not null`、`DECODE` 表达式大小写不同），导致比较阶段误判大量约束和索引缺失，并生成无效 fixup。需要对表达式做大小写不敏感的规范化（保留字符串字面量）以避免误报。

## What Changes
- CHECK 约束表达式比较改为大小写不敏感（保留字符串字面量原样）。
- 函数索引表达式比较改为大小写不敏感（保留字符串字面量原样）。
- 新增单元测试覆盖上述规则，并通过 Oracle/OB 实测验证。

## Impact
- Affected specs: `compare-objects`.
- Affected code: `schema_diff_reconciler.py`, `test_schema_diff_reconciler.py`.
- No new configuration switches.
