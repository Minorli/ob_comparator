# Change: DDL 引号规范化与约束比对纠偏

## Why
触发器与部分 DDL 生成路径存在引号格式错误（"SCHEMA.OBJECT"），在 OceanBase 执行会报错；同时 CHECK 约束存在“已存在却被判缺失”的误判，导致重复创建与 fixup 失败。需要统一输出规范与比对逻辑，保证修复脚本可执行并降低误报。

## What Changes
- 统一 DDL 输出中 schema/object 的引号格式，避免生成 "SCHEMA.OBJECT"。
- 触发器 CREATE/ON 子句及触发器体内 DML/序列引用的 schema 补全使用规范引用格式。
- VIEW/SYNONYM/FK REFERENCES/ALTER/DROP 等 DDL 生成点统一引用格式。
- 约束比对逻辑：当约束名已存在时，不再判为缺失；仅记录表达式差异。

## Impact
- Affected specs: compare-objects, generate-fixup
- Affected code: schema_diff_reconciler.py（DDL 输出与约束比对核心路径）、测试用例
- Risk: 中（输出规范变更触及多类型 DDL，需要回归）
