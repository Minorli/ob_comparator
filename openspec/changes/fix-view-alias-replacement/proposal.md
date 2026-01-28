# Change: Fix view alias replacement during remap

## Why
当前 remap_view_dependencies 在处理 VIEW DDL 时，会把表别名当作对象名替换，导致生成的视图 DDL 语法错误或语义错误（如 `FROM A.TABLE B` 被替换成 `FROM A.TABLE SCHEMA.B`）。该问题在存在短名对象且视图使用相同别名时稳定复现，影响所有需要 remap 的视图。

## What Changes
- 在 VIEW DDL 重写时识别 FROM/JOIN 等位置的表别名，并禁止对别名进行替换。
- 仅对真正的对象引用执行 remap 替换；保留别名文本与引用一致性。
- 补充单测覆盖别名冲突场景（短名别名与对象名冲突）。

## Confirmation (行为保持)
- VIEW 目标 schema **不推导**，除非在 remap 文件中显式声明。
- VIEW 内部引用对象需要按 remap 规则替换为目标 schema。
- 若 VIEW 引用同义词，需解析到最终对象并使用最终对象的目标 schema。

## Impact
- Affected specs: generate-fixup
- Affected code: schema_diff_reconciler.py (remap_view_dependencies)
- Risk: 低（仅减少错误替换，仍保留现有 qualified-name 替换规则）
