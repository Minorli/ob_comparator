## 1. Implementation
- [x] 1.1 解析 VIEW DDL 中 FROM/JOIN 片段，仅在对象位置替换 unqualified 名称
- [x] 1.2 remap_view_dependencies 避免别名与非对象位置被替换（别名保持不变）
- [x] 1.3 保留 qualified 引用替换行为，确保 remap 仍生效

## 2. Tests
- [x] 2.1 新增视图别名冲突场景单测（短名别名与对象名冲突）
- [x] 2.2 新增派生表/子查询 alias 场景单测（alias 不应被替换）
- [x] 2.3 确认 VIEW schema 不推导、内部引用按 remap 替换（含同义词解析）不被本修复破坏

## 3. Validation
- [x] 3.1 `python3 -m py_compile $(git ls-files '*.py')`
- [x] 3.2 `python3 -m unittest test_schema_diff_reconciler.py`
