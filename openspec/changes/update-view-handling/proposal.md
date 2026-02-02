# Change: Strengthen view handling (X$ compatibility + grant ordering)

## Why
视图迁移中出现权限链条失败、FORCE 视图残留、以及 X$ 系统表引用导致的不可用问题，需要在修补生成与执行顺序中明确处理，降低迁移失败率。

## What Changes
- 视图兼容性规则：识别 X$ 系统对象，默认判定不支持；若为用户自建 X$ 对象则允许。
- 视图 DDL 清洗：移除 `CREATE OR REPLACE FORCE VIEW` 中的 FORCE 关键字。
- 视图权限：拆分视图前置授权（依赖对象）与视图创建后授权（同步源端视图权限），避免执行顺序错误。
- 执行顺序：run_fixup 增加 view_prereq_grants / view_post_grants 目录并纳入顺序。

## Impact
- Affected specs: compare-objects, generate-fixup, execute-fixup
- Affected code: schema_diff_reconciler.py, run_fixup.py
