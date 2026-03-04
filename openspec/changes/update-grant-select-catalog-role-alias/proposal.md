# Change: Map SELECT_CATALOG_ROLE grants to OB_CATALOG_ROLE

## Why
在 Oracle 到 OceanBase 迁移中，源端常见角色 `SELECT_CATALOG_ROLE` 在 OB 侧通常不存在，导致自动生成的 `GRANT SELECT_CATALOG_ROLE TO ...` 在执行阶段失败。团队已在目标侧落地兼容角色 `OB_CATALOG_ROLE`，需要在工具侧统一替换，避免重复人工修补。

## What Changes
- 在授权计划生成阶段内置角色兼容映射：`SELECT_CATALOG_ROLE -> OB_CATALOG_ROLE`。
- 覆盖两类授权：
  - 角色授权（`GRANT <ROLE> TO <GRANTEE>`）
  - 以该角色作为 grantee 的对象授权归属。
- 保持现有配置使用方式不变（无新增开关）。

## Impact
- Affected specs: generate-fixup
- Affected code: `schema_diff_reconciler.py`, `test_schema_diff_reconciler.py`, grant docs
