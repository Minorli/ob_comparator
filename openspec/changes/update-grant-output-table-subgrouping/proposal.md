# Change: Split TABLE grant sections into object-grants and column-grants

## Why
当前对象/列授权已经统一落在：

- `grants_all/<OWNER>.grants.sql`
- `grants_miss/<OWNER>.grants.sql`
- `grants_deferred/<OWNER>.grants.sql`

并按 `OBJECT_TYPE` 分段。

但当 `OBJECT_TYPE=TABLE` 时，普通表级授权与列级授权仍然混在同一个段里，例如：

- `GRANT SELECT ON APP.T1 TO U1`
- `GRANT UPDATE (C1) ON APP.T1 TO U1`

这会让用户在审核时不容易第一眼区分：

- 是整表对象权限
- 还是列级权限

本次只优化可读性，不改变授权集合、不改变校验结果、不改变 fixup 目录结构。

## What Changes

- 在 `OBJECT_TYPE: TABLE` 段内，再按两类子段输出：
  - `TABLE_OBJECT_GRANTS`
  - `TABLE_COLUMN_GRANTS`
- 其他对象类型保持当前输出方式不变
- 列级授权仍与 owner 级 `*.grants.sql` 文件共存，不单独拆目录

## Non-Goals

- 不新增配置开关
- 不改变 `grants_miss` / `grants_all` 的语义
- 不改变 `run_fixup.py` 的执行目录和选择逻辑

## Impact

- Affected specs:
  - `generate-fixup`
- Affected code:
  - `schema_diff_reconciler.py`
  - `test_schema_diff_reconciler.py`
  - README / config docs (minimal wording)

