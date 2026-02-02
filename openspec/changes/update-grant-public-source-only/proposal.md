# Change: Dependency-derived grants skip PUBLIC unless explicit

## Why
依赖推导的授权在遇到 PUBLIC 同义词/依赖时会生成 `GRANT ... TO PUBLIC`，与“公共同义词不要求 PUBLIC 授权”的行为不一致，导致过度授权。

## What Changes
- 依赖推导的授权不再对 PUBLIC 生成 GRANT。
- 仅保留源端显式存在的 PUBLIC 授权（DBA_TAB_PRIVS）。

## Impact
- Affected specs: generate-fixup
- Affected code: schema_diff_reconciler.py, tests
