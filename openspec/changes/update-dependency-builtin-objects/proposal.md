# Change: Skip built-in dependency objects (DUAL)

## Why
Oracle 侧依赖元数据包含 PUBLIC.DUAL（或 SYS.DUAL）的记录，但 OceanBase 中 DUAL 是内建对象，不存在于元数据视图。
当前逻辑会把此类依赖标记为“缺少 remap 规则”，导致 VIEW 依赖报告出现噪音。

## What Changes
- 当依赖指向内建对象（PUBLIC.DUAL / SYS.DUAL）且无法映射到目标对象时，将该依赖标记为“内建对象依赖无需映射”，并从期望依赖集合中排除。
- 其他缺失依赖仍按原逻辑报告，不做降噪。

## Impact
- Affected specs: compare-objects
- Affected code: schema_diff_reconciler.py (dependency mapping), test_schema_diff_reconciler.py
