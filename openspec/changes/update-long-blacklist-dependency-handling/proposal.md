# Change: Treat LONG/LONG RAW blacklist tables as non-blocking

## Why
当前逻辑会把 LONG/LONG RAW 的黑名单表作为阻断源，导致依赖这些表的对象被标记为“不支持/阻断”。但在实际迁移中，这类表通常通过 LONG→CLOB/BLOB 转换解决；无论目标端是否已创建，依赖对象都不应被 LONG 阻断。

## What Changes
- 调整 LONG/LONG RAW 黑名单表的阻断逻辑：
  - **目标端表存在或不存在** ⇒ 均不作为阻断源（依赖对象不再被列为不支持/阻断）。
- 保留现有 LONG 列类型校验（LONG→CLOB，LONG RAW→BLOB），继续输出转换状态到黑名单报告，但不再决定是否阻断。

## Impact
- Affected specs: `compare-objects`, `export-reports`
- Affected code: `schema_diff_reconciler.py` blacklist/blocked 逻辑
- Behavior change: 依赖阻断减少，缺失/不支持的统计会变化（符合用户期望）
