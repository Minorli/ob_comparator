# Change: Compare sequences by existence only

## Why
当前序列比较会检查属性（cache、increment、min/max 等），在 Oracle 与 OceanBase 的实现差异下产生大量噪声，即使实际可用性一致也会被判为不匹配。客户期望序列只校验“存在/不存在”。

## What Changes
- 序列比较从“属性一致性”降级为“仅存在性”。
- 报告中不再输出 sequence 的 mismatched 细节（只保留 missing/extra）。
- fixup 逻辑保持：缺失序列仍生成 DDL。

## Impact
- Affected specs: `compare-objects`
- Affected code: `schema_diff_reconciler.py` 序列对比与报告汇总
- Behavior change: sequence mismatched 归零，噪声显著降低
