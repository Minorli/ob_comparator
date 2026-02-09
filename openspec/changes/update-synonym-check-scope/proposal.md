# Change: Add synonym check scope toggle

## Why
当前同义词范围控制仅作用于 fixup（`synonym_fixup_scope`），主校验与检查汇总仍可能纳入私有同义词，导致 `SYNONYM` 的 extra 噪声偏大。

## What Changes
- 新增配置开关 `synonym_check_scope` 控制同义词“校验范围”（`public_only` / `all`）。
- 默认 `public_only`，仅将 PUBLIC 同义词纳入主校验与 extra 统计口径。
- 当配置 `all` 时，恢复 PUBLIC + 私有同义词全量校验。
- 统一源端采集、目标端元数据与检查汇总的同义词范围，避免口径不一致。

## Impact
- Affected specs: configuration-control, compare-objects
- Affected code: `schema_diff_reconciler.py`, `test_schema_diff_reconciler.py`
- Docs/config: `config.ini.template`, `readme_config.txt`
