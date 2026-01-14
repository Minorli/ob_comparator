# Change: Preserve supported Oracle hints during DDL cleanup

## Why
Current DDL cleanup removes all Oracle hint comments (/*+ ... */) from generated fixup DDL. OceanBase Oracle mode supports a large subset of Oracle hints and ignores unknown hints without error, so blanket removal can change execution plans after migration. We need a configurable policy that preserves supported hints by default while retaining an explicit drop-all fallback for safety.

References:
- https://raw.githubusercontent.com/oceanbase/oceanbase-doc/V4.3.5/zh-CN/700.reference/500.sql-reference/200.sql-specifications-and-practices/200.sql-writing-Specification/700.hint-usage-specification.md
- https://raw.githubusercontent.com/oceanbase/oceanbase-doc/V4.3.5/zh-CN/700.reference/500.sql-reference/100.sql-syntax/300.common-tenant-of-oracle-mode/300.basic-elements-of-oracle-mode/600.annotation-of-oracle-mode/400.hint-of-oracle-mode/100.hint-overview-of-oracle-mode.md
- https://raw.githubusercontent.com/oceanbase/oceanbase-doc/V4.3.5/zh-CN/700.reference/500.sql-reference/100.sql-syntax/300.common-tenant-of-oracle-mode/300.basic-elements-of-oracle-mode/600.annotation-of-oracle-mode/400.hint-of-oracle-mode/200.hint-list-of-oracle-mode/

## What Changes
- Add configurable hint cleanup policy (ddl_hint_policy: drop_all, keep_supported, keep_all, report_only).
- Add allowlist/denylist controls (ddl_hint_allowlist, ddl_hint_denylist, ddl_hint_allowlist_file).
- Replace blanket hint removal with filtering that keeps supported hints and removes unsupported ones per policy.
- Log a summary of kept/removed hint tokens during fixup generation.

## Impact
- Affected specs: configuration-control, generate-fixup
- Affected code: schema_diff_reconciler.py (DDL cleanup), config.ini.template, tests
- Behavior change: default hint policy becomes keep_supported (drop_all remains available for strict cleanup).
- Risk: preserved hints may alter execution plans if statistics or schema differ; mitigated by configurable drop_all.
