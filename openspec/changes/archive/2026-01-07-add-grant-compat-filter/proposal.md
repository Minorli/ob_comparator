# Change: Add grant compatibility filtering and role DDL

## Why
Grant DDL currently assumes Oracle system privileges exist in OceanBase. Some Oracle privileges (e.g., MERGE ANY VIEW / MERGE VIEW) are not supported in OB, causing fixup execution to fail. In addition, grants to custom roles require CREATE ROLE DDL to be generated so grant scripts can apply cleanly. OMS missing exports currently mix TABLE and VIEW in a single schema file, which is inconvenient for downstream tooling.

## What Changes
- Filter unsupported system/object privileges from generated GRANT statements using the target OB privilege catalog.
- Emit CREATE ROLE DDL for custom roles referenced by grants so role-based grants apply successfully.
- Split OMS-ready missing exports into per-schema TABLE and VIEW files for clean consumption.
- Export filtered/unsupported GRANT privileges to a report file for audit.
- Record and document Oracle vs OB privilege differences in OpenSpec design notes.

## Impact
- Affected specs: generate-fixup, configuration-control, export-reports
- Affected code: schema_diff_reconciler.py (grant plan, metadata load, fixup grant emission), config/docs
