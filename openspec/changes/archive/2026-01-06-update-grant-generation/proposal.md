# Change: Enhance grant generation with source privileges and deep dependencies

## Why
Remapped schemas and dependency chains create new cross-schema privileges that are not captured by the current dependency-only grant suggestions. We need a grant pipeline that aligns with Oracle's privilege model, remap rules, and deep dependency chains while removing grant details from the report.

## What Changes
- Generate grant DDL by combining Oracle privilege metadata (DBA_TAB_PRIVS/DBA_SYS_PRIVS/DBA_ROLE_PRIVS) with remap and dependency-derived grants.
- Add a generate_grants toggle in config to control grant generation and DDL injection.
- Output grant scripts under fixup_scripts/grants and append object-level grants into per-object fixup DDL where applicable.
- Remove grant details from the report output.

## Impact
- Affected specs: configuration-control, generate-fixup, export-reports.
- Affected code: schema_diff_reconciler.py, config.ini, config.ini.template, readme_config.txt.
