# Change: Fixup execution/session handling and grant/compile filtering

## Why
Fixup execution currently splits SQL files into separate obclient sessions, so ALTER SESSION SET CURRENT_SCHEMA does not apply to subsequent statements. This causes compile and grant scripts to fail even when objects exist. In addition, grant generation still emits SYS/PUBLIC-owned grants via dependency-derived rules, and VIEW compile statements are not supported by OceanBase.

## What Changes
- Preserve session context when executing files containing ALTER SESSION SET CURRENT_SCHEMA by applying the directive to each subsequent statement.
- Skip VIEW/MATERIALIZED VIEW compile scripts in fixup generation (unsupported in OceanBase).
- Filter dependency-derived and object-level grants to owners within configured schema scope (source schemas and their remap targets), excluding SYS/PUBLIC/system owners.
- Emit GRANT scripts without relying on ALTER SESSION SET CURRENT_SCHEMA.

## Impact
- Affected specs: execute-fixup, generate-fixup
- Affected code: run_fixup.py, schema_diff_reconciler.py
