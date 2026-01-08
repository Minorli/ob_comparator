# Change: Grant missing-only output with grants_all + grants_miss

## Why
Full GRANT execution can involve hundreds of thousands of statements and take days in production. We need a missing-only GRANT output so run_fixup can apply only what is absent in OceanBase.

## What Changes
- Generate two grant directories: grants_all (full expected grants) and grants_miss (missing grants only).
- Compute missing grants by comparing Oracle-derived grants (after remap) with OceanBase DBA_* privilege views.
- Default run_fixup execution uses grants_miss when present.

## Impact
- Affected specs: generate-fixup, execute-fixup
- Affected code: schema_diff_reconciler.py, run_fixup.py
