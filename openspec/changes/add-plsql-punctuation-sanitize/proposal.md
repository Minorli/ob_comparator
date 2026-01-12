# Change: Add PL/SQL punctuation sanitization for fixup DDL

## Why
Full-width punctuation in PL/SQL DDL can fail to parse on OceanBase even when the source compiles on Oracle. This blocks fixup generation for packages/procedures and requires manual edits.

## What Changes
- Add a sanitizer that replaces full-width punctuation with ASCII equivalents in PL/SQL DDL before writing fixup scripts.
- Preserve string literals and quoted identifiers to avoid semantic changes.
- Add configuration to enable/disable the sanitizer and log what was changed.
- Write a report file summarizing which PL/SQL objects were sanitized and how many replacements occurred.

## Impact
- Affected specs: generate-fixup, configuration-control
- Affected code: schema_diff_reconciler.py (DDL cleanup pipeline)
