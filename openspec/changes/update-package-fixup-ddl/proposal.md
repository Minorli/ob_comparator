# Change: Enable PACKAGE fixup DDL generation

## Why
Users need to generate PACKAGE and PACKAGE BODY DDL to repair missing objects, instead of treating packages as print-only.

## What Changes
- Remove PACKAGE/PACKAGE BODY from print-only fixup restrictions.
- Generate PACKAGE/PACKAGE BODY DDL under fixup_scripts/package and fixup_scripts/package_body when missing and allowed by filters.
- Keep DDL source behavior consistent: dbcat preferred for PACKAGE/PACKAGE BODY, DBMS_METADATA fallback when dbcat is missing.

## Impact
- Affected specs: generate-fixup
- Affected code: schema_diff_reconciler.py
- Outputs: fixup_scripts/package, fixup_scripts/package_body
