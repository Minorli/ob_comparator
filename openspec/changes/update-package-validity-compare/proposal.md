# Change: Package validity comparison and reporting

## Why
Users need to validate PACKAGE and PACKAGE BODY objects between Oracle and OceanBase, including existence and compile status, with a detailed per-object report for troubleshooting.

## What Changes
- Treat PACKAGE and PACKAGE BODY as primary comparison objects instead of print-only.
- Compare source/target VALID/INVALID status and capture compile error details when available.
- Exclude source-invalid packages from mismatch counts while listing them explicitly.
- Export a detailed package comparison report and include package differences in the main report.

## Impact
- Affected specs: compare-objects, export-reports
- Affected code: schema_diff_reconciler.py, report generation
- Reports: new package comparison report file
