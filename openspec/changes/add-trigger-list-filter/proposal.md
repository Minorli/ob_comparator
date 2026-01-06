# Change: Add trigger list filtering for fixup generation

## Why
Large migrations can have thousands of missing triggers, but users may only want to generate fixup DDL for a curated subset. Without filtering, the fixup output is noisy and harder to review.

## What Changes
- Add a `[SETTINGS]` option `trigger_list` pointing to a file of `SCHEMA.TRIGGER_NAME` entries.
- When `trigger_list` is configured, generate TRIGGER fixup DDL only for triggers in the list.
- Report list entries that are invalid, not found, or not missing in a new `main_reports/trigger_miss.txt` report.
- Summarize the filtering effect in the main report (total missing vs selected vs skipped).

## Impact
- Affected specs: generate-fixup, export-reports
- Affected code: `schema_diff_reconciler.py`, config docs (`config.ini.template`, `readme_config.txt`)
