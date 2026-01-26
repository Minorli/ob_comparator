# Change: Add optional column order checks and audit regression coverage

## Why
Column order mismatches are currently invisible and users sometimes request explicit validation, but
always-on ordering checks would surface noise from system-generated columns. We also want to codify
the Jan 25 audit findings with targeted regression coverage to keep the low-noise behavior intact.

## What Changes
- Add an optional `check_column_order` toggle (default off) to compare table column order only when enabled.
- Exclude auto-generated and OMS helper columns from the order comparison, and skip order checks when column sets differ or order metadata is unavailable.
- Report column order mismatches in summary + split detail export; keep them report-only with no fixup DDL.
- Add regression tests/verification for the 2026-01-25 audit findings (virtual/invisible metadata fallback, virtual expression normalization, auto-sequence suppression, auto column comment noise filtering, metadata-gated IDENTITY/DEFAULT ON NULL checks).

## Impact
- Affected specs: `compare-objects`, `configuration-control`, `export-reports`, `generate-fixup`.
- Affected code: `schema_diff_reconciler.py`, `config.ini.template`, `readme_config.txt`, tests.
- No breaking changes to default behavior because `check_column_order` defaults to false.
