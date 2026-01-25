# Change: Add noise-reduced reporting and system-generated mismatch filters

## Why
Current reports surface low-signal mismatches caused by system-generated artifacts (auto columns,
SYS_NC hidden columns, OMS helper objects, and auto-generated constraints/indexes). These findings
create user anxiety and obscure real issues. We need noise reduction that preserves traceability.

## What Changes
- Introduce report tiers: high-signal mismatches remain in the main summary; noise-suppressed
  mismatches are exported to a dedicated detail file with reasons.
- Classify system-generated artifacts deterministically (auto columns, SYS_NC hidden columns, OMS
  helper columns, OMS rowid indexes, OBNOTNULL constraints) for noise suppression.
- Keep comparison logic unchanged; noise-suppressed items are excluded from fixup generation.
- Continue to ignore auto-generated columns in comment comparison (merged from prior change).

## Impact
- Affected specs: `compare-objects`, `export-reports`.
- Affected code: `schema_diff_reconciler.py` (comment comparison, mismatch classification, report
  exports), tests.
- No new configuration switches.
