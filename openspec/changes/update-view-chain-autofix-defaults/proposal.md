# Change: Update view-chain autofix defaults and blocked recovery

## Why
View-chain autofix reprocesses existing views and stalls on common blocked cases (missing DDL or GRANT lookup miss). The execution summary can also be misleading when a view is created but one statement fails. We need default skip behavior for existing views and more precise recovery/reporting for blocked or partial outcomes.

## What Changes
- Default: skip views that already exist in OceanBase (still emit per-view plan/SQL with SKIP markers).
- Missing DDL fallback searches fixup_scripts/done for previously executed scripts.
- Missing GRANT fallback auto-generates a targeted object GRANT when no matching statement exists in grants_miss/grants_all.
- Execution summary reports per-view status (SUCCESS/PARTIAL/FAILED/BLOCKED/SKIPPED) and failure reasons.

## Impact
- Affected specs: execute-fixup
- Affected code: run_fixup.py
- Docs: README.md, docs/ADVANCED_USAGE.md
