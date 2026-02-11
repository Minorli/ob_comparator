# Change: Constraint validate fallback for fixup and execution

## Why
Missing constraint fixup currently emits VALIDATE in some paths. In real migrations, target-side dirty data can cause ORA-02298 and block batch execution, even when source is clean. We need a safer default that preserves migration progress and still supports later full validation.

## What Changes
- Add `constraint_missing_fixup_validate_mode` to control missing-constraint DDL validation strategy.
- Default missing-constraint fixup to safe `NOVALIDATE` behavior to reduce ORA-02298 failures.
- Generate deferred validation scripts under `fixup_scripts/constraint_validate_later` for post-cleanup promotion to VALIDATED.
- Enhance run_fixup error classification for ORA-02298 and treat it as non-retryable data-quality failure.
- Add report outputs for deferred validation summary/details to guide operational closure.
- Update config template and config documentation for the new switch and behavior.

## Impact
- Affected specs: `configuration-control`, `generate-fixup`, `execute-fixup`, `export-reports`
- Affected code: `schema_diff_reconciler.py`, `run_fixup.py`, `config.ini.template`, `readme_config.txt`, HOW TO / docs
- Behavioral impact: safer default for missing constraint creation; no change to compare semantics
