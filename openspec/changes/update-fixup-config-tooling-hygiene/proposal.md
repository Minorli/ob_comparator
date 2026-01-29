# Change: Fix fixup failure counts, config template duplication, stats tool hygiene, and exception handling

## Why
- Fixup summary currently misreports cumulative failures in iterative mode, reducing trust in run results.
- The configuration template has duplicated keys, which confuses users and makes changes error-prone.
- The source-object stats helper duplicates SQL templates and carries unused imports, increasing maintenance cost and drift risk.
- Exception handling is inconsistent across helper scripts, causing silent failures and low observability.

## What Changes
- Correct `run_fixup` cumulative failure accounting across iterative rounds and expose accurate summary counts.
- Remove duplicated `ddl_*` configuration blocks from `config.ini.template` (no behavior change, clarity only).
- Refactor `collect_source_object_stats.py` to share SQL templates between brief/full outputs and remove unused imports.
- Standardize exception handling patterns in helper scripts (log context, avoid silent `except`), with targeted tests.
- Add regression tests for fixup cumulative failure counting and stats helper consistency.

## Impact
- Affected specs: `execute-fixup`, `configuration-control`, `export-reports`
- Affected code:
  - `run_fixup.py`
  - `config.ini.template`
  - `collect_source_object_stats.py`
  - helper scripts (`init_test.py`, `init_users_roles.py`) and shared utilities (as needed)
  - test files under `test_*.py`
