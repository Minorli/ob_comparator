# Change: Iterative VIEW fixup resolver and long-running execution

## Why
VIEW creation frequently fails due to missing dependent objects or insufficient privileges. The current iterative runs re-apply full GRANT files and can time out, while a single failing GRANT can abort the whole file.

## What Changes
- Add an iterative VIEW resolver that, on failure, creates missing dependent objects from existing fixup scripts and retries.
- Apply only the GRANT statements needed by the failing VIEW and continue when individual GRANT statements fail.
- Execute SQL files statement-by-statement so a single statement error does not skip the rest of the file.
- Allow run_fixup to use long or unlimited execution timeouts via a dedicated setting (default 3600s, 0 disables timeout).

## Impact
- Affected specs: execute-fixup, configuration-control
- Affected code: run_fixup.py (no changes to schema_diff_reconciler.py)
- Behavior: Only enabled under --iterative; non-iterative runs keep current behavior except for statement-level execution and timeout control.
