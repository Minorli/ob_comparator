# Change: Grant pruning and error report for run_fixup

## Why
Grant scripts can be large, and a single failing GRANT keeps the entire file in place, causing repeated long runs. Production error collection also needs a short, structured report for OCR-based review.

## What Changes
- Execute GRANT files statement-by-statement and prune successful GRANT statements from the file.
- Keep only failed GRANT statements in the original file for the next retry; move the file to done when all statements succeed.
- Write a concise error report under fixup_scripts/errors with capped entries for OCR-friendly collection.
- Enable this behavior by default for grant scripts in both single and iterative modes.

## Impact
- Affected specs: execute-fixup
- Affected code: run_fixup.py
- Output: fixup_scripts/errors/fixup_errors_<timestamp>.txt
