# Change: Clarify report outputs and add blocked detail exports

## Why
Users are confused when summary counts report "unsupported/blocked" indexes or constraints but no corresponding
`indexes_unsupported_detail` / `constraints_unsupported_detail` file exists. The root cause is that these
counts include "dependency-blocked" items (e.g., based on unsupported tables) which are currently only present
inside `unsupported_objects_detail`. The report output needs clearer explanations and explicit blocked detail
files to avoid misinterpretation.

## What Changes
- Add dedicated blocked detail exports for INDEX/CONSTRAINT/TRIGGER derived from dependency-blocked rows.
- Clarify report descriptions and hints to distinguish syntax-unsupported vs dependency-blocked counts.
- Keep existing filenames (no renaming) and preserve current data logic.

## Impact
- Affected specs: export-reports
- Affected code: schema_diff_reconciler.py (report generation), readme/report docs (if applicable)
