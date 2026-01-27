# Change: Add migration focus report and improve main summary clarity

## Why
In late-stage migrations, users repeatedly run the comparator and only care about two questions:
1) Which objects are missing (can be fixed with scripts)?
2) Which objects are incompatible/unsupported (must be refactored)?

The current report set is comprehensive but noisy. The information exists, yet it is not presented
as a single focused view, causing confusion and extra manual filtering.

## What Changes
- Add a **migration focus** report that only lists:
  - Missing but supported objects (actionable fixups)
  - Unsupported/blocked objects (require refactor)
- Enhance the main report summary to **explicitly highlight** these two groups and point to the
  corresponding detail files.
- Keep all existing reports unchanged for debugging and traceability.

## Impact
- Affected specs: export-reports
- Affected code: schema_diff_reconciler.py (report assembly), optional docs
