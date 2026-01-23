# Change: Address audit follow-ups for fixup ordering, cleanup, and constraint/grant metadata

## Why
Recent audit updates surfaced correctness gaps in fixup cleanup, dependency ordering, and constraint/grant metadata handling. These gaps can leave stale scripts, mis-order PL/SQL objects, and misclassify constraint or dependency issues, reducing trust in reports and fixup reliability.

## What Changes
- Clean fixup output directories deterministically (including master_list empty runs) with error-tolerant deletion and an explicit safety override for absolute paths.
- Add PL/SQL cleanup for collection attribute ranges (FIRST/LAST/COUNT) using single-dot to double-dot fixes.
- Extend dependency-aware ordering to TYPE/TYPE BODY and to PROCEDURE/FUNCTION/TRIGGER using dependency pairs, with stable fallback on cycles.
- Surface target DEFERRABLE/DEFERRED metadata when available and avoid false CHECK mismatches when OB lacks the columns.
- Match CHECK constraints by expression first while still flagging same-name expression mismatches.
- Prefer index column-set matching for SYS_NC normalization even when names differ.
- Expand grant linkage evaluation beyond VIEW dependencies with clearer GRANT_UNKNOWN semantics for unmapped types.
- Fix ordering/initialization quirks identified in audit (support_summary timing, redundant extra_results call).

## Impact
- Affected specs: compare-objects, generate-fixup, execute-fixup, configuration-control
- Affected code: schema_diff_reconciler.py, run_fixup.py
- Tests: new unit tests for PL/SQL cleanup and ordering, plus targeted tests for fixup cleanup and constraint metadata handling

## Non-Goals
- Password handling changes (P2-2)
- Monolithic file refactor (P3-1)
- Column order comparison (report-only) unless explicitly requested
