# Change: Improve invalid object handling and trigger status filtering

## Why
Audit findings show false positives and invalid DDL generation when source objects are INVALID or depend on blacklisted tables. PACKAGE fixup ordering can also be wrong without dependency-aware sequencing, and SYNONYM objects pointing to INVALID targets are not flagged as blocked.

## What Changes
- Expand source object status capture to include VIEW/TRIGGER and other primary PL/SQL types needed for invalid handling.
- Treat INVALID source objects as unsupported nodes for dependency blocking and support classification (including SYNONYM target checks).
- Skip fixup DDL generation for INVALID VIEW and TRIGGER objects and record skip reasons.
- Order PACKAGE and PACKAGE BODY fixups using dependency graph with cycle detection and stable fallback order.
- Filter trigger status report entries for triggers whose parent tables are unsupported/blacklisted.

## Impact
- Affected specs: compare-objects, generate-fixup, export-reports
- Affected code: schema_diff_reconciler.py (Oracle metadata status load, support classification, trigger status reporting, fixup ordering)
