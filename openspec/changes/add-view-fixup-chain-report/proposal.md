# Change: View fixup dependency chain report

## Why
Views can depend on multiple layers of views and synonyms before reaching base tables. Fixup failures are hard to diagnose without a clear dependency chain that shows object type, owner, existence, and privilege readiness for each hop.

## What Changes
- Generate a VIEW dependency chain report during fixup generation.
- Report chains for views requiring fixup with type/owner/existence/grant markers per hop, including synonym hops.
- Output a concise file (VIEWs_chain.txt) for post-run diagnostics.

## Impact
- Affected specs: export-reports
- Affected code: schema_diff_reconciler.py
- Output: main_reports/VIEWs_chain_<timestamp>.txt
