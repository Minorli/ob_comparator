# Change: View chain guided autofix in run_fixup

## Why
Missing VIEWs often require hours of manual dependency checks and targeted GRANTs. Full retry loops with full GRANT files are too slow at scale. We need a precise, chain-driven repair mode that follows the dependency chain, fixes only what is missing, and executes in a per-view order.

## What Changes
- Add a view-chain-guided autofix mode in run_fixup (opt-in flag).
- Use the latest VIEWs_chain report to build per-view repair plans.
- Generate per-view plan + SQL files, then auto-execute them.
- Resolve missing privileges by searching grants_miss first, then grants_all for the specific GRANT statements only.

## Impact
- Affected specs: execute-fixup
- Affected code: run_fixup.py
- Docs: usage and output directories
