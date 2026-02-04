# Change: Harden run_fixup execution and reporting (excluding password handling)

## Why
`run_fixup.py` executes generated DDL across many files; several robustness gaps can cause crashes, incorrect iteration summaries, unsafe file moves, confusing view-chain outputs, and unbounded memory/file-size risks. We want deterministic, safer execution without altering password handling.

## What Changes
- Harden subprocess execution error handling (non-timeout failures).
- Prevent done/ directory overwrites by safe move/backup strategy.
- Fix iterative mode cumulative failure counting.
- Validate obclient port range.
- Add optional fixup_dir boundary check (configurable).
- Improve auto-grant missing dependency warnings.
- Block view-chain execution when cycles detected (no SQL emitted).
- Add SQL file size limit and skip/report large scripts.
- Improve SQL splitter robustness (boundary cases + tests).
- Expand SQL error classification.
- Add AutoGrant cache size limit to prevent unbounded memory.

## Explicit Non-Goals
- Do **not** change password handling or obclient credential passing.

## Impact
- Affected spec: `execute-fixup`, `configuration-control`
- Affected code: `run_fixup.py`
- No change to `schema_diff_reconciler.py` output format (except clearer run_fixup reporting)
