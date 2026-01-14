# Change: Optimize extra object checks for large schema sets

## Why
Extra object checks (INDEX/CONSTRAINT/SEQUENCE/TRIGGER) run slowly at 1,500+ schemas and ~94k objects. The current per-table loop re-normalizes metadata on every iteration and logs only every 100 tables, which looks like a stall for large runs.

## What Changes
- Precompute normalized signatures for index/constraint/trigger metadata once per table and reuse them during extra checks.
- Add fast-path equality checks so tables with matching signatures skip detailed diff logic.
- Introduce extra check tuning settings: extra_check_workers, extra_check_chunk_size, extra_check_progress_interval.
- Add time-based progress logging and per-type timing metrics for extra checks.

## Impact
- Affected specs: compare-objects, configuration-control
- Affected code: schema_diff_reconciler.py (metadata caches, extra checks, logging), config docs
- Behavior: comparison results unchanged; extra check output ordering is deterministic when parallelized
