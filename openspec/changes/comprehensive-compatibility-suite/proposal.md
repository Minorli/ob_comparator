# Change: Consolidate Compatibility Suite

## Why
- The compatibility suite currently lives in two separate roots, which makes it easy to miss cases and harder to maintain.
- The suite is an internal reinforcement tool and should not require user config entries.

## What Changes
- Move the executable runner and case data into `compatibility_suite/runner/` and `compatibility_suite/cases/`.
- Re-home proposal docs and SQL drafts under `compatibility_suite/docs/` and `compatibility_suite/sql/`.
- Update the runner to use CLI-only suite options and keep config usage limited to connection settings.
- Normalize report naming and add a `|`-delimited case detail file for Excel review.
- Update suite docs to reference the new layout.

## Impact
- Affected code: `compatibility_suite/runner/compatibility_runner.py`, `compatibility_suite/cases/*`, `compatibility_suite/docs/*`, `compatibility_suite/sql/*`.
- No changes to main program logic or user-facing configuration.
