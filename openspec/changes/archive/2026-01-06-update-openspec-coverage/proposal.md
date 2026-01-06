# Change: Update OpenSpec coverage for current behavior

## Why
The OpenSpec specs do not fully reflect the current runtime behavior (configuration knobs, logging, dbcat caching/parallelism, fixup concurrency, and report/run summaries). Keeping a comprehensive spec is required for consistent future iteration.

## What Changes
- Update configuration specs to include logging, timeouts, dbcat/cache settings, fixup tuning, and runtime path validation.
- Update comparison specs for PUBLIC synonym inclusion rules.
- Update fixup specs for safe cleanup, concurrency/progress logging, dbcat caching/parallel export, and trigger_list fallback behavior.
- Update report specs for endpoint info, execution summary, and run summary sections.
- Update fixup executor specs for exclude-dirs filtering and log_level behavior.

## Impact
- Affected specs: configuration-control, compare-objects, generate-fixup, export-reports, execute-fixup.
- Affected code: None (spec-only change).
