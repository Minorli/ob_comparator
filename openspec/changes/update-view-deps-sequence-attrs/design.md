## Context
View DDL rewrite currently relies on SQL token extraction and remap rules, which miss PUBLIC synonyms and subquery-based dependencies. Sequence comparison only checks existence, and column features like IDENTITY/DEFAULT ON NULL are not surfaced in reports.

## Goals / Non-Goals
- Goals:
  - Resolve PUBLIC synonym references in VIEW DDL to base objects with remap applied.
  - Use dependency metadata as a fallback when SQL extraction is incomplete.
  - Compare SEQUENCE attributes and report mismatches.
  - Surface IDENTITY/DEFAULT ON NULL column features during table comparison.
- Non-Goals:
  - Auto-generate IDENTITY or DEFAULT ON NULL fixup DDL.
  - Implement a full SQL parser for VIEW dependency extraction.

## Decisions
- Add optional dependency fallback for VIEW remap using preloaded Oracle dependency metadata (DBA_DEPENDENCIES).
- Resolve PUBLIC synonyms during VIEW rewrite by consulting cached synonym metadata and remap targets.
- Extend metadata loaders to include IDENTITY_COLUMN, DEFAULT_ON_NULL, and SEQUENCE attributes when available; degrade gracefully if the metadata columns are unavailable.
- Treat identity/default-on-null mismatches as table type issues and report them without auto-fix.

## Risks / Trade-offs
- Dependency fallback may include objects not present in the view SQL text; replacements are guarded by safe identifier matching to avoid unintended rewrites.
- Metadata columns for identity/default-on-null may not exist in older OB versions; the implementation must treat missing metadata as unknown and avoid false positives.

## Migration Plan
1. Land metadata and comparison changes behind existing code paths (no new config required).
2. Update tests to cover view rewrite and sequence attribute mismatch.
3. Run full comparator on a staging schema to validate reports.

## Open Questions
- Should private synonyms (non-PUBLIC) also be resolved to base objects during VIEW rewrite, or limit to PUBLIC only?
