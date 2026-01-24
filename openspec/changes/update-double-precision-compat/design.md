## Context
Oracle accepts `DOUBLE PRECISION` as a floating-point type synonym, but OceanBase (Oracle mode) rejects the `DOUBLE PRECISION` syntax in DDL. Oracle metadata/fixup extraction can emit this token, which breaks fixup execution even though `BINARY_DOUBLE` is supported.

## Goals / Non-Goals
- Goals:
  - Make fixup scripts compatible by normalizing `DOUBLE PRECISION` to `BINARY_DOUBLE`.
  - Keep comparisons accurate if the alias appears in metadata or DDL.
- Non-Goals:
  - Introduce new configuration switches.
  - Reformat or re-parse full SQL beyond the existing cleanup pipeline.

## Decisions
- Decision: Normalize `DOUBLE PRECISION` to `BINARY_DOUBLE` during DDL cleanup.
  - Rationale: Semantics are equivalent, and OB supports `BINARY_DOUBLE`.
- Decision: Add defensive normalization in type comparison for any `DOUBLE PRECISION` tokens.
  - Rationale: Guards against metadata or DDL that reports the alias directly.

## Risks / Trade-offs
- Risk: Over-eager replacement inside string literals/comments.
  - Mitigation: Reuse existing SQL rewrite safety utilities that avoid string/comment rewrites.
- Risk: Non-standard use of `DOUBLE PRECISION` in custom text (e.g., comments).
  - Mitigation: Only apply cleanup on DDL tokens, not comment/literal segments.

## Migration Plan
- Implement normalization logic and tests.
- Validate on Oracle 19c and OceanBase 4.2.5.7 using compatibility-suite.

## Open Questions
- None.
