## Context
Oracle is tolerant of a wider range of Unicode characters in PL/SQL source (often via NLS and quoted identifiers), while OceanBase rejects full-width punctuation that appears in syntactic positions. This causes fixup DDL failures for PL/SQL objects when the source contains full-width punctuation.

## Goals / Non-Goals
- Goals:
  - Prevent fixup DDL failures caused by full-width punctuation in PL/SQL code.
  - Preserve string literals and quoted identifiers to avoid semantic changes.
  - Provide visibility into what was sanitized.
- Non-Goals:
  - Rewrite or remove Chinese text in identifiers or strings.
  - Change the semantics of PL/SQL code beyond punctuation normalization.

## Decisions
- Sanitize only PL/SQL object types (PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TYPE, TYPE BODY, TRIGGER).
- Mask string literals and comments before replacements; unmask after sanitization.
- Replace known full-width punctuation with ASCII equivalents; normalize full-width space to ASCII space.
- Log the replacement count per object and optionally sample a few replacements.

## Risks / Trade-offs
- If a quoted identifier intentionally uses full-width punctuation, sanitization must not touch it. Masking quoted identifiers mitigates this risk.
- Removing or altering comments could remove useful context; default behavior should preserve comments.

## Migration Plan
- Introduce a configuration flag for enabling the sanitizer.
- Default to enabled for PL/SQL fixups to avoid runtime failures.
- Provide a quick rollback by disabling the flag.

## Open Questions
- Should the sanitizer default to on or off for PL/SQL types? Current recommendation is on, but can be adjusted if desired.
