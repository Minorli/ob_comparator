## Context
DDL cleanup currently strips all Oracle hint comments (/*+ ... */) via clean_oracle_hints, even though OceanBase Oracle mode supports many hints and ignores unknown hints without error. This can change execution plans when migrating VIEW/PLSQL/DDL objects and makes fixup output less faithful to the source.

## Goals / Non-Goals
Goals:
- Preserve OceanBase-supported hints by default during DDL cleanup.
- Allow explicit drop-all behavior to keep current strict cleanup when needed.
- Provide allowlist/denylist overrides and optional file-based allowlist.
- Emit a summary of kept/removed hints for audit.

Non-Goals:
- Do not attempt to rewrite or normalize hint semantics.
- Do not add new hints that were not present in source DDL.
- Do not fully parse SQL; only handle hint comments safely.

## Decisions
- Replace clean_oracle_hints with a hint filter that parses /*+ ... */ blocks, extracts hint keywords, and filters per ddl_hint_policy.
- Use a built-in allowlist derived from OceanBase official hint list for Oracle mode (V4.3.5). Allow user overrides via ddl_hint_allowlist/ddl_hint_denylist and optional ddl_hint_allowlist_file.
- Default ddl_hint_policy to keep_supported to retain hints likely supported by OceanBase while avoiding unsupported ones.
- Remove a hint block entirely if all tokens are filtered out; keep spacing stable otherwise.

## Risks / Trade-offs
- Preserving hints can change execution plans if statistics or data distributions differ from the source.
- The allowlist may drift across OceanBase versions; user overrides and file-based allowlist mitigate this.
- A lightweight parser may miss edge cases; masking string literals/comments reduces the risk of false replacements.

## Migration Plan
- Introduce new config settings with defaults that preserve supported hints.
- Provide ddl_hint_policy=drop_all to emulate old behavior immediately.
- Update config.ini.template and document recommended policies for conservative rollouts.

## Open Questions
- Should the default policy be keep_supported or report_only for the first release?
- Should we emit a dedicated hint-clean report file or reuse the existing DDL clean report mechanism?
