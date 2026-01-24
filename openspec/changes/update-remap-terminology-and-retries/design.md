## Context
Synonym remap inference currently prioritizes dependency-driven targets even when the schema mapping is 1:1. This can remap synonyms across schemas and trigger false multi-to-one warnings. In parallel, mapping summary wording is inconsistent, and obclient/DDL failure handling lacks bounded retries.

## Goals / Non-Goals
- Goals:
  - Keep synonyms in source schema for 1:1 mappings unless explicitly remapped.
  - Provide a clear synonym remap policy (auto/source_only/infer) that is deterministic.
  - Normalize mapping terminology in logs and reports.
  - Add bounded, configurable retries for obclient calls and Oracle DDL batch fallback.
- Non-Goals:
  - Redesign remap inference for all object types.
  - Add new external dependencies.

## Decisions
- Synonym remap policy:
  - Introduce synonym_remap_policy with values: auto, source_only, infer.
  - Auto behavior:
    - PUBLIC synonyms stay PUBLIC.
    - If schema mapping for the source schema is 1:1, keep the synonym in the source schema (no inference).
    - If one-to-many mapping exists, infer schema from direct dependencies when possible.
    - If inference is ambiguous, record a remap_conflict and exclude from master list.
  - source_only always keeps the synonym in the source schema (unless explicit remap exists).
  - infer always attempts dependency inference, falling back to dominant schema from TABLE remaps when available.

- Mapping terminology:
  - Use consistent labels: 1:1, N:1, 1:N.
  - When schema-level output cannot resolve a single target in 1:N, log "schema-level fallback" and rely on per-object inference.

- obclient error handling:
  - Classify errors into transient (timeout, connection reset, server gone, too many connections) and fatal (auth failure, syntax error, access denied).
  - Add obclient_error_policy: auto (retry transient only), retry (retry all non-fatal), abort (no retry).
  - Add obclient_retry_max=3 and obclient_retry_backoff_ms=1000 for bounded retries.

- Oracle DDL fallback retries:
  - Add oracle_ddl_batch_retry_limit to cap per-object fallback retries after batch failures.
  - Log per-object retry exhaustion with a clear summary count.

## Risks / Trade-offs
- Synonym policy changes may alter existing remap expectations in complex 1:N scenarios. Mitigation: keep auto default and allow explicit overrides.
- Retry policies can mask persistent errors if set too high. Mitigation: conservative defaults and clear retry logs.

## Migration Plan
1. Add new config settings with defaults and validation.
2. Update remap inference and logging output.
3. Implement retry logic and update error handling.
4. Add tests and update docs.

## Open Questions
- Do we need separate retry policies for metadata dump vs fixup execution?
