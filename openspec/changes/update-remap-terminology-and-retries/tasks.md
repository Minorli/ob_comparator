## 1. Implementation
- [ ] 1.1 Add synonym_remap_policy config parsing, defaults, and validation.
- [ ] 1.2 Update resolve_remap_target to honor 1:1 schema mapping and synonym_remap_policy.
- [ ] 1.3 Normalize schema mapping terminology in logs and report summaries.
- [ ] 1.4 Add obclient error classification and bounded retry loop (policy + backoff).
- [ ] 1.5 Add oracle_ddl_batch_retry_limit to cap per-object fallback retries.
- [ ] 1.6 Update config templates and readme_config.txt with new settings.

## 2. Tests
- [ ] 2.1 Unit tests for synonym_remap_policy (auto/source_only/infer) across 1:1 and 1:N cases.
- [ ] 2.2 Log/report terminology snapshot tests for mapping summary output.
- [ ] 2.3 obclient retry behavior tests using mocked subprocess errors.
- [ ] 2.4 Oracle DDL fallback retry limit tests for batch failures.

## 3. Docs
- [ ] 3.1 Update OpenSpec deltas and run openspec validate --strict.
- [ ] 3.2 Document mapping terminology in README/config guidance.
