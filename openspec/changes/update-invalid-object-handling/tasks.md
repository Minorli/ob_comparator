## 1. Implementation
- [ ] 1.1 Extend Oracle status collection for VIEW/TRIGGER/PLSQL types needed for invalid handling.
- [ ] 1.2 Build invalid-object registry and add it to support classification + dependency blocking.
- [ ] 1.3 Block SYNONYM entries whose targets are invalid in the source.
- [ ] 1.4 Filter trigger status report rows for triggers whose base tables are blacklisted/unsupported.
- [ ] 1.5 Skip fixup DDL generation for INVALID VIEW/TRIGGER objects and record skip reasons.
- [ ] 1.6 Add PACKAGE/PACKAGE BODY dependency-aware ordering with cycle detection + stable fallback.
- [ ] 1.7 Add invalid_source_policy + invalid_source_types switches with defaults + validation.
- [ ] 1.8 Apply invalid_source_policy to fixup generation (skip/block/force) across VIEW/TRIGGER/PLSQL.
- [ ] 1.9 Apply invalid_source_policy to support classification + dependency blocking behavior.
- [ ] 1.10 Add INVALID summary section to main report and include active policy.

## 2. Tests
- [ ] 2.1 Unit tests: trigger status filtering for blacklisted tables.
- [ ] 2.2 Unit tests: invalid object propagation and synonym blocking.
- [ ] 2.3 Unit tests: fixup skips invalid view/trigger DDL generation.
- [ ] 2.4 Integration tests: package ordering + cycle detection behavior.
- [ ] 2.5 Integration tests: invalid VIEW/TRIGGER in Oracle -> skipped fixup in OB.
- [ ] 2.6 Unit tests: invalid_source_policy=skip removes missing entries.
- [ ] 2.7 Unit tests: invalid_source_policy=block emits unsupported DDL with reason.
- [ ] 2.8 Unit tests: invalid_source_policy=fixup allows DDL generation.

## 3. Validation
- [ ] 3.1 Run openspec validate update-invalid-object-handling --strict
