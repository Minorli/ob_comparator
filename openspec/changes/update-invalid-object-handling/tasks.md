## 1. Implementation
- [ ] 1.1 Extend Oracle status collection for VIEW/TRIGGER/PLSQL types needed for invalid handling.
- [ ] 1.2 Build invalid-object registry and add it to support classification + dependency blocking.
- [ ] 1.3 Block SYNONYM entries whose targets are invalid in the source.
- [ ] 1.4 Filter trigger status report rows for triggers whose base tables are blacklisted/unsupported.
- [ ] 1.5 Skip fixup DDL generation for INVALID VIEW/TRIGGER objects and record skip reasons.
- [ ] 1.6 Add PACKAGE/PACKAGE BODY dependency-aware ordering with cycle detection + stable fallback.

## 2. Tests
- [ ] 2.1 Unit tests: trigger status filtering for blacklisted tables.
- [ ] 2.2 Unit tests: invalid object propagation and synonym blocking.
- [ ] 2.3 Unit tests: fixup skips invalid view/trigger DDL generation.
- [ ] 2.4 Integration tests: package ordering + cycle detection behavior.
- [ ] 2.5 Integration tests: invalid VIEW/TRIGGER in Oracle -> skipped fixup in OB.

## 3. Validation
- [ ] 3.1 Run openspec validate update-invalid-object-handling --strict
