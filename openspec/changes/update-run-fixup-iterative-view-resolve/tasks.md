## 1. Implementation
- [x] 1.1 Add fixup_cli_timeout parsing and defaults in run_fixup
- [x] 1.2 Execute SQL files statement-by-statement and continue after statement failures
- [x] 1.3 Build fixup script index for dependency lookups
- [x] 1.4 Build GRANT statement index (per object and per grantee/object)
- [x] 1.5 Implement iterative VIEW resolver for missing objects and insufficient privileges
- [x] 1.6 Add logging for applied dependencies/grants and retry outcomes

## 2. Tests
- [ ] 2.1 Unit tests for SQL statement splitting and error handling
- [ ] 2.2 Unit tests for grant statement indexing and matching
- [ ] 2.3 Unit tests for iterative VIEW retry logic (mocked failures)

## 3. Documentation
- [x] 3.1 Update run_fixup usage docs to mention fixup_cli_timeout and iterative VIEW resolver
