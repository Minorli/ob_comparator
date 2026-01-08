## 1. Implementation
- [x] 1.1 Load OB privilege catalogs (DBA_TAB_PRIVS / DBA_SYS_PRIVS / DBA_ROLE_PRIVS)
- [x] 1.2 Compare expected grants with OB privileges and compute missing-only sets
- [x] 1.3 Output grants_all and grants_miss directories
- [x] 1.4 Update run_fixup to default to grants_miss when available

## 2. Tests
- [x] 2.1 Unit tests for privilege normalization and diffing
- [x] 2.2 Unit tests for grants_all vs grants_miss output

## 3. Documentation
- [x] 3.1 Update docs to explain grant_all/grant_miss usage
