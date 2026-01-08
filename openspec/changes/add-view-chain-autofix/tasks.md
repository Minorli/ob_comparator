## 1. Implementation
- [x] 1.1 Add CLI flag --view-chain-autofix and load latest VIEWs_chain file from report_dir
- [x] 1.2 Parse chain file into per-view dependency plans with cycle detection
- [x] 1.3 Build per-view SQL using fixup DDL + targeted GRANT lookup (grants_miss first, then grants_all)
- [x] 1.4 Execute generated SQL per view and preserve plan/SQL artifacts

## 2. Tests
- [x] 2.1 Unit tests for chain parsing and plan ordering
- [x] 2.2 Unit tests for grant lookup priority (miss then all)

## 3. Documentation
- [x] 3.1 Document view-chain autofix mode, outputs, and flags
