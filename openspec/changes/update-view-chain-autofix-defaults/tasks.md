## 1. Implementation
- [x] 1.1 Skip existing views by default in view-chain autofix while still emitting plan/SQL with SKIP markers
- [x] 1.2 Search fixup_scripts/done for missing DDL when building per-view plans
- [x] 1.3 Auto-generate targeted GRANT statements when grants_miss/grants_all have no match
- [x] 1.4 Report per-view execution status (SUCCESS/PARTIAL/FAILED/BLOCKED/SKIPPED) and failure reasons

## 2. Tests
- [x] 2.1 Unit tests for done-directory DDL fallback selection
- [x] 2.2 Unit tests for status classification and auto-grant fallback behavior

## 3. Documentation
- [x] 3.1 Document default skip behavior, blocked recovery, and execution summary output
