## 1. Implementation
- [x] 1.1 Detect grants/*.sql and execute statements with per-statement success/failure tracking
- [x] 1.2 Rewrite grant files to keep only failed statements; move to done when none remain
- [x] 1.3 Generate fixup_scripts/errors/fixup_errors_<timestamp>.txt with capped entries
- [x] 1.4 Add logging summary for pruned statements and report path

## 2. Tests
- [ ] 2.1 Unit tests for grant pruning rewrite behavior
- [ ] 2.2 Unit tests for error report formatting and cap

## 3. Documentation
- [x] 3.1 Update run_fixup usage docs to describe grant pruning and error report output
