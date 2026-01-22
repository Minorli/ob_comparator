## 1. Implementation
- [x] 1.1 Extend Oracle/OceanBase metadata loading to capture INVISIBLE_COLUMN (and optional HIDDEN_COLUMN fallback).
- [x] 1.2 Add visibility mismatch detection to table comparison results.
- [x] 1.3 Generate visibility fixup DDL for CREATE/ALTER table flows based on policy.
- [x] 1.4 Add config parsing and validation for column_visibility_policy (and any fallback toggles).
- [x] 1.5 Update documentation and config templates.

## 2. Validation
- [x] 2.1 Unit tests for visibility parsing and mismatch reporting.
- [x] 2.2 Unit tests for visibility DDL output in CREATE and ALTER paths.
- [x] 2.3 Integration tests against Oracle/OB to confirm metadata availability.
