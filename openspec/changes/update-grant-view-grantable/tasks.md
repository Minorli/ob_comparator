## 1. Implementation
- [x] 1.1 Detect view grants that target non-owners and compute required dependency privileges
- [x] 1.2 Emit missing WITH GRANT OPTION statements for view owners on dependency objects
- [x] 1.3 Avoid duplicate grantable statements and respect privilege allowlist

## 2. Tests
- [x] 2.1 Unit tests for detecting missing grantable privileges on view dependencies
- [x] 2.2 Unit tests for generated GRANT WITH GRANT OPTION statements

## 3. Documentation
- [x] 3.1 Document view-owner grantable behavior
