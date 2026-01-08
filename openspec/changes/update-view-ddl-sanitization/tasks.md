## 1. Implementation
- [x] 1.1 Add view DDL sanitizer for split identifiers using column metadata
- [x] 1.2 Add inline comment boundary restoration for SELECT lists
- [x] 1.3 Preserve string literals and comment content during sanitization
- [x] 1.4 Retain WITH CHECK OPTION only when OB version >= 4.2.5.7

## 2. Tests
- [x] 2.1 Unit tests for split-identifier repair
- [x] 2.2 Unit tests for inline comment line restoration
- [x] 2.3 Unit tests for WITH CHECK OPTION version gating

## 3. Documentation
- [x] 3.1 Document view DDL sanitization rules and limitations
