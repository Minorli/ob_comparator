## 1. Implementation
- [x] 1.1 Include PACKAGE/PACKAGE BODY in missing object fixup task list
- [x] 1.2 Generate PACKAGE/PACKAGE BODY DDL scripts under fixup_scripts/package and fixup_scripts/package_body
- [x] 1.3 Ensure DDL source tracking shows dbcat vs DBMS_METADATA for packages

## 2. Tests
- [x] 2.1 Unit test: missing PACKAGE produces fixup script when generate_fixup enabled
- [x] 2.2 Unit test: fixup_types filter can restrict to package types

## 3. Documentation
- [x] 3.1 Update README/config docs to explain package fixup output and configuration
