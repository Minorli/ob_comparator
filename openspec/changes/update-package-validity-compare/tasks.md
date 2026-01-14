## 1. Implementation
- [x] 1.1 Extend Oracle metadata dump to capture PACKAGE/PACKAGE BODY status and compile errors (DBA_OBJECTS, DBA_ERRORS)
- [x] 1.2 Extend OceanBase metadata load to capture PACKAGE/PACKAGE BODY status (and errors if available)
- [x] 1.3 Implement package compare results with SOURCE_INVALID classification and mismatch exclusions
- [x] 1.4 Add package comparison export with per-object status and error summary
- [x] 1.5 Add package comparison section in main report with source-invalid list and target status breakdown

## 2. Tests
- [x] 2.1 Unit tests for status comparison, source-invalid exclusion, and missing detection
- [x] 2.2 Unit tests for report formatting and error summary output
- [x] 2.3 Unit tests for main report package section

## 3. Documentation
- [x] 3.1 Update README/report docs to mention package comparison report
