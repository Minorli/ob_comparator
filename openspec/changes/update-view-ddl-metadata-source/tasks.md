## 1. Implementation
- [x] 1.1 Route VIEW fixup DDL extraction through DBMS_METADATA and skip dbcat view output
- [x] 1.2 Ensure view cleanup removes Oracle-only modifiers while preserving OB-supported syntax
- [x] 1.3 Preserve WITH CHECK OPTION behavior based on OB version (<4.2.5.7 strip, >= keep)
- [x] 1.4 Update or add tests covering view DDL extraction and cleanup
