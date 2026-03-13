## 1. Rendering
- [x] Update grant-file rendering so `OBJECT_TYPE: TABLE` contains `TABLE_OBJECT_GRANTS` and `TABLE_COLUMN_GRANTS` subsections.
- [x] Keep other object-type sections unchanged.

## 2. Tests
- [x] Add/update unit tests for mixed table object grants + column grants in the same owner file.

## 3. Verification
- [x] Run py_compile.
- [x] Run relevant unit tests.
- [x] Run one real compare case containing column grants and inspect generated `grants_miss/grants_all` output.
