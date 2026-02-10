## 1. Implementation
- [x] 1.1 Add OB GTT internal index detection helper (`IDX_FOR_HEAP_GTT_*`).
- [x] 1.2 Normalize target GTT index columns by removing leading `SYS_SESSION_ID` during index signature/map building.
- [x] 1.3 Exclude OB GTT internal indexes from target comparison map.
- [x] 1.4 Keep non-GTT behavior unchanged.

## 2. Tests
- [x] 2.1 Add unit test: GTT index with `SYS_SESSION_ID` prefix is treated as match.
- [x] 2.2 Add unit test: internal index `IDX_FOR_HEAP_GTT_*` does not produce extra mismatch.
- [x] 2.3 Add unit test: non-GTT table with `SYS_SESSION_ID` remains mismatched (no over-normalization).

## 3. Validation
- [x] 3.1 Run `python3 -m py_compile $(git ls-files '*.py')`.
- [x] 3.2 Run targeted unit tests for index comparison.
