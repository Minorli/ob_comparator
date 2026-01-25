## 1. Implementation
- [x] 1.1 Extend OceanBase constraint metadata loading to capture `INDEX_NAME` when the column exists.
- [x] 1.2 Store `index_name` in constraint metadata entries for target PK/UK constraints.
- [x] 1.3 Build `constraint_index_cols` using the referenced index definitions (expressions + columns)
      via `normalize_index_columns`, with a fallback to `normalize_column_sequence`.
- [x] 1.4 Add unit test coverage for expression index + SYS_NC constraint matching (no missing index).
- [x] 1.5 Run `openspec validate update-index-constraint-expression-match --strict`.
