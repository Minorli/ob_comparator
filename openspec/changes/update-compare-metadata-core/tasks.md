## 1. Implementation
- [x] 1.1 Extend Oracle metadata queries for constraints (CHECK, SEARCH_CONDITION, DELETE_RULE) and columns (VIRTUAL_COLUMN, DATA_DEFAULT expression)
- [x] 1.2 Extend Oracle index metadata with DBA_IND_EXPRESSIONS and bind expressions to index column positions
- [x] 1.3 Extend OceanBase metadata queries to capture DATA_LENGTH and CHAR_USED when available, with feature detection and fallback paths
- [x] 1.4 Expand metadata models to store CHECK conditions, FK delete rules, virtual column flags/expressions, and index expressions
- [x] 1.5 Update column comparison logic for NUMBER precision/scale and CHAR semantics using target CHAR_USED when available
- [x] 1.6 Update constraint comparison for CHECK expressions and FK delete rules; continue to ignore system-generated NOT NULL checks
- [x] 1.7 Update index comparison to use function-based expressions when available and preserve existing SYS_NC normalization
- [x] 1.8 Update fixup generation for CHECK constraints, FK delete rules, virtual columns, NUMBER precision/scale widening, and function-based index fallback
- [x] 1.9 Update report detail output to include new mismatch reasons and metadata fields

## 2. Tests
- [x] 2.1 Unit tests: precision/scale comparison, CHAR semantics mismatch handling, virtual column normalization, CHECK constraint normalization
- [x] 2.2 Unit tests: function-based index expression mapping and comparison signatures
- [ ] 2.3 Integration tests (Oracle/OB): CHECK constraints, FK delete rules, virtual columns, function-based indexes, NUMBER precision/scale
- [ ] 2.4 Regression tests: ensure partitioned PK downgrade logic is unchanged and still valid

## 3. Validation
- [x] 3.1 Run openspec validate update-compare-metadata-core --strict
- [ ] 3.2 Confirm docs and report outputs remain stable (no unintended format regressions)
